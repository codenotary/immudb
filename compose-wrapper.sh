#!/bin/bash
set -o errexit
compose_bin=$(which docker-compose) || { echo "Could not find docker-compose in PAHT, exitting"; exit 2 ;}

OPTIND=1
DATA_DIR_OPT=""
COMPOSE_PREFIX_VARS=""
BUILD_FLAG=""

show_usage(){
    echo -en "Usage $0 -d [DIRECTORY] [OPTIONS]
Start immudb using docker-compose, exposes all used ports by default

With no options, it starts a container with no volumes (No data is persisted)\n

Options:
-d  Data directory. Attempts to create directory, otherwise checks for write access\n
-b  Build -- Necessary when the docker-compose file is updated
-c  Stop containers, remove orphans (if any), ignores all other flags
-e  Prompts user to remove environment file
-x  Runs docker-compose down (Stop immudb)
-s  Runs immudb in the background (service mode, runs immudb in the background)
"
}

clean_docker_compose(){
    docker-compose down --remove-orphans
    exit 0
}

clear_env_file(){
    read -p "Do you want to remove the environment file?(Y/N) " -r
    echo
    case "$REPLY" in
        yes|y|Y) rm -f .env
        ;;
        no|n|N) echo "Exitting without removing environment file"
        ;;
    esac
    exit 0
}

while getopts "hd:bcesx" opt; do
  case "$opt" in
    h|\?)
      show_usage
      exit 0
      ;;
    d)  DATA_DIR_OPT=$OPTARG
      ;;
    b)  BUILD_FLAG="--build"
      ;;
    c) clean_docker_compose
      ;;
    e) clear_env_file
      ;;
    x) $compose_bin down; exit 0;
      ;;
    s) POST_FLAG="-d"
      ;;
    *) echo "Invalid option received $OPTARG"; show_usage
  esac
done

if [ -f .env ]; then
    . .env
    if [ -n "$IMMU_UID" ]; then
        echo -en "Environment file already populated.\nIf any issues occur beyond this point please truncate the .env file"
    fi
else
    uid=$(id -u "$USER")
    gid=$(id -g "$USER")
    echo "No environment file found, creating .env file with UID:$uid and GID:$gid"
    echo -en "IMMU_UID=$uid\nIMMU_GID=$gid\n" >> .env
fi

if [ -n "$DATA_DIR_OPT" ]; then
    if [ "$DATA_DIR" = "$DATA_DIR_OPT" ]; then
        "-d Flag matches environment file, checking for write access"
        # Create data directory
        if [ -d "$DATA_DIR" ]; then
            if [ -w "$DATA_DIR" ]; then
                echo "Found writable directory, using $DATA_DIR for immudb data"
                echo "LOCAL_IMMUDB_DATA_DIR=$DATA_DIR" >> .env
            else
                echo "Directory $DATA_DIR found but not writable, aborting"
                exit 2
            fi
        else
            mkdir "$DATA_DIR" || { echo "Failed to create data directory, aborting" && exit 2; }
        fi
    else
        echo "-d Flag doesn't match .env file directory; A different data directory will be used. Stop now if this is not intentional"
        sleep 3
        if [ -d "$DATA_DIR_OPT" ]; then
            if [ -w "$DATA_DIR_OPT" ]; then
                echo "Found writable directory, using $DATA_DIR_OPT for immudb data"
            else
                echo "Directory $DATA_DIR_OPT found but not writable, aborting"
                exit 2
            fi
        else
            mkdir "$DATA_DIR_OPT" || { echo "Failed to create data directory, aborting" && exit 2; }
        fi
        DATA_DIR_OVERRIDE="$DATA_DIR_OPT"
    fi
fi

# 
if [ -n "$DATA_DIR_OVERRIDE" ]; then
    sed -i '/^#.*volumes:/s/^#//' docker-compose.yml
    sed -i '/^#.*LOCAL_IMMUDB_DATA_DIR:/s/^#//' docker-compose.yml
    LOCAL_IMMUDB_DATA_DIR="./$DATA_DIR_OVERRIDE" $compose_bin up $BUILD_FLAG $POST_FLAG
else
    sed -e '/^    volumes:$/ s/^#*/#/g' -i docker-compose.yml
    sed -e '/^        - ${LOCAL_IMMUDB_DATA_DIR:-.\/data}:\/var\/lib\/immudb$/ s/^#*/#/g' -i docker-compose.yml
    $compose_bin up $BUILD_FLAG $POST_FLAG
fi

