#!/bin/bash
# 
# usage: ./immudbin.sh [<writing_mode>]  
# Arguments: 
#   writing_mode: --ask |--quite
#   default: --ask
#
# Description:
# This script will update the immudb binaries. After building, running this script will overwrite files all over the place.
# You can choose whether to use interactive (--ask) or quiet (--quite) mode.
# If systemctl is available and service is running, it will be stopped and started afterwards.
#
# 
# Example:
# $ bash tools/linux/update_bins.sh --ask # this will ask you for confirmation before overwriting files
# $ bash tools/linux/update_bins.sh --quite # this will overwrite files without asking for confirmation
#

set -o nounset

# script stuff
ERROR=1
SUCCESS=0
PWD=$(pwd)
FIMMUDB="immudb"
FIMMUCLIENT="immuclient"
FIMMUADMIN="immuadmin"
FIMMUTEST="immutest"
SERVICE="immudb.service"
USAGE="Usage: ./tools/linux/$0 [--ask|--quite]\nDebug: bash -x tools/linux/$0 [--ask|--quite]"

# Parsing script arguments, in order to identify the writing mode
INTERACTIVE=${1:-"--ask"}

# I need one argument, either --ask or --quite. Nothing more, nothing less.
if [ $# -eq 0 ]; then
    echo "Writing mode not passed, I set --ask for you"
elif [ $# -gt 1 ]; then
    echo -e $USAGE
    exit $ERROR
else
    echo "Passed: $INTERACTIVE"
fi

# Argument parsing: parameters passed to the script, or help. Everything else is an error
if [ "${INTERACTIVE}" == "--quite" ]; then
    CPI=""
elif [ "${INTERACTIVE}" == "--ask" ]; then
    CPI="-i"
elif [ "${INTERACTIVE}" == "--help" ]; then
    echo -e "${USAGE}"
    exit ${SUCCESS}
else
    echo "EROOR: unknown option ${INTERACTIVE}"
    exit ${ERROR}
fi


# Searching for all binaries occurences, it will overwrite them 
search_replace(){
    BNAME=${1}

    echo "## WRITING ${BNAME^^} ##"
    C=0
    for i in $( sudo find / -name ${BNAME} -type f 2> /dev/null );do 
        DIRNAME=$( dirname ${i} )
        # we don't have to replace the the bin with itself
        if [[ ${DIRNAME} != ${PWD} ]];then
            ((C=C+1))
            echo -n "[${C}]..."
            sudo cp ${CPI} ${BNAME} ${i}
        fi
    done
    echo -e "Processed ${C} ${BNAME} files\n\n"
}

# if systemctl is available and service installed, we will stop and start the service
restart_idb_service(){
    echo "Checking for ${FIMMUDB} service..."
    SYSCTL=$( which systemctl )
    SERV_INSTALLED=$( sudo ${SYSCTL} list-unit-files | grep ${SERVICE} | cut -f1 -d" " )
    # if systemd service is installed, then we can restart it
    if [[ ${SERV_INSTALLED} == ${SERVICE} ]]; then
        echo -n "Restarting: "
        sudo ${SYSCTL} restart ${SERVICE}
        if [[ $? -ne ${SUCCESS} ]];then
            echo "Operation failed"
            exit ${ERROR}
        else
            echo "Operation completed"
        fi
    else
        echo "Not installed. NOOP."
    fi
    echo -e "${FIMMUDB^^} service management done\n\n"
}

#------------#
# EXECUTIONS #
#------------#

# 1 IMMUDB ACTIONS
search_replace ${FIMMUDB} 
restart_idb_service 

# 2 IMMUCLIENT ACTIONS
search_replace ${FIMMUCLIENT} 

#  3 IMMUADMIN ACTIONS
search_replace ${FIMMUADMIN} 

# 4 IMMUTEST ACTIONS
search_replace ${FIMMUTEST} 


# GOODBYE 
echo "Done."