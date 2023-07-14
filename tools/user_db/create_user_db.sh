#!/bin/bash

# PID file to use for an initialization immudb instance.
INIT_PID_FILE=/var/run/immudb/init.pid

# Directory in which temporary files may be created.
TEMP_DIR="/tmp"
TEMP_USER_PASSWORD_FILE=${TEMP_DIR}/user-pass
IMMUDB_ADMIN_PASSWORD_FILE=${TEMP_DIR}/admin-pass

# Cleanup temporary password files.
CLEANUP() {
   if [ -f "$TEMP_USER_PASSWORD_FILE" ]
   then
      rm $TEMP_USER_PASSWORD_FILE
   fi
   if [ -f "$IMMUDB_ADMIN_PASSWORD_FILE" ]
   then
      rm $IMMUDB_ADMIN_PASSWORD_FILE
   fi
}

# Trap interrupts.
# This is required to exit the waiting loops in the init function.
trap "echo aborted; CLEANUP; exit;" INT TERM

INIT() {
   # If an initial user or database to be created is configured,
   # run immudb on localhost on perform the necessary actions.

   # Determine the configured user database.
   USER_DATABASE="${IMMUDB_DATABASE:-defaultdb}"

   # Test if a database and/or user should be created.
   CREATE_DATABASE=0
   if [ "$USER_DATABASE" != "defaultdb" ]
   then
      CREATE_DATABASE=1
   fi

   # Check if a secret file for the user password was specified.
   if [ -n "$IMMUDB_PASSWORD_FILE" ]
   then
      if [ ! -f "$IMMUDB_PASSWORD_FILE" ]
      then
         echo "file ${IMMUDB_PASSWORD_FILE} specified for IMMUDB_PASSWORD_FILE does not exist"
         return 1
      fi
   else 
      # If no password file was specified, create a temporary file with
      # the content of the IMMUDB_PASSWORD variable.
      if [ -n "$IMMUDB_PASSWORD" ]
      then
         if ! echo "$IMMUDB_PASSWORD" > $TEMP_USER_PASSWORD_FILE
         then
            echo "creating temporary password file to create IMMUDB_USER failed"
            return 1
         fi
         IMMUDB_PASSWORD_FILE=$TEMP_USER_PASSWORD_FILE
      fi
   fi

   CREATE_USER=0
   if [ -n "$IMMUDB_USER" ] && [ -n "$IMMUDB_PASSWORD_FILE" ]
   then
      CREATE_USER=1
   fi
   
   # Check if a user and or a database should be initialized.
   if [ $CREATE_USER -eq 0 ] && [ $CREATE_DATABASE -eq 0 ]
   then
      return
   fi

   # Create temporary file with admin password to issue immuadmin commands.
   if ! echo "$IMMUDB_ADMIN_PASSWORD" > $IMMUDB_ADMIN_PASSWORD_FILE
   then
      echo "creating temporary password file with immudb admin passsword failed"
      return 1
   fi

   # Start immudb on localhost to setup the database and user.
   echo "starting init immudb instance on localhost."
   if ! $@ --address 127.0.0.1 -d --pidfile $INIT_PID_FILE
   then
      echo "starting init immudb instance failed"
      return 1
   fi

   # Wait until the server is running.
   until immuadmin status --password-file $IMMUDB_ADMIN_PASSWORD_FILE --non-interactive
   do
      echo "waiting for immudb instance on localhost to be ready"
      sleep 1
   done

   # Create it, if it is not the database created by default.
   if [ $CREATE_DATABASE -eq 1 ]
   then
      echo "creating user database ${IMMUDB_DATABASE@Q}"
      if ! immuadmin database create "${IMMUDB_DATABASE}" \
         --password-file $IMMUDB_ADMIN_PASSWORD_FILE \
         --non-interactive
      then
        echo "creating database ${IMMUDB_DATABASE@Q} failed"
        return 1
      fi
   fi
   # Create a user if configured and grant it access to the user database.
   if [ $CREATE_USER -eq 1 ]
   then
      if ! immuadmin user create "${IMMUDB_USER}" readwrite "${IMMUDB_DATABASE}" \
       --new-password-file $IMMUDB_PASSWORD_FILE \
       --password-file $IMMUDB_ADMIN_PASSWORD_FILE \
       --non-interactive
      then
        echo "creating user ${IMMUDB_USER@Q} failed"
        return 1
      fi
   fi
   
   # Stop the init instance.
   if [ -f $INIT_PID_FILE ]
   then
      echo "stopping init immudb instance on localhost."
      # Kill the immudb instance.
      INIT_PID=$(cat $INIT_PID_FILE)
      kill $INIT_PID
      # Wait for the proccess to exit.
      while kill -0 $INIT_PID 2> /dev/null
      do
         echo "waiting for init immudb instance to stop"
         sleep 1
      done
   fi
}

# Check if this is the first time immudb is run.
if [ ! -f "$IMMUDB_DIR/immudb.identifier" ]
then
    # Initialize a user database from environment variables.
    echo "initialilzing database"
    if ! INIT $@
    then
       # Cleanup temporary files.
       CLEANUP
       echo "initializing database failed"
       exit 1
    fi
    # Cleanup temporary files.
    CLEANUP
fi

exec $@