#!/bin/sh

# PID file to use for an initialization immudb instance.
INIT_PID_FILE=/var/run/immudb/init.pid

# Trap interrupts.
# This is required to exit the waiting loops in the init function.
trap "echo aborted; exit;" INT TERM

INIT() {
   # If an initial user or database to be created is configured,
   # run immudb on localhost on perform the necessary actions.

   # Determine the configured user database.
   USER_DATABASE="${IMMUDB_DATABASE:-defaultdb}"

   # Test if a database and/or user should be created.
   CREATE_DATABASE=0
   if [ $USER_DATABASE != "defaultdb" ]
   then
      CREATE_DATABASE=1
   fi
   CREATE_USER=0
   if [ -n "$IMMUDB_USER" ] && [ -n "$IMMUDB_PASSWORD" ]
   then
      CREATE_USER=1
   fi

   # Run immudb on localhost.
   if [ $CREATE_USER -eq 0 ] && [ $CREATE_DATABASE -eq 0 ]
   then
      return
   fi
   echo "starting init immudb instance on localhost."
   if ! $@ --address 127.0.0.1 -d --pidfile $INIT_PID_FILE
   then
      echo "starting init immudb instance failed"
      return 1
   fi

   # Wait until the server is running.
   until immuadmin status
   do
      echo "waiting for immudb instance on localhost to be ready"
      sleep 1
   done

   # Log into the immudb instance.
   echo -n "$IMMUDB_ADMIN_PASSWORD" | immuadmin login immudb

   # Create it, if it is not the database created by default.
   if [ $CREATE_DATABASE -eq 1 ]
   then
      echo "creating uesr database $IMMUDB_DATABASE"
      if ! immuadmin database create $IMMUDB_DATABASE
      then
        echo "creating database $IMMUDB_DATABASE failed"
        return 1
      fi
   fi
   # Create a user if configured and grant it access to the user database.
   if [ $CREATE_USER -eq 1 ]
   then
      echo "creating user $IMMUDB_USER"
      # Assemble expect script to create the user.
      CREATE_USER_SCRIPT="
spawn immuadmin user create $IMMUDB_USER readwrite $USER_DATABASE
expect \"Choose a password for $IMMUDB_USER:\"
send \"$IMMUDB_PASSWORD\r\"
expect {
    timeout     { exit 1 }
    eof         { exit 1 }
    \"Password does not meet the requirements.\"  { exit 1 }
    \"Confirm password:\"
}
send \"$IMMUDB_PASSWORD\r\"
catch wait result
exit [lindex \$result 3]
"
      if ! expect -c "$CREATE_USER_SCRIPT"
      then
        echo "creating user $IMMUDB_USER failed"
        return 1
      fi
   fi

   # Stop the init instance.
   if [ -f $INIT_PID_FILE ]
   then
      echo "stopping init immudb instance on localhost."
      # Log out of the instance.
      immuadmin logout
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
       echo "initializing database failed"
       exit 1
    fi
fi

exec $@