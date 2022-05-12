# Running as a service

Every operating system has different ways of running services. Immudb provides a facility called `immudb service` to hide this complexity.

To install the service run:

This will for example, on Linux, install `/etc/systemd/system/immudb.service` and create the appropriate user to run the service. On other operating systems, the native method would be used.

The `immudb service` command also allows to control the lifecycle of the service:

On Linux, `immudb service status` is equivalent to `systemctl status immudb.service`, and is what it does under the hoods.
