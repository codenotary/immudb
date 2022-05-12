# Running as a system service

Every operating system has different ways of running services. Immudb provides a facility called `immudb service` to hide this complexity.

To install the service run:

```bash
immudb service install
```

This will for example, on Linux, install `/etc/systemd/system/immudb.service` and create the appropriate user to run the service. On other operating systems, the native method relevant to that operating system will be used.

The `immudb service` command also allows to control the lifecycle of the service:


```bash
immudb service start
immudb service stop
immudb service status
```

This facility is added a convenience for users to simplify running and management of immudb as a system service. It detects which operating system you have and acts accordingly. For example, on Linux, `immudb service status` would run `systemctl status immudb.service` under the hood.
