# Running an auditor with immuclient

### Use immuclient as Auditor <a href="#use-immuclient-as-auditor" id="use-immuclient-as-auditor"></a>

The Auditor is a component for checking if immudb was tampered, it's a good practice to run the auditor as a separate and independent component. immuclient can act as Auditor by running the following command:

Start interactive:

immuclient is now running on the following address: \*\*immuclient Port: 9477 - http://immuclient-auditor:9477/metrics \*\*

example output:

immuclient looks for immudb at 127.0.0.1:3322 by default with the default username and password. Nevertheless a number of parameters can be defined:

### Running immuclient Auditor as a service <a href="#running-immuclient-auditor-as-a-service" id="running-immuclient-auditor-as-a-service"></a>

immuclient as Auditor can be installed in the system with the following command:

Install service:

In this case, all parameters are written into the `immuclient` configuration file:

* Linux: `/etc/immudb/immuclient.toml`
* Windows: `C:\ProgramData\ImmuClient\config\immuclient.toml`

### Running immuclient Auditor with docker <a href="#running-immuclient-auditor-with-docker" id="running-immuclient-auditor-with-docker"></a>

We also provide a docker image starting immuclient as Auditor:

Then it's possible to run the command with:

### Auditor best practices <a href="#auditor-best-practices" id="auditor-best-practices"></a>

#### How can I be notified if my immudb instance was tampered? <a href="#how-can-i-be-notified-if-my-immudb-instance-was-tampered" id="how-can-i-be-notified-if-my-immudb-instance-was-tampered"></a>

It's possible to provide an external url that will be triggered in case a tamper is detected. By configuring `IMMUCLIENT_AUDIT_NOTIFICATION_URL`, a POST request will be sent with the following body:

NOTE: it's not possible to know at which transaction the database was tampered. The Auditor checks every second if the data was tampered - so it's only possible to know at which time frame the tampering was detected.

#### How many Auditors should I run to secure my immudb instance? <a href="#how-many-auditors-should-i-run-to-secure-my-immudb-instance" id="how-many-auditors-should-i-run-to-secure-my-immudb-instance"></a>

A proper setup of one immuclient instance can fit most of cases, but there are ways to increase the security on detecting tampering. A single instance can go offline for any reason: network problems, hardware failures or attacks. Therefore a good practice can be to have multiple Auditor instances running in different zones.

### License <a href="#license" id="license"></a>

immuclient is [Apache v2.0 License (opens new window)](https://github.com/codenotary/immudb/blob/master/LICENSE).
