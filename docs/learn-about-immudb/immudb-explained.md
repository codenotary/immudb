# immudb explained

### What is immudb? <a href="#what-is-immudb" id="what-is-immudb"></a>

immudb is database with built-in cryptographic proof and verification. It can track changes in sensitive data and the integrity of the history will be protected by the clients, without the need to trust the server.

immudb can operate both as a key-value or relational (SQL) database. You can add new transactions, but deletion or modification of older transactions isn’t allowed, thus making your data immutable. When a key record's value changes over time (such as a bank balance), you can get multiple instances with different timestamps to give you the complete history of that record's changes. You can store a variety of common data types, verification checksums, or JSON.

### What makes immudb special? <a href="#what-makes-immudb-special" id="what-makes-immudb-special"></a>

#### Immutable

Data is never overwritten. See the history of data updates.

#### Auditable

Tamper-evident history system ensures data authenticity.

#### Secure

Data ownership is verifiable by clients and auditors.

* <mark style="color:blue;">**Keep track of changes and audit them.**</mark> Traditional database transactions and logs are hard to scale and are mutable, so there is no way to know for sure if your data has been compromised. immudb is immutable. You can add new versions of existing records, but never change or delete records. This lets you store critical data without fear of it being changed silently.
* <mark style="color:blue;">**Verify your data without sacrificing performance.**</mark> Data stored in immudb is cryptographically coherent and verifiable, just like blockchains, but without all the complexity. Unlike blockchains, immudb can handle millions of transactions per second, and can be used both as a lightweight service or embedded in your application as a library.
* <mark style="color:blue;">**Protect yourself from supply-chain attacks.**</mark> While Cyber Security is an important part of your organization’s business plan, immudb provides another layer of security to ensure data integrity even in the event your perimeter is breached during an attack. Data cannot be deleted or modified once stored into immudb. Additions of new data are logged and auditable, enabling you to view any suspect additions made during the intrusion.

### How can I use immudb? <a href="#how-can-i-use-immudb" id="how-can-i-use-immudb"></a>

Depending on your use case, immudb might function as your application's primary or as a secondary database. As a secondary, complimentary database, use immudb to cross-check the integrity of your important data (by verifying checksums or comparing stored data values). A secondary database enables you to quickly use immudb without completely re-engineering your existing application.

#### Use cases: <a href="#use-cases" id="use-cases"></a>

* Integration with your DevOps ensures code security throughout the development and deployment process. Embed immudb into your [Azure DevOps (opens new window)](https://codenotary.io/blog/securing-your-azure-devops-ecosystem-jenkins-and-kubernetes-aks/) with Jenkins and Kubernetes. Use just [Jenkins (opens new window)](https://codenotary.io/blog/jenkins-build-deployment-pipeline-a-how-to-for-ensuring-integrity/). Alternatively, integrate with [GitLab (opens new window)](https://codenotary.io/blog/fully-trusted-gitlab-pipeline/) or [GitHub (opens new window)](https://codenotary.io/blog/use-github-actions-for-validated-builds/).
* Guarantee [File Integrity (opens new window)](https://codenotary.io/blog/file-integrity-monitoring-change-management/) of your critical data. Examples include storing your organization's sensitive financial, credit card transactional, invoices, contracts, educational transcripts, and other important data.
* Ensure integrity of your legal [Documents and Invoices (opens new window)](https://codenotary.io/blog/immutably-store-or-guarantee-the-immutability-of-documents-and-invoices-for-compliance-reasons/), contracts, forms, and your downloads and emails.
* Save your Internet of Things (IoT) sensor data as a failsafe plan for loss of data.
* Keep your investment guidelines or stock market data tamperproof for your investment bank or client financial portfolios.
* Store important log files to keep them tamperproof to meet regulations like PCI compliance.
* Protect medical data, test results, or recipes from alteration.
* Companies use immudb to protect credit card transactions and to secure processes by storing digital certificates and checksums.
