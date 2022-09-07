# Security Policy

## Table of contents

1. Introduction
2. Scope
3. Supported Versions
4. Reporting a Vulnerability
5. Guidelines
6. Test methods
7. Authorization
8. Questions

## Introduction

Codenotary, September 16th 2022

*This security policy is based on [this template](https://www.cisa.gov/vulnerability-disclosure-policy-template).*

Codenotary is committed to ensuring the security of the global public by protecting their information. This policy is intended to give security researchers clear guidelines for conducting vulnerability discovery activities and to convey our preferences in how to submit discovered vulnerabilities to us.

This policy is addressed to people using the immudb and / or contributing to the project. It describes **what systems and types of research** are covered under this policy, **how to send** us vulnerability reports, and **how long** we ask security researchers to wait before publicly disclosing vulnerabilities.

We encourage you to contact us to report potential vulnerabilities in our systems.

## Scope

This policy applies to the following systems and services:

- immudb database core application, 
- immuadmin and immuclient applications
- golang immudb SDK

Source code of the above tools [is available on GH](https://github.com/codenotary/immudb).
Also:

- Java immudb SDK ([repo link](https://github.com/codenotary/immudb4j))
- Python immudb SDK ([repo link](https://github.com/codenotary/immudb-py))
- .Net immudb SDK ([repo link](https://github.com/codenotary/immudb4net))
- Nodejs SDK ([repo link](https://github.com/codenotary/immudb-node))

Any service not expressly listed above, such as any connected services, are excluded from scope and are not authorized for testing. Additionally, vulnerabilities found in systems from our vendors fall outside of this policy’s scope and should be reported directly to the vendor according to their disclosure policy (if any). If you aren’t sure whether a system is in scope or not, contact us at (immudb-security at codenotary.com) before starting your research (or at the security contact for the system’s domain name listed in the .gov WHOIS).

Though we develop and maintain other internet-accessible systems or services, we ask that active research and testing only be conducted on the systems and services covered by the scope of this document. If there is a particular system not in scope that you think merits testing, please contact us to discuss it first. We will increase the scope of this policy over time.

Other immudb SDK repositories that are not covered by this policy:

- Ruby on rails SDK ([repo link](https://github.com/ankane/immudb-ruby))

## Supported Versions

Only the latest version will be supported with security updates.

## Reporting a vulnerability

**IMPORTANT: Do not file public issues on GitHub for security vulnerabilities.**

We accept vulnerability reports via (immudb-security at codenotary.com). Reports may be submitted anonymously. If you share contact information, we will acknowledge receipt of your report within 3 business days.

We do not support PGP-encrypted emails.

In order to help us triage and prioritize submissions, we recommend that your reports:

- Describe the location the vulnerability was discovered and the potential impact of exploitation.
- Offer a detailed description of the steps needed to reproduce the vulnerability (proof of concept scripts or screenshots are helpful).
- Explain how the vulnerability affects immudb usage and an estimation of the attack surface, if there is one.
- List other projects or dependencies that were used in conjunction with Pinniped to produce the vulnerability.
- Be in English, if possible.

What you can expect from us:

- When you choose to share your contact information with us, we commit to coordinating with you as openly and as quickly as possible.
- Within 3 business days, we will acknowledge that your report has been received.
- To the best of our ability, we will confirm the existence of the vulnerability to you and be as transparent as possible about what steps we are taking during the remediation process, including on issues or challenges that may delay resolution.
- A public disclosure date is negotiated by the immudb team, the SDK developers and the bug submitter. We prefer to fully disclose the bug as soon as possible once a user mitigation or patch is available. It is reasonable to delay disclosure when the bug or the fix is not yet fully understood, the solution is not well-tested, or for distributor coordination. The timeframe for disclosure is from immediate (especially if it’s already publicly known) to a maximum of 90 business days.  

We will maintain an open dialogue to discuss issues.

## Guidelines

Under this policy, “research” means activities in which you:

- Notify us as soon as possible after you discover a real or potential security issue.
- Make every effort to avoid privacy violations, degradation of user experience, disruption to production systems, and destruction or manipulation of data.
- Only use exploits to the extent necessary to confirm a vulnerability’s presence. Do not use an exploit to compromise or exfiltrate data, establish persistent command line access, or use the exploit to pivot to other systems.
- Provide us a reasonable amount of time to resolve the issue before you disclose it publicly.
- Do not submit a high volume of low-quality reports.

Once you’ve established that a vulnerability exists or encounter any sensitive data (including personally identifiable information, financial information, or proprietary information or trade secrets of any party), **you must stop your test, notify us immediately, and not disclose this data to anyone else.**

## Test methods

The following test methods are not authorized:

- Network denial of service (DoS or DDoS) tests or other tests that impair access to or damage a system or data
- Physical testing (e.g. office access, open doors, tailgating), social engineering (e.g. phishing, vishing), or any other non-technical vulnerability testing

## Authorization

If you make a good faith effort to comply with this policy during your security research, we will consider your research to be authorized, we will work with you to understand and resolve the issue quickly, and Codenotary will not recommend or pursue legal action related to your research. Should legal action be initiated by a third party against you for activities that were conducted in accordance with this policy, we will make this authorization known.

## Questions

Questions regarding this policy may be sent to immudb-security at codenotary.com. We also invite you to contact us with suggestions for improving this policy.

## Document change history

| Version Date | Description          |
| ------- | ------------------ |
| Sept. 7th 2022   | First version |
| Sept. 16th 2022   | Reorganization and corrections |
| October. 14th 2022   | Supported versions |
| October, 18th 2022   | Commit following Conventional Commit specification |
