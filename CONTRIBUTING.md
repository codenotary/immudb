<!--
---
title: "Contributing"
custom_edit_url: https://github.com/codenotary/immudb/edit/master/CONTRIBUTING.md
---
-->

# Contributing to immudb
​
👍🎉 First off, thanks for taking the time to contribute! 🎉👍

The following is a set of guidelines for contributing to immudb. These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.


### Active Participation
​
Open Source projects are maintained and backed by a **wonderful community** of users and collaborators. ​
We encourage you to actively participate in the development and future of immudb by contributing to the source code regularly, improving the documentation, reporting potential bugs, or testing new features.
​
### Channels
​
There are many ways to take part in the immudb community.
​
1. <a href="https://github.com/codenotary/immudb" rel="nofollow" target="_blank">Github Repositories</a>: Report bugs or create feature requests against the dedicated immudb repository.
2. <a href="https://discord.gg/ThSJxNEHhZ" rel="nofollow" target="_blank">Discord</a>: Join the Discord channel and chat with other developers in the immudb community.
3. <a href="https://twitter.com/immudb" rel="nofollow" target="_blank">Twitter</a>: Stay in touch with the progress we make and learn about the awesome things happening around immudb.
​
## Developer Certificate of Origin

Contributions to this project must be accompanied by a Developer Certificate of Origin (DCO). You retain the copyright to your contribution; this simply gives us permission to use and redistribute your contributions as part of the project.
Therefore, with every contribution to the code, you must sign-off the following DCO:

By making a contribution to this project, I certify that:

    a) The contribution was created in whole or in part by me and I have the right to submit it under the open source license indicated in the file; or
    b) The contribution is based upon previous work that, to the best of my knowledge, is covered under an appropriate open source license and I have the right under that license to submit that work with modifications, whether created in whole or in part by me, under the same open source license (unless I am permitted to submit under a different license), as indicated in the file; or
    c) The contribution was provided directly to me by some other person who certified (a), (b) or (c) and I have not modified it.
    d) I understand and agree that this project and the contribution are public and that a record of the contribution (including all personal information I submit with it, including my sign-off) is maintained indefinitely and may be redistributed consistent with this project or the open source license(s) involved.

You can sign-off that you adhere to these requirements by simply adding a signed-off-by line to your commit message, as specified in the [pull request guidelines](#pull-requests).

## Using the Issue Tracker
​
The [issue tracker](https://github.com/codenotary/immudb/issues) is
the preferred channel for [bug reports](#bug-reports), [feature requests](#feature-requests),
and [pull requests](#pull-requests), but please respect the following restrictions:
​
* Please **do not** use the issue tracker for personal support requests.
​
* Please **do not** get off track in issues. Keep the discussion on topic and
  respect the opinions of others.
​
* Please **do not** post comments consisting solely of "+1" or ":thumbsup:".
  Use [GitHub's "reactions" feature](https://github.com/blog/2119-add-reactions-to-pull-requests-issues- and-comments) instead. We reserve the right to delete comments which violate this rule.
​
## Issues and Labels
​
Our bug tracker utilizes several labels to help organize and identify issues. For a complete look at our labels, see the [project labels page](https://github.com/codenotary/immudb/labels).
​
## Bug Reports
​
A bug is a _demonstrable problem_ that is caused by the code in the repository.
Good bug reports are extremely helpful, so thanks!
​
Guidelines for bug reports:
​
1. Provide a clear title and description of the issue.
2. Share the version of immudb you are using.
3. Add code examples to demonstrate the issue. You can also provide a complete repository to reproduce the issue quickly.
​
A good bug report shouldn't leave others needing to chase you up for more information. Please try to be as detailed as possible in your report:
​
- What is your environment?
- What steps will reproduce the issue?
- What OS experiences the problem?
- What would you expect to be the outcome?
​
All these details will help us fix any potential bugs. Remember, fixing bugs takes time. We're doing our best!
​
Example:
​
> Short and descriptive example bug report title
>
> A summary of the issue and the OS environment in which it occurs. If
> suitable, include the steps required to reproduce the bug.
>
> 1. This is the first step
> 2. This is the second step
> 3. Further steps, etc.
>
> `<url>` - a link to the reduced test case
>
> Any other information you want to share that is relevant to the issue being
> reported. This might include the lines of code that you have identified as
> causing the bug, and potential solutions (and your opinions on their
> merits).
​
## Feature Requests
Feature requests are welcome! When opening a feature request, it's up to *you* to make a strong case to convince the project's developers of the merits of this feature. Please provide as much detail and context as possible.
​
When adding a new feature to immudb, make sure you update the documentation as well.
​
### Testing
Before providing a pull request, be sure to test the feature you are adding. We will only approve pull requests with at least 80% of code covered by unit tests.
​
## Pull Requests
Good pull requests—patches, improvements, new features are a fantastic
help. They should remain focused in scope and avoid containing unrelated
commits.
​
**Please ask first** before starting on any significant pull request (e.g.
implementing features, refactoring code, porting to a different language),
otherwise you might spend a lot of time working on something that the
project's developers might not want to merge into the project.
​
Please adhere to the [code guidelines](#code-guidelines) used throughout the
project (indentation, accurate comments, etc.) and any other requirements
(such as [test coverage](#testing)).
​
Adhering to the following process is the best way to get your work
included in the project:
​
1. [Fork](https://help.github.com/fork-a-repo/) the project, clone your fork and configure the remotes:
​
   ```bash
   # Clone your fork of the repo into the current directory
   git clone https://github.com/<your-username>/immudb.git
   
2. Navigate to the newly cloned directory

   ```bash
   cd immudb
   ```
3. Assign the original repo to a remote called "upstream"
   ```
   git remote add upstream https://github.com/codenotary/immudb.git
   ```

4. Create a new topic branch (off the main project master branch) to
   contain your feature, change, or fix:
​
   ```bash
   git checkout -b <topic-branch-name>
   ```

5. Make sure your commits are logically structured. Please adhere to these [git commit
   message guidelines](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html). Use Git's
   [interactive rebase feature](https://help.github.com/en/github/using-git/about-git-rebase)
to tidy up your commits before making them public. For immudb, we follow the [conventional commits layout](https://www.conventionalcommits.org/en/v1.0.0/). This way commits are more meaningful and the autogenerated part of the readme is better structured.

6. Sign-off that you adhere to the [DCO](#developer-certificate-of-origin)    by adding a signed-off-by line to your commit message, separated by a blank line from the body of the commit.
This is my commit message

   ```bash
   Signed-off-by: Your Name <your.email@example.org>
   ```
   Git even has a -s or -s –amend (in case you already have a commit) command line option to append this automatically to your existing commit message:
   ```bash
   $ git commit -s -m 'This is my commit message'
   ```
   Signed-off-by: Your Name <your.email@example.org> to the last line of each Git commit message

7. Locally rebase the upstream master branch into your topic branch:
​
   ```bash
   git pull --rebase upstream master
   ```
8. Push your topic branch up to your fork:
​
   ```bash
   git push origin <topic-branch-name>
   ```
9. [Open a pull request](https://help.github.com/articles/using-pull-requests/)
    with a clear title and description against the `master` branch.
​
​
## Code Guidelines
​
### Go

Here is a non-exhaustive list of documents and articles talking about Go best practices and tips & tricks. We are continuously trying to improve our code, and taking inspiration from community works helps us grow.

* https://golang.org/doc/effective_go.html
* https://github.com/golang/go/wiki/CodeReviewComments
* https://go-proverbs.github.io/
* https://the-zen-of-go.netlify.app/
* https://dave.cheney.net/practical-go/presentations/qcon-china.html
* https://github.com/bahlo/go-styleguide
* https://github.com/Pungyeon/clean-go-article
* https://github.com/dgryski/go-perfbook

Please note again that [code coverage](#testing) should no less than 80% and that we encourage you to minimize the use of 3rd party libraries.
​
### Vue
​
Adhere to the linting and [concepts](https://immudb.io/docs/preface/concepts) guidelines.
​
- Prefix immudb components with the `I` character
- Provide multiple customization options
- Use mixins where applicable
​
## Local Development
​
Fork the repository and create a branch as specified in the [pull request guidelines](#pull-requests) above.
​
## License
​
By contributing your code, you agree to license your contribution under the [Apache Version 2.0 License](https://github.com/codenotary/immudb/tree/master/LICENSE).
