# Contributing to Atlas

First off, thanks for taking the time to contribute!

The following is a set of guidelines for contributing to Atlas. Use your best judgment, and
feel free to propose changes to this document in a pull request.

**Table of Contents**

[How can I contribute?](#how-can-i-contribute)
* [Asking Questions](#asking-questions)
* [Reporting Issues or Feature Requests](#reporting-bugs)
* [Contributing Code](#contributing-code)

[Guides](#guides)
* [Issue Labels](#issue-labels)
* [Git Commit Messages](#git-commit-messages)
* [Build and Test](#build-and-test)
* [License Headers](#license-headers)
* [Scalafmt](#scalafmt)
* [Versions and Compatibility](#versions-and-compatibility)
* [Updating Documentation](#updating-documentation)

## How can I contribute?

### Asking Questions

If you have a question, then you can ask on the [mailing list] or by filing an [issue]. There
is not a strong preference among the developers, however, users that are not actively involved
in development may be more likely to see the question on the mailing list.

[mailing list]: https://groups.google.com/forum/#!forum/netflix-atlas
[issue]: https://github.com/Netflix/atlas/issues

### Reporting Issues or Feature Requests

Issues and feature requests are managed via [GitHub issues][issue]. When filing an issue for
a bug, we would appreciate if you check open issues to see if there are any similar requests.
If there is a match, then 

When **reporting a bug**, then please include the following:

* Expected results
* Actual results
* Exact steps to reproduce the problem, bonus points for providing a failing unit test

When **requesting a feature**, then please try to answer the following:

* What does this allow a user to accomplish that they cannot do now?
* How urgent is the need?
* Does it align with the goals of Atlas?

### Contributing Code

[APLv2]: https://github.com/Netflix/atlas/blob/master/LICENSE

By contributing code, you agree to license your contribution under the terms of the [APLv2].
To submit code:

* Create a fork of the project (this includes Netflix contributors, do not push branches
  directly to the main repository)
* Create a branch for your change
* Make changes and add tests
* Commit the changes following the [commit guidelines](#git-commit-messages)
* Push the branch with your changes to your fork
* Open a pull request against the Atlas project

#### Testing

Where possible, test cases should be added to cover the new functionality or bug being
fixed. Test cases should be small, focused, and quick to executed.

#### Pull Requests

The following guidelines are to help ensure that pull requests (PRs) are easy to review and
comprehend.

* **One PR addresses one problem**, conflating issues in the same PR makes it more difficult
  to review and merge.
* **One commit per PR**, the final merge should have a single commit with a
  [good commit message](#git-commit-messages). Note, we can squash and merge via GitHub
  so it is fine to have many commits while working through the change and have us squash
  when it is complete. The exception is dependency updates where the
  only change is a dependency version. We typically do these as a batch with separate commits
  per version and merge without squashing. For this case, separate commits can be useful to
  allow for a git bisect to pinpoint a problem starting with a dependency change.
* **Reference related or fixed issues**, this helps us get more context for the change.
* **Partial work is welcome**, submit with a title including `[WIP]` (work in progress) to
  indicate it is not yet ready.
* **Keep us updated**, we will try our best to review and merge incoming PRs. We may close
  PRs after 30 days of inactivity. This covers cases like: failing tests, unresolved conflicts
  against master branch or unaddressed review comments.

## Guides

### Issue Labels

For [issues][issue] we use the following labels to quickly categorize issues:

| Label Name     | Description                                                               |
|----------------|---------------------------------------------------------------------------|
| `bug`          | Confirmed bugs or reports that are very likely to be bugs.                |
| `enhancement`  | Feature requests.                                                         |
| `discussion`   | Requests for comment to figure out the direction.                         |
| `help wanted`  | Help from the community would be appreciated. Good first issues.          |
| `question`     | Questions more than bug reports or feature requests (e.g. how do I do X). |

### Git Commit Messages

Commit messages should try to follow these guidelines:

* First line is no more than 50 characters and describes the changeset.
* The body of the commit message should include a more detailed explanation of the change.
  It is ok to use markdown formatting in the explanation.

More information can be found in the [Git docs]. Sample message:

```
Short (50 chars or less) summary of changes

More detailed explanatory text, if necessary.  Wrap it to
about 72 characters or so.  In some contexts, the first
line is treated as the subject of an email and the rest of
the text as the body.  The blank line separating the
summary from the body is critical (unless you omit the body
entirely); tools like rebase can get confused if you run
the two together.

Further paragraphs come after blank lines.

  - Bullet points are okay, too

  - Typically a hyphen or asterisk is used for the bullet,
    preceded by a single space, with blank lines in
    between, but conventions vary here
```

[Git docs]: https://git-scm.com/book/en/v2/Distributed-Git-Contributing-to-a-Project

### Build and Test

The Atlas build uses SBT. If you do not already have it installed, then you can use the
included launcher script. To do a basic build and run tests:

```
$ project/sbt test
```

There is also a makeful included that runs SBT with some convenient targets.
To reproduce the validation done for PR builds locally including verification of
[license headers](#license-headers) and [formatting](#scalafmt), just run:

```
$ make
```

For making changes you are welcome to use whatever editor you are comfortable with. Most
current developers on the project use Intellij IDEA.

### License Headers

Atlas is licensed under the terms of the [APLv2]. License headers must be included on source
files and that is checked as part of the PR validation. To check license headers locally:

```
$ project/sbt checkLicenseHeaders
```

The headers can be automatically added or fixed by running:

```
$ project/sbt formatLicenseHeaders
```

### Scalafmt

We use [scalafmt] to ensure a base level of consistency across the project. This is checked
as part of PR validation to help ensure the format is maintained over time and avoid ruining
the git history with occasional reformatting runs. To check the format locally:

```
$ project/sbt scalafmt::test test:scalafmt::test
```

To fix the formatting:

```
$ project/sbt scalafmt test:scalafmt
```

[scalafmt]: http://scalameta.org/scalafmt/

### Versions and Compatibility

A frequent question is what version of Atlas is in use at Netflix and what compatibility
guarantees we make between versions.

#### Version Numbers

The Atlas version has three parts:

```
[major].[minor].[patch]
```

These are currently used to indicate the following:

* **major**, indicates compatibility of the HTTP endpoints. Atlas is a hosted service
  and most users at Netflix do not link with the code, but access the web APIs that are
  exposed. We try hard to avoid making incompatible changes at this layer as it is highly
  disruptive to many teams at Netflix.
* **minor**, indicates compatibility of the libraries that make up Atlas. For the in-progress
  release we do not make any compatibility guarantees to give us more flexibility with updating
  the software and making performance improvements.
* **patch**, bug fixes and minor changes for stable releases. There should be backwards
  compatibility from one patch release to the next. In most cases there will also be forwards
  compatibility, but we do not test or verify.
  
If you need stability and are using the Atlas libraries directly, then use one of the
stable patch releases.

#### What version does Netflix use?

There are typically several versions of Atlas in use at Netflix. Atlas is a hosted service
that is operated by a single team. We are using a mix of versions from the latest in progress
release, 1.6.0 when this was written. Snapshots are created for every commit into master and
some of our services pull in the snapshot to test and verify new functionality in our
environment. Occasionally when there is a need, we'll cut a release candidate for the in progress
version so there is a more stable point depend on for use-cases where we need more stability
than being on the latest snapshot.

Once a final release is made, 1.5.x is the latest stable release right now, it should be stable
with no breaking changes for at least the next release cycle. These can be considered long-term
stable releases. They will receive bug fixes, but no new features. These are used for projects
that do not want to keep up with the churn and potential breakage of using the latest in progress
version.

### Updating Documentation

The main documentation for using Atlas is on the [GitHub wiki]. This wiki is not directly
editable for several reasons:

1. We have had problems with spammers in the past putting up bogus pages
2. GitHub does not allow pull requests for the wiki
3. A number of the tedious steps like including sample graphs and formatting expressions are
   easier to do with some scripting
   
The documentation is kept inline with the code as part of the [atlas-wiki] sub-project. To
update the documentation, update the content in that sub-project and send in a PR just as you
would for [contributing code](#contributing-code). Once the PR has been reviewed and merged, then
one of the project maintainers will need to publish the changes to the wiki repository by
running:

```
$ make publish-wiki
```

[GitHub wiki]: https://github.com/Netflix/atlas/wiki
[atlas-wiki]: https://github.com/Netflix/atlas/tree/master/atlas-wiki/src/main
