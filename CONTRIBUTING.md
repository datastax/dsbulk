[![Build Status](https://travis-ci.com/riptano/dsbulk.svg?token=HsKMUW45xxTpytJgq1ku&branch=1.x)](https://travis-ci.com/riptano/dsbulk) [![Build status](https://ci.appveyor.com/api/projects/status/p7cicgkbyanmxlsl/branch/1.x?svg=true)](https://ci.appveyor.com/project/DataStax/dsbulk/branch/1.x)

# Before you contribute

## Documentation

Read the [manual](manual/) or the [online docs](https://docs.datastax.com/en/dsbulk/doc/). 

## Building

Install JDK8 and latest maven, and run the following command:

```
mvn package -Prelease
```

## Understanding DSBulk's architecture 

Make sure you understand how DSBulk works by reading the manual page about 
[DSBulk's architecture](https://docs.datastax.com/en/dsbulk/doc/dsbulk/dsbulkArch.html).

DSBulk code is divided in several modules:

1. [dsbulk-config](./config): This module contains utilities to configure DSBulk.
1. [dsbulk-url](./url): This module contains utilities to deal with URLs.
2. [dsbulk-codecs](./codecs): This module groups together submodules related to codec handling in 
   DSBulk:
    1. The [dsbulk-codecs-api](./codecs/api) submodule contains an API for codec handling in DSBulk.
    2. The [dsbulk-codecs-text](./codecs/text) submodule contains implementations of that API for 
       plain text and Json.
    3. The [dsbulk-codecs-jdk](./codecs/jdk) submodule contains implementations of that API for 
       converting to and from common JDK types: Boolean, Number, Temporal, Collections, etc.
3. [dsbulk-io](./io): This module contains utilities for reading and writing to 
   compressed files.
4. [dsbulk-connectors](./connectors): Connectors form a pluggable abstraction that allows DSBulk to 
   read and write to a variety of backends.
   1. The [dsbulk-connectors-api](./connectors/api) submodule contains the Connector API.
   2. The [dsbulk-connectors-commons](./connectors/commons) submodule contains common base classes 
      for text-based connectors.
   3. The [dsbulk-connectors-csv](./connectors/csv) submodule contains the CSV connector.
   4. The [dsbulk-connectors-json](./connectors/json) submodule contains the Json connector.
5. [dsbulk-cql](./cql): This module contains a lightweight ANTLR 4 grammar and parser for the CQL 
   language.
6. [dsbulk-mapping](./mapping): This module contains an ANTLR 4 grammar and parser for DSBulk's 
   mapping syntax.
7. [dsbulk-format](./format): This module contains utilities to format common DataStax Java driver 
   objects: statements and rows.
8. [dsbulk-partitioner](./partitioner): This module contains the `TokenRangeSplitter` API, that 
    DSBulk uses to parallelize reads by targeting all the token subranges simultaneously from 
    different replicas.
8. [dsbulk-sampler](./sampler): This module contains utilities to sample row and statement sizes.
9. [dsbulk-executor](./executor): DSBulk's Bulk Executor API is a pluggable abstraction that allows 
   to execute queries using different paradigms: synchronous, asynchronous and reactive programming. 
   DSBulk itself only uses the reactive paradigm.
    1. The [dsbulk-executor-api](./executor/api) submodule contains the Executor API.
    2. The [dsbulk-executor-reactor](./executor/reactor) submodule contains an implementation of 
       the Executor API using Reactor.
10. [dsbulk-workflow](./workflow): Workflows form a pluggable abstraction that allows DSBulk to 
   execute virtually any kind of operation.
    1. The [dsbulk-workflow-api](./workflow/api) submodule contains the Workflow API.
    2. The [dsbulk-workflow-commons](./workflow/commons) submodule contains common base classes for 
       workflows, and especially configuration utilities shared by DSBulk's built-in workflows 
       (load, unload and count).
    3. The [dsbulk-workflow-load](./workflow/load) submodule contains the Load Workflow.
    4. The [dsbulk-workflow-unload](./workflow/unload) submodule contains the Unload Workflow.
    5. The [dsbulk-workflow-count](./workflow/count) submodule contains the Count Workflow.
11. [dsbulk-runner](./runner): This module contains the DSBulk's runner, and a parser for command 
   lines.
12. [dsbulk-docs](./docs): This module generates DSBulk's in-tree documentation, template files and
    javadocs. 
13. [dsbulk-distribution](./distribution): This module assembles DSBulk's binary distributions.  
14. [dsbulk-tests](./tests): This module contains test utilities for DSBulk.

## Issue Management

DataStax Bulk Loader has its own [Jira project](https://datastax.jira.com/projects/DAT/summary), but
it's private to DataStax. For external contributors, feel free to open an issue in DSBulk's 
GitHub repository.

# Contribution guidelines

## Branching model

DSBulk uses [semantic versioning](http://semver.org/) and our development branches use the 
following scheme:

```
            1.0.1      1.0.2 ...                1.1.1 ...
         -----*----------*------> 1.0.x      -----*------> 1.1.x
        /                                   /
       /                                   /
      /                                   /
-----*-----------------------------------*-------------------------> 1.x
   1.0.0                               1.1.0        ...

Legend:
 > branch
 * tag
```

- new features are developed on "minor" branches such as `1.x`, where minor releases (ending in 
  `.0`) happen.
- bugfixes go to "patch" branches such as `1.0.x` and `1.1.x`, where patch releases (ending in 
  `.1`, `.2`...) happen.
- patch branches are regularly merged to the bottom (`1.0.x` to `1.x`) so that bugfixes are 
  applied to newer versions too.

Consequently, the branch having the highest major + minor version (in the format `x.x.x`) 
will be the branch to target bugfixes to. The branch in the format `x.x` which has the 
highest major will be the branch to target new features to.

## Code formatting

We follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). See
[here](https://github.com/google/google-java-format) for IDE plugins. The rules are not configurable.

The build will automatically format the code if necessary.

Some aspects are not covered by the formatter:

* imports: please configure your IDE to follow the guide (no wildcard imports, 
  static imports in ASCII sort order come first, followed by a blank line, 
  followed by normal imports in ASCII sort order).
* braces must be used with `if`, `else`, `for`, `do` and `while` statements, even when the body is
  empty or contains only a single statement.
* implementation comments: wrap them to respect the column limit of 100 characters.

XML files are formatted with a Maven plugin as well. Make sure your IDE follows the rule below:

* Indent with two spaces and wrap to respect the column limit of 100 characters.

## Coding style -- production code

Use static imports only when they do not make your code hard to understand; 
this usually depends on how meaningful is the imported member's name. For example, 
a static import of `TimeUnit.SECONDS` is acceptable: `future.get(1, SECONDS)`; 
the static import of `IntStream.of` should however be avoided as the resulting 
statement becomes unintelligible:  `of()`.

Avoid abbreviations in class and variable names. A good rule of thumb is that you should only use
them if you would also do so verbally, for example "id" and "config" are probably reasonable.
Single-letter variables are permissible if the variable scope is only a few lines, or for commonly
understood cases (like `i` for a loop index).

Acronyms: use capital letters for acronyms with up to 3 letters (`CSV`, `CQL`, `XML`); 
use capitalized words for acronyms of 4 letters and more (`Json`).

Keep source files short. Short files are easy to understand and test. The average should probably 
be around 200-300 lines. 

### Javadoc

You don't need to document every parameter or return type, or even every method. Don't document 
something if it is completely obvious, we don't want to end up with this:

```java
/**
 * Returns the name.
 * 
 * @return the name
 */
String getName();
```

On the other hand, there is often something useful to say about a method, so most should have at
least a one-line comment. Use common sense.

Users importing the DataStax Bulk API should find the right documentation at the right time 
using their IDE. Try to think of how they will come into contact with the class. 
For example, if a type is constructed with a builder, each builder method should probably explain 
what the default is when you don't call it.

Avoid using too many links, they can make comments harder to read, especially in the IDE. Link to a
type the first time it's mentioned, then use a text description ("this registry"...) or an `@code`
block. Don't link to a class in its own documentation. Don't link to types that appear right below
in the documented item's signature.

```java
/**
* @return this {@link Builder} <-- completely unnecessary
*/
Builder withLimit(int limit) {
```

### Logs

We use SLF4J; loggers are declared like this:

```java
private static final Logger LOGGER = LoggerFactory.getLogger(TheEnclosingClass.class);
```

Logs are intended for two personae:

* Ops who manage the application in production.
* Developers (maybe you) who debug a particular issue.

The first 3 log levels are for ops:

* `ERROR`: something that renders the loader&nbsp;— or a part of it&nbsp;— completely unusable. An 
  action is required to fix it, and a re-run should probably be scheduled.
* `WARN`: something that the loader can recover from automatically, but indicates a configuration or
  programming error that should be addressed. For example: the loader cannot read a file that was 
  found inside the directory to scan.
* `INFO`: something that is part of the normal operation of the loader, but might be useful to know
  for an operator. For example: the loader has initialized successfully and is ready to ingest
  records, or the loader finished loading successfully in X minutes.

The last 2 levels are for developers:

* `DEBUG`: anything that would be useful to understand what the loader is doing from a "black box"
  perspective, i.e. if all you have are the logs.
* `TRACE`: same thing, but for events that happen very often, produce a lot of output, or should be
  irrelevant most of the time (this is a bit more subjective and left to your interpretation).

Tests are run with the Logback configuration file defined for each module in 
`src/test/resources/logback-test.xml`. The default level for all loggers is `OFF`, which means that 
no log output is produced, but you can override it with a system property: `-Dlog.root.level=DEBUG`.
A nice setup is to use `DEBUG` when you run from your IDE, and keep the default for the command
line. Also, some modules use the special `NOPAppender` by default, to reduce clutter printed on the
console; you can temporarily change that appender to a regular `ConsoleAppender`, especially while
debugging tests, but avoid committing that change.

When you add or review new code, take a moment to run the tests in `DEBUG` mode and check if the
output looks good.

### Fair usage of Stream API

Please use the Stream API with parsimony. Streams were designed for *data processing*, not to make 
collection traversals "functional".

### Never assume a specific format for `toString()`

Only use `toString()` for debug logs or exception messages, and always assume that its format is
unspecified and can change at any time.
  
If you need a specific string representation for a class, make it a dedicated method with a
documented format, for example `asCQLQueryString`. Otherwise it's too easy to lose track of 
the intended usage and break things: for example, someone modifies your `toString()` method 
to make their logs prettier, but unintentionally breaks the script export feature that expected 
it to produce CQL literals.
 
`toString()` can delegate to `asCQLQueryString()` if that is appropriate for logs. 

## Coding style — test code

Static imports are encouraged in a couple of places to make assertions more fluid:

* All AssertJ's methods.
* All Mockito methods.

Test methods names use lower snake case, generally start with `should`, and clearly indicate the
purpose of the test, for example: `should_fail_if_key_already_exists`. If you have trouble coming 
up with a simple name, it might be a sign that your test does too much, and should be split.

Tests are encouraged to use the given-when-then pattern, but that is not a hard requirement.

We use AssertJ (`assertThat`) for assertions. Don't use jUnit assertions (`assertEquals`,
`assertNull`, etc).

Don't try to generify at all cost: a bit of duplication is acceptable, if that helps keep the tests
simple to understand (a newcomer should be able to understand how to fix a failing test without
having to read too much code).

Test classes can be a bit longer, since they often enumerate similar test cases. You can also
factor some common code in a parent abstract class named with "XxxTestBase", and then split
different families of tests into separate child classes.

## License headers

The build will automatically add or update license headers if they are missing or outdated. To 
update all license headers from the command line at once, run:

```
mvn license:format
```

## Commits

Keep your changes **focused**. Each commit should have a single, clear purpose expressed in its 
message. (Note: these rules can be somewhat relaxed during the initial development phase, when
adding a feature sometimes requires other semi-related features).

Resist the urge to "fix" cosmetic issues (add/remove blank lines, etc.) in existing code. This adds
cognitive load for reviewers, who have to figure out which changes are relevant to the actual
issue. If you see legitimate issues, like typos, address them in a separate commit (it's fine to
group multiple typo fixes in a single commit).

Commit message subjects start with a capital letter, use the imperative form and do **not** end
with a period:

* correct: "Add test for CQL request handler"
* incorrect: "~~Added test for CQL request handler~~"
* incorrect: "~~New test for CQL request handler~~"

Avoid catch-all messages like "Minor cleanup", "Various fixes", etc. They don't provide any useful
information to reviewers, and might be a sign that your commit contains unrelated changes.
 
We don't enforce a particular subject line length limit, but try to keep it short. 
Github likes it when commit lines are less than 80 characters long.

You can add more details after the subject line, separated by a blank line. The following pattern
(inspired by [Netty](http://netty.io/wiki/writing-a-commit-message.html)) is not mandatory, but
welcome for complex changes:

```
One line description of your change
 
Motivation:

Explain here the context, and why you're making that change.
What is the problem you're trying to solve.
 
Modifications:

Describe the modifications you've done.
 
Result:

After your change, what will change.
```

## Pull requests

Like commits, pull requests should be focused on a single, clearly stated goal.

### Opening a pull request

Name your pull request branches after their corresponding Jira issue where applicable, 
e.g. "DAT-42".

Before you send your pull request, make sure that you have a unit test that failed before 
the fix and succeeds after.

You will also need to sign the [DataStax Contribution License Agreement](https://cla.datastax.com/).

If your pull requests addresses a Jira issue, add the corresponding entry to the 
[changelog](./changelog/README.md), in the following format:

- [_issue type_] DAT-XXX: _issue title_.

Example:

- [new feature] DAT-14: Implement configuration service.

The first commit message should reference the JIRA issue for automatic linking where applicable. 
The recommended template for the commit title is:

    DAT-XXX: Issue title

See the "Commits" section above for more tips for writing good commit messages.

Avoid basing a pull request onto another one. If possible, the first one should be merged in first,
and only then should the second one be created.

### Adding commits

If you have to address feedback, avoid rebasing your branch and force-pushing (this makes the
reviewers' job harder, because they have to re-read the full diff and figure out where your new
changes are). Instead, push a new commit on top of the existing history; it will be squashed later
when the PR gets merged. If the history is complex, it's a good idea to indicate in the message
where the changes should be squashed:

```
* 20c88f4 - Address feedback (to squash with "Add metadata parsing logic") (36 minutes ago)
* 7044739 - Fix various typos in Javadocs (2 days ago)
* 574dd08 - Add metadata parsing logic (2 days ago)
```

(Note that the message refers to the other commit's subject line, not the SHA-1.)

### Merging pull requests

We require at least one approval by a peer reviewer.
When the pull request is finally approved, squash all commits together to have a cleaner history
before merging. It is OK to delete the branch after merging.

# Building from Source

If you would like to build `dsbulk` from source, you will need the following:

* Java 8. Later versions of Java may be able to run `dsbulk`, but 8 is required for building.
* Maven 3.x.

From the root directory, run `mvn install`. This is the basic build and pretty much just makes sure
that everything compiles and all the tests pass. To build the _.tar.gz_ or _.zip_ file that can be
distributed to end users for execution, run `mvn install -Prelease`. The tarred/zipped files will
appear in _distribution/target_, from which you can extract and run your updated code on your test
cases.

## Building Documentation

Note that the build process automatically updates certain documentation files. For example, the
documents in the [manual](./manual) are generated from the config templates for
[dsbulk options](./workflow/commons/src/main/resources/dsbulk-reference.conf) and
[driver options](./workflow/commons/src/main/resources/driver-reference.conf).
