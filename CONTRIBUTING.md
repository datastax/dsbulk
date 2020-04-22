[![Build Status](https://travis-ci.com/riptano/dsbulk.svg?token=HsKMUW45xxTpytJgq1ku&branch=1.x)](https://travis-ci.com/riptano/dsbulk) [![Build status](https://ci.appveyor.com/api/projects/status/p7cicgkbyanmxlsl/branch/1.x?svg=true)](https://ci.appveyor.com/project/DataStax/dsbulk/branch/1.x)

# Before you contribute

## Documentation

Read the [manual](manual/) or the [online docs](https://docs.datastax.com/en/dsbulk/doc/). 

## Issue Management

DataStax Bulk Loader has its own [Jira project](https://datastax.jira.com/projects/DAT/summary).

# Contribution guidelines

## Branching model

DSBulk uses [semantic versioning](http://semver.org/) and our development branches use the following scheme:

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

- new features are developed on "minor" branches such as `1.x`, where minor releases (ending in `.0`) happen.
- bugfixes go to "patch" branches such as `1.0.x` and `1.1.x`, where patch releases (ending in `.1`, `.2`...) happen.
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
* XML files: indent with two spaces and wrap to respect the column limit of 100 characters.

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

All types in "API" packages must be documented. For "internal" packages, documentation is optional,
but in no way discouraged: it's generally a good idea to have a class-level comment that explains
where the component fits in the architecture, and anything else that you feel is important.

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
For example, if a type is constructed with
a builder, each builder method should probably explain what the default is when you don't call it.

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

* `ERROR`: something that renders the loader&nbsp;— or a part of it&nbsp;— completely unusable. An action is
  required to fix it, and a re-run should probably be scheduled.
* `WARN`: something that the loader can recover from automatically, but indicates a configuration or
  programming error that should be addressed. For example: the loader cannot read a file that was found inside the directory to scan.
* `INFO`: something that is part of the normal operation of the loader, but might be useful to know
  for an operator. For example: the loader has initialized successfully and is ready to ingest
  records, or the loader finished loading successfully in X minutes.

The last 2 levels are for developers:

* `DEBUG`: anything that would be useful to understand what the loader is doing from a "black box"
  perspective, i.e. if all you have are the logs.
* `TRACE`: same thing, but for events that happen very often, produce a lot of output, or should be
  irrelevant most of the time (this is a bit more subjective and left to your interpretation).

Tests are run with the Logback configuration file defined for each module in `src/test/resources/logback-test.xml`. 
The default level for all loggers is `OFF`, which means that no log output is produced, 
but you can override it with a system property: `-Dlog.root.level=DEBUG`.
A nice setup is to use `DEBUG` when you run from your IDE, and keep the default for the command
line.

When you add or review new code, take a moment to run the tests in `DEBUG` mode and check if the
output looks good.

### Fair usage of Stream API

Please use the Stream API with parsimony. Streams were designed for *data 
processing*, not to make collection traversals "functional".

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

* AssertJ's `assertThat` / `fail`.
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

The build will fail if some license headers are missing. To update all files from the command line,
run:

```
mvn license:format
```

## Pre-commit hook
 
Ensure `pre-commit.sh` is executable, then install it:

```
chmod +x ./pre-commit.sh
ln -s pre-commit.sh .git/hooks/pre-commit
```

The pre-commit hook will only allow commits if:
 
* All files to be committed have the expected license headers;
* All source files to be committed are properly formatted;
* All unit tests pass. 

Note that the pre-commit hook will stash all files that are not staged
for commit, then restore them. This way, the hook script tests
really what is about to be committed.

The pre-commit hook is also a good reminder to keep the test suite short.

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

If your pull requests addresses a Jira issue, add the corresponding entry to the 
[changelog](./changelog/README.md), in the following format:

- [_issue type_] DAT-XXX: _issue title_.

Example:

- [new feature] DAT-14: Implement configuration service.

The first commit message should reference the JIRA issue for automatic linking where applicable. The recommended template for the commit title is:

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
