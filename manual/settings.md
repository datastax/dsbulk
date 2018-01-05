# DataStax Bulk Loader Options

*NOTE:* The long options described here can be persisted in `conf/application.conf` and thus permanently override defaults and avoid specifying options on the command line.

A template configuration file can be found [here](./application.template.conf).

## Sections

<a href="#Common">Common Settings</a><br>
<a href="#connector">Connector Settings</a><br>
&nbsp;&nbsp;&nbsp;<a href="#connector.csv">Connector Csv Settings</a><br>
&nbsp;&nbsp;&nbsp;<a href="#connector.json">Connector Json Settings</a><br>
<a href="#schema">Schema Settings</a><br>
<a href="#batch">Batch Settings</a><br>
<a href="#codec">Codec Settings</a><br>
<a href="#driver">Driver Settings</a><br>
&nbsp;&nbsp;&nbsp;<a href="#driver.auth">Driver Auth Settings</a><br>
&nbsp;&nbsp;&nbsp;<a href="#driver.policy">Driver Policy Settings</a><br>
&nbsp;&nbsp;&nbsp;<a href="#driver.pooling">Driver Pooling Settings</a><br>
&nbsp;&nbsp;&nbsp;<a href="#driver.protocol">Driver Protocol Settings</a><br>
&nbsp;&nbsp;&nbsp;<a href="#driver.query">Driver Query Settings</a><br>
&nbsp;&nbsp;&nbsp;<a href="#driver.socket">Driver Socket Settings</a><br>
&nbsp;&nbsp;&nbsp;<a href="#driver.ssl">Driver Ssl Settings</a><br>
<a href="#engine">Engine Settings</a><br>
<a href="#executor">Executor Settings</a><br>
<a href="#log">Log Settings</a><br>
<a href="#monitoring">Monitoring Settings</a><br>
<a name="Common"></a>
## Common Settings

#### -f _&lt;string&gt;_

Load settings from the given file rather than `conf/application.conf`.

#### -c,--connector.name _&lt;string&gt;_

The name of the connector to use.

It is used in two places:

1. The path to the group of settings for the connector are located under `connector.<name>`.
2. The connector class name must start with `name`, case-insensitively. It is permitted for `name` to be the fully-qualified class name of the connector. That simply implies that the settings root will be at that fully-qualified location.

Example: `csv` for class `CSVConnector`, with settings located under `connector.csv`.

Default: **"csv"**.

#### -url,--connector.csv.url _&lt;string&gt;_

The URL or path of the resource(s) to read from or write to.

Which URL protocols are available depend on which URL stream handlers have been installed, but at least the **file** protocol is guaranteed to be supported.

The file protocol can be used with all supported file systems, local or not.
- **When reading**: the URL can point to a single file, or to an existing directory; in case of a directory, the *fileNamePattern* setting can be used to filter files to read, and the *recursive* setting can be used to control whether or not the connector should look for files in subdirectories as well.
- **When writing**: the URL will be treated as a directory; if it doesn't exist, the loader will attempt to create it; CSV files will be created inside this directory, and their names can be controlled with the *fileNameFormat* setting.

Note that if the value specified here does not have a protocol, then it is assumed to be a file protocol.

In addition the value `-` indicates `stdin` when loading and `stdout` when unloading. This is in line with Unix tools such as tar, which uses `-` to represent stdin/stdout when reading/writing an archive.

Examples:

    url = "/path/to/dir/or/file"           # without protocol
    url = "file:///path/to/dir/or/file"    # with protocol
    url = "-"                              # to read csv data from stdin (for load) or
    url = "-"                              # write csv data to stdout (for unload)

For other URLs: the URL will be read or written directly; settings like *fileNamePattern*, *recursive*, and *fileNameFormat* will have no effect.

The default value is `-` (read from `stdin` / write to `stdout`).

Default: **"-"**.

#### -delim,--connector.csv.delimiter _&lt;string&gt;_

The character to use as field delimiter.

Only one character can be specified. Note that this setting applies to all files to be read or written.

Default: **","**.

#### -header,--connector.csv.header _&lt;boolean&gt;_

Whether the files to read or write begin with a header line or not.

When reading:

 - if set to true, the first non-empty line in every file will be used to assign field names to each record, thus allowing mappings by field name such as `{myFieldName1 = myColumnName1, myFieldName2 = myColumnName2}`.
 - if set to false, records will not contain field names, only (zero-based) field indexes; in this case, the mapping should be index-based, such as in `{0 = myColumnName1, 1 = myColumnName2}`.

When writing:

 - if set to true: each file will begin with a header line;
 - if set to false, files will not contain header lines.

Note that this setting applies to all files to be read or written.

Default: **true**.

#### -skipRecords,--connector.csv.skipRecords _&lt;number&gt;_

Defines a number of records to skip from each input file before the parser can begin to execute.

Note that if the file contains a header line, that line is not counted as a valid record.

Ignored when writing.

Default: **0**.

#### -maxRecords,--connector.csv.maxRecords _&lt;number&gt;_

Defines the maximum number of records to read from or write to each file.

When reading, all records past this number will be discarded.

When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting.

Note that when writing to anything other than a directory, this setting is ignored.

Note that if the file contains a header line, that line is not counted as a valid record.

This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### -url,--connector.json.url _&lt;string&gt;_

The URL or path of the resource(s) to read from or write to.

Which URL protocols are available depend on which URL stream handlers have been installed, but at least the **file** protocol is guaranteed to be supported.

The file protocol can be used with all supported file systems, local or not.
- **When reading**: the URL can point to a single file, or to an existing directory; in case of a directory, the *fileNamePattern* setting can be used to filter files to read, and the *recursive* setting can be used to control whether or not the connector should look for files in subdirectories as well.
- **When writing**: the URL will be treated as a directory; if it doesn't exist, the loader will attempt to create it; json files will be created inside this directory, and their names can be controlled with the *fileNameFormat* setting.

Note that if the value specified here does not have a protocol, then it is assumed to be a file protocol.

In addition the value `-` indicates `stdin` when loading and `stdout` when unloading. This is in line with Unix tools such as tar, which uses `-` to represent stdin/stdout when reading/writing an archive.

Examples:

    url = "/path/to/dir/or/file"           # without protocol
    url = "file:///path/to/dir/or/file"    # with protocol
    url = "-"                              # to read json data from stdin (for load) or
    url = "-"                              # write json data to stdout (for unload)

For other URLs: the URL will be read or written directly; settings like *fileNamePattern*, *recursive*, and *fileNameFormat* will have no effect.

The default value is `-` (read from `stdin` / write to `stdout`).

Default: **"-"**.

#### -skipRecords,--connector.json.skipRecords _&lt;number&gt;_

Defines a number of Json records to skip from each input file before the parser can begin to execute.

Ignored when writing.

Default: **0**.

#### -maxRecords,--connector.json.maxRecords _&lt;number&gt;_

Defines the maximum number of Json records to read from or write to each file.

When reading, all records past this number will be discarded.

When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting.

Note that when writing to anything other than a directory, this setting is ignored.

This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### -k,--schema.keyspace _&lt;string&gt;_

The keyspace to connect to. Optional.

If not specified, then the *schema.query* setting must be specified.

Default: **&lt;unspecified&gt;**.

#### -t,--schema.table _&lt;string&gt;_

The destination table. Optional.

If not specified, then the *schema.query* setting must be specified.

Default: **&lt;unspecified&gt;**.

#### -m,--schema.mapping _&lt;string&gt;_

The field-to-column mapping to use.

Applies to both load and unload workflows.

If not specified, the loader will apply a strict one-to-one mapping between the source fields and the database table. If that is not what you want, then you must supply an explicit mapping.

Mappings should be specified as a map of the following form:

- Indexed data sources: `0 = col1, 1 = col2, 2 = col3`, where `0`, `1`, `2`, etc. are the zero-based indices of fields in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map the first `n` fields is to simply specify the destination columns: `col1, col2, col3`.
- Mapped data sources: `fieldA = col1, fieldB = col2, fieldC = col3`, where `fieldA`, `fieldB`, `fieldC`, etc. are field names in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.

To specify that a field should be used for the query timestamp or ttl, use the specially named fake columns `__ttl` and `__timestamp`: `fieldA = __ttl`. Note that ttl fields are always parsed as integers representing the time-to-live in seconds; timestamp formats can be parsed with the following formats:

* An integer indicating number of microseconds since epoch.
* A valid date-time format; the string will be parsed using the following settings: `codec.timestamp` and `codec.timeZone`; see the `codec` section for more information on accepted date-time formats.

To specify that a column should be populated with the result of a function call, specify the function call as the input field (e.g. `now() = c4`). Note, this is only relevant for load operations.

In addition, for mapped data sources, it is also possible to specify that the mapping be partly auto-generated and partly explicitly specified. For example, if a source row has fields `c1`, `c2`, `c3`, and `c5`, and the table has columns `c1`, `c2`, `c3`, `c4`, one can map all like-named columns and specify that `c5` in the source maps to `c4` in the table as follows: `* = *, c5 = c4`

One can specify that all like-named fields be mapped, except for `c2`: `* = -c2`

To skip `c2` and `c3`: `* = [-c2, -c3]`

The exact type of mapping to use depends on the connector being used. Some connectors can only produce indexed records; others can only produce mapped ones, while others are capable of producing both indexed and mapped records at the same time. Refer to the connector's documentation to know which kinds of mapping it supports.

Default: **&lt;unspecified&gt;**.

#### -dryRun,--engine.dryRun _&lt;boolean&gt;_

Whether or not to run in dry-run mode.

Only applicable for loads, the tool proceeds as normal except it does not actually load any data.

Default: **false**.

#### -h,--driver.hosts _&lt;string&gt;_

The contact points to use for the initial connection to the cluster.

This must be a comma-separated list of hosts, each specified by a host-name or ip address. If the host is a DNS name that resolves to multiple A-records, all the corresponding addresses will be used. Do not use `localhost` as a host-name (since it resolves to both IPv4 and IPv6 addresses on some platforms).

Note that each host entry may optionally be followed by `:port` to specify the port to connect to. When not specified, this value falls back to the *port* setting.

Default: **"127.0.0.1"**.

#### -port,--driver.port _&lt;number&gt;_

The port to connect to at initial contact points.

Note that all nodes in a cluster must accept connections on the same port number.

Default: **9042**.

#### -u,--driver.auth.username _&lt;string&gt;_

The username to use. Required.

Providers that accept this setting:
 - `PlainTextAuthProvider`
 - `DsePlainTextAuthProvider`

Default: **"cassandra"**.

#### -p,--driver.auth.password _&lt;string&gt;_

The password to use. Required.

Providers that accept this setting:
 - `PlainTextAuthProvider`
 - `DsePlainTextAuthProvider`

Default: **"cassandra"**.

#### -cl,--driver.query.consistency _&lt;string&gt;_

The consistency level to use for both loads and unloads.

Valid values are: `ANY`, `LOCAL_ONE`, `ONE`, `TWO`, `THREE`, `LOCAL_QUORUM`, `QUORUM`, `EACH_QUORUM`, `ALL`.

Default: **"LOCAL_ONE"**.

#### --executor.maxPerSecond _&lt;number&gt;_

The maximum number of concurrent operations per second. This acts as a safeguard against workflows that could overwhelm the cluster with more requests than it can handle. Batch statements count for as many operations as their number of inner statements.

You should reduce this value when your latencies get too high. This is usually a sign that the remote cluster cannot keep up with the throughput, and requests will eventually time out.

Setting this option to any negative value will disable it.

Default: **-1**.

#### -maxErrors,--log.maxErrors _&lt;number&gt;_

The maximum number of errors or error % to tolerate before aborting the entire operation.

Setting this value to `-1` disables this feature (not recommended).

This can be set to a fixed number like 100, or a percentage like 2%.


Default: **100**.

#### -logDir,--log.directory _&lt;string&gt;_

The directory where all log files will be stored.

Note that this must be a path pointing to a writable directory. URLs are not accepted here (not even `file:/` URLs).

Log files for a specific run, or execution, will be located in a sub-directory inside the directory specified here. Each execution generates a sub-directory identified by an "execution ID". See `engine.executionId` for more information about execution IDs.

Setting this value to `.` denotes the current working directory.

Default: **"./logs"**.

#### -reportRate,--monitoring.reportRate _&lt;string&gt;_

The report interval for the console reporter.

The console reporter will print useful metrics about the ongoing operation at this rate.

Durations lesser than one second will be rounded up to 1 second.

Default: **"5 seconds"**.

<a name="connector"></a>
## Connector Settings

Connector-specific settings. This section contains settings for the connector to use; it also contains sub-sections, one for each available connector.

#### -c,--connector.name _&lt;string&gt;_

The name of the connector to use.

It is used in two places:

1. The path to the group of settings for the connector are located under `connector.<name>`.
2. The connector class name must start with `name`, case-insensitively. It is permitted for `name` to be the fully-qualified class name of the connector. That simply implies that the settings root will be at that fully-qualified location.

Example: `csv` for class `CSVConnector`, with settings located under `connector.csv`.

Default: **"csv"**.

<a name="connector.csv"></a>
### Connector Csv Settings

CSV Connector configuration.

#### -url,--connector.csv.url _&lt;string&gt;_

The URL or path of the resource(s) to read from or write to.

Which URL protocols are available depend on which URL stream handlers have been installed, but at least the **file** protocol is guaranteed to be supported.

The file protocol can be used with all supported file systems, local or not.
- **When reading**: the URL can point to a single file, or to an existing directory; in case of a directory, the *fileNamePattern* setting can be used to filter files to read, and the *recursive* setting can be used to control whether or not the connector should look for files in subdirectories as well.
- **When writing**: the URL will be treated as a directory; if it doesn't exist, the loader will attempt to create it; CSV files will be created inside this directory, and their names can be controlled with the *fileNameFormat* setting.

Note that if the value specified here does not have a protocol, then it is assumed to be a file protocol.

In addition the value `-` indicates `stdin` when loading and `stdout` when unloading. This is in line with Unix tools such as tar, which uses `-` to represent stdin/stdout when reading/writing an archive.

Examples:

    url = "/path/to/dir/or/file"           # without protocol
    url = "file:///path/to/dir/or/file"    # with protocol
    url = "-"                              # to read csv data from stdin (for load) or
    url = "-"                              # write csv data to stdout (for unload)

For other URLs: the URL will be read or written directly; settings like *fileNamePattern*, *recursive*, and *fileNameFormat* will have no effect.

The default value is `-` (read from `stdin` / write to `stdout`).

Default: **"-"**.

#### -delim,--connector.csv.delimiter _&lt;string&gt;_

The character to use as field delimiter.

Only one character can be specified. Note that this setting applies to all files to be read or written.

Default: **","**.

#### -header,--connector.csv.header _&lt;boolean&gt;_

Whether the files to read or write begin with a header line or not.

When reading:

 - if set to true, the first non-empty line in every file will be used to assign field names to each record, thus allowing mappings by field name such as `{myFieldName1 = myColumnName1, myFieldName2 = myColumnName2}`.
 - if set to false, records will not contain field names, only (zero-based) field indexes; in this case, the mapping should be index-based, such as in `{0 = myColumnName1, 1 = myColumnName2}`.

When writing:

 - if set to true: each file will begin with a header line;
 - if set to false, files will not contain header lines.

Note that this setting applies to all files to be read or written.

Default: **true**.

#### -skipRecords,--connector.csv.skipRecords _&lt;number&gt;_

Defines a number of records to skip from each input file before the parser can begin to execute.

Note that if the file contains a header line, that line is not counted as a valid record.

Ignored when writing.

Default: **0**.

#### -maxRecords,--connector.csv.maxRecords _&lt;number&gt;_

Defines the maximum number of records to read from or write to each file.

When reading, all records past this number will be discarded.

When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting.

Note that when writing to anything other than a directory, this setting is ignored.

Note that if the file contains a header line, that line is not counted as a valid record.

This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### -quote,--connector.csv.quote _&lt;string&gt;_

The character used for quoting fields when the field delimiter is part of the field value.

Only one character can be specified. Note that this setting applies to all files to be read or written.

Default: **"\""**.

#### -comment,--connector.csv.comment _&lt;string&gt;_

The character that represents a line comment when found in the beginning of a line of text.

Only one character can be specified. Note that this setting applies to all files to be read or written.

This feature is disabled by default (indicated by its `null` character value).

Default: **"\u0000"**.

#### -encoding,--connector.csv.encoding _&lt;string&gt;_

The file encoding to use.

Note that this setting applies to all files to be read or written.

Default: **"UTF-8"**.

#### -escape,--connector.csv.escape _&lt;string&gt;_

The character used for escaping quotes inside an already quoted value.

Only one character can be specified. Note that this setting applies to all files to be read or written.

Default: **"\\"**.

#### --connector.csv.fileNameFormat _&lt;string&gt;_

The file name format to use when writing.

Ignored when reading. Ignored for non-file URLs.

The file name must comply with the formatting rules of `String.format()`, and must contain a `%d` format specifier that will be used to increment file name counters.

Default: **"output-%0,6d.csv"**.

#### --connector.csv.fileNamePattern _&lt;string&gt;_

The glob pattern to use when searching for files to read. The syntax to use is the glob syntax, as described in `java.nio.file.FileSystem.getPathMatcher()`.

Ignored when writing. Ignored for non-file URLs.

Only applicable when the *url* setting points to a directory on a known filesystem, ignored otherwise.

Default: **"\*\*/\*.csv"**.

#### --connector.csv.maxCharsPerColumn _&lt;number&gt;_

The maximum number of characters that a field can contain.

This setting is used to size internal buffers and to avoid out-of-memory problems.

If set to -1, internal buffers will be resized dynamically. While convenient, this might lead to memory problems. It could also hurt throughput, if some large fields require constant resizings; if this is the case, it is preferable to set this value to a fixed positive number that is big enough to contain all field values.

Default: **4096**.

#### -maxConcurrentFiles,--connector.csv.maxConcurrentFiles _&lt;string&gt;_

The maximum number of files that can be written simultaneously.

Ignored when reading. Ignored when the output URL is anything other than a directory on a filesystem.

The special syntax `NC` can be used to specify a number of threads that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 threads.

Default: **"0.25C"**.

#### --connector.csv.recursive _&lt;boolean&gt;_

Whether to scan for files in subdirectories of the root directory.

Only applicable when the *url* setting points to a directory on a known filesystem, ignored otherwise. Ignored when writing.

Default: **false**.

<a name="connector.json"></a>
### Connector Json Settings

Json Connector configuration.

#### -url,--connector.json.url _&lt;string&gt;_

The URL or path of the resource(s) to read from or write to.

Which URL protocols are available depend on which URL stream handlers have been installed, but at least the **file** protocol is guaranteed to be supported.

The file protocol can be used with all supported file systems, local or not.
- **When reading**: the URL can point to a single file, or to an existing directory; in case of a directory, the *fileNamePattern* setting can be used to filter files to read, and the *recursive* setting can be used to control whether or not the connector should look for files in subdirectories as well.
- **When writing**: the URL will be treated as a directory; if it doesn't exist, the loader will attempt to create it; json files will be created inside this directory, and their names can be controlled with the *fileNameFormat* setting.

Note that if the value specified here does not have a protocol, then it is assumed to be a file protocol.

In addition the value `-` indicates `stdin` when loading and `stdout` when unloading. This is in line with Unix tools such as tar, which uses `-` to represent stdin/stdout when reading/writing an archive.

Examples:

    url = "/path/to/dir/or/file"           # without protocol
    url = "file:///path/to/dir/or/file"    # with protocol
    url = "-"                              # to read json data from stdin (for load) or
    url = "-"                              # write json data to stdout (for unload)

For other URLs: the URL will be read or written directly; settings like *fileNamePattern*, *recursive*, and *fileNameFormat* will have no effect.

The default value is `-` (read from `stdin` / write to `stdout`).

Default: **"-"**.

#### -skipRecords,--connector.json.skipRecords _&lt;number&gt;_

Defines a number of Json records to skip from each input file before the parser can begin to execute.

Ignored when writing.

Default: **0**.

#### -maxRecords,--connector.json.maxRecords _&lt;number&gt;_

Defines the maximum number of Json records to read from or write to each file.

When reading, all records past this number will be discarded.

When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting.

Note that when writing to anything other than a directory, this setting is ignored.

This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### --connector.json.mode _&lt;string&gt;_

The Json document mode.

Valid values are:

* *MULTI_DOCUMENT*: each resource may contain an arbitrary number of successive Json documents to be mapped to records, e.g.:
```json
{/*doc1*/}
{/*doc2*/}
{/*doc3*/}
...
```
* *SINGLE_DOCUMENT*: each resource contains a root array whose elements are Json documents to be mapped to records, e.g.:
```json
[
{/*doc1*/}
{/*doc2*/}
{/*doc3*/}
...
]
```

Default: **"MULTI_DOCUMENT"**.

#### -encoding,--connector.json.encoding _&lt;string&gt;_

The file encoding to use.

Note that this setting applies to all files to be read or written.

Default: **"UTF-8"**.

#### --connector.json.fileNameFormat _&lt;string&gt;_

The file name format to use when writing.

Ignored when reading. Ignored for non-file URLs.

The file name must comply with the formatting rules of `String.format()`, and must contain a `%d` format specifier that will be used to increment file name counters.

Default: **"output-%0,6d.json"**.

#### --connector.json.fileNamePattern _&lt;string&gt;_

The glob pattern to use when searching for files to read. The syntax to use is the glob syntax, as described in `java.nio.file.FileSystem.getPathMatcher()`.

Ignored when writing. Ignored for non-file URLs.

Only applicable when the *url* setting points to a directory on a known filesystem, ignored otherwise.

Default: **"\*\*/\*.json"**.

#### --connector.json.generatorFeatures _&lt;list&gt;_

Json generator features to enable.

Ignored when reading.

Valid values are all the enum constants defined in `com.fasterxml.jackson.core.JsonGenerator.Feature`.

Default: **[]**.

#### -maxConcurrentFiles,--connector.json.maxConcurrentFiles _&lt;string&gt;_

The maximum number of files that can be written simultaneously.

Ignored when reading. Ignored when the output URL is anything other than a directory on a filesystem.

The special syntax `NC` can be used to specify a number of threads that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 threads.

Default: **"0.25C"**.

#### --connector.json.parserFeatures _&lt;list&gt;_

Json parser features to enable.

Ignored when writing.

Valid values are all the enum constants defined in `com.fasterxml.jackson.core.JsonParser.Feature`.

Default: **[]**.

#### --connector.json.prettyPrint _&lt;boolean&gt;_

Whether to enable pretty print.

Ignored when reading.

When pretty print is enabled, Json records are written indented. Beware that this may result in much bigger records.

Default: **false**.

#### --connector.json.recursive _&lt;boolean&gt;_

Whether to scan for files in subdirectories of the root directory.

Only applicable when the *url* setting points to a directory on a known filesystem, ignored otherwise. Ignored when writing.

Default: **false**.

<a name="schema"></a>
## Schema Settings

Schema-specific settings.

#### -k,--schema.keyspace _&lt;string&gt;_

The keyspace to connect to. Optional.

If not specified, then the *schema.query* setting must be specified.

Default: **&lt;unspecified&gt;**.

#### -t,--schema.table _&lt;string&gt;_

The destination table. Optional.

If not specified, then the *schema.query* setting must be specified.

Default: **&lt;unspecified&gt;**.

#### -m,--schema.mapping _&lt;string&gt;_

The field-to-column mapping to use.

Applies to both load and unload workflows.

If not specified, the loader will apply a strict one-to-one mapping between the source fields and the database table. If that is not what you want, then you must supply an explicit mapping.

Mappings should be specified as a map of the following form:

- Indexed data sources: `0 = col1, 1 = col2, 2 = col3`, where `0`, `1`, `2`, etc. are the zero-based indices of fields in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map the first `n` fields is to simply specify the destination columns: `col1, col2, col3`.
- Mapped data sources: `fieldA = col1, fieldB = col2, fieldC = col3`, where `fieldA`, `fieldB`, `fieldC`, etc. are field names in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.

To specify that a field should be used for the query timestamp or ttl, use the specially named fake columns `__ttl` and `__timestamp`: `fieldA = __ttl`. Note that ttl fields are always parsed as integers representing the time-to-live in seconds; timestamp formats can be parsed with the following formats:

* An integer indicating number of microseconds since epoch.
* A valid date-time format; the string will be parsed using the following settings: `codec.timestamp` and `codec.timeZone`; see the `codec` section for more information on accepted date-time formats.

To specify that a column should be populated with the result of a function call, specify the function call as the input field (e.g. `now() = c4`). Note, this is only relevant for load operations.

In addition, for mapped data sources, it is also possible to specify that the mapping be partly auto-generated and partly explicitly specified. For example, if a source row has fields `c1`, `c2`, `c3`, and `c5`, and the table has columns `c1`, `c2`, `c3`, `c4`, one can map all like-named columns and specify that `c5` in the source maps to `c4` in the table as follows: `* = *, c5 = c4`

One can specify that all like-named fields be mapped, except for `c2`: `* = -c2`

To skip `c2` and `c3`: `* = [-c2, -c3]`

The exact type of mapping to use depends on the connector being used. Some connectors can only produce indexed records; others can only produce mapped ones, while others are capable of producing both indexed and mapped records at the same time. Refer to the connector's documentation to know which kinds of mapping it supports.

Default: **&lt;unspecified&gt;**.

#### -nullStrings,--schema.nullStrings _&lt;string&gt;_

Case-sensitive strings (in the form of a comma-delimited list) that should be mapped to `null`.

In load workflows, when a record field value matches one of these words, then that value is replaced with a `null` and forwarded to DSE as such.

This setting only applies for fields of type String.

Note that this setting is applied before the *schema.nullToUnset* setting, hence any `null`s produced by a null-string can still be left unset if required.

In unload workflows, only the first string specified here will be used: when a row cell contains a `null` value, then it will be replaced with that word and forwarded as such to the connector.

By default, empty strings are converted to `null`s in load workflows, and conversely `null`s are converted to empty strings in unload workflows.

Default: **&lt;unspecified&gt;**.

#### --schema.nullToUnset _&lt;boolean&gt;_

Whether or not to map `null` input values to "unset" in the database, meaning don't modify a potentially pre-existing value of this field for this row.

This is only valid for load scenarios; it is ignored otherwise.

Note that this setting is applied after the *schema.nullStrings* setting, and may intercept `null`s produced by that setting.

Note that setting this to false leads to tombstones being created in the database to represent `null`.

Default: **true**.

#### -query,--schema.query _&lt;string&gt;_

The query to use. Optional.

It is not required to qualify the table with a keyspace name. If the keyspace is _not_ provided however, then `schema.keyspace` _must_ be provided.

If this setting is not specified, then *schema.keyspace* and *schema.table* must be specified, and dsbulk will infer the appropriate statement based on the table's metadata, using all available columns.

In load worflows, the statement can be any `INSERT` or `UPDATE` statement, but **must** use named bound variables exclusively; positional bound variables will not work.

Bound variable names usually match those of the columns in the destination table, but this is not a strict requirement; it is, however, required that their names match those specified in the mapping.

See *schema.mapping* setting for more information.

In unload worflows, the statement can be any regular `SELECT` statement; it can optionally contain a token range restriction clause of the form: `token(...) > :start and token(...) <= :end.`

If such a clause is present, the engine will generate as many statements as there are token ranges in the cluster, thus allowing parallelization of reads while at the same time targeting coordinators that are also replicas.

The column names in the SELECT clause will be used to match column names specified in the mapping. See "mapping" setting for more information.

Important: the query will be parsed because DSBulk needs to know which bound variables are present in order to correctly map them to fields. The parser used internally is based on CQL 3.4.5 (as used in Apache Cassandra 3.11.1) and is capable of parsing all statements defined by that specific grammar version, even complex ones. However, future versions of CQL might introduce new grammar rules that DSBulk's parser will not be able to parse; DSBulk will emit a warning in such situations.

Default: **&lt;unspecified&gt;**.

#### --schema.queryTimestamp _&lt;string&gt;_

Timestamp of inserted/updated cells during load.

Only applicable for load; ignored for unload.

The following formats are supported:

* Alphanumeric: a valid date-time format; the value will be parsed using the following settings: `codec.timestamp` and `codec.timeZone`.
* Numeric: a numeric timestamp; the value will be parsed using the following settings: `codec.unit` and `codec.epoch`.

See the `codec` section for more information on accepted date-time formats.

If not specified, inserts/updates use current time of the system running the tool.

Query timestamps for Cassandra and DSE have microsecond resolution; any sub-microsecond information specified here will be lost.
For more information, see the [CQL INSERT INTO Reference](https://docs.datastax.com/en/dse/5.1/cql/cql/cql_reference/cql_commands/cqlInsert.html#cqlInsert__timestamp-value).

Default: **&lt;unspecified&gt;**.

#### --schema.queryTtl _&lt;number&gt;_

Time-to-live of inserted/updated cells during load (seconds).

Only applicable for load; ignored for unload.

A value of -1 means there is no ttl.

For more information, see the [CQL INSERT INTO Reference](https://docs.datastax.com/en/dse/5.1/cql/cql/cql_reference/cql_commands/cqlInsert.html#cqlInsert__ime-value), [Setting the time-to-live (TTL) for value](http://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/useTTL.html?hl=ttl) and [Expiring data with time-to-live](http://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/useExpire.html).

Default: **-1**.

<a name="batch"></a>
## Batch Settings

Batch-specific settings.

These settings control how the workflow engine groups together statements before writing them.

Only applicable for load workflows, ignored otherwise.

See `com.datastax.dsbulk.executor.api.batch.StatementBatcher` for more information.

#### --batch.bufferSize _&lt;number&gt;_

The buffer size to use for batching statements.

The buffer will be flushed when this size is reached.

When set to a negative value the buffer size is implicitly set to `maxBatchSize`.

It is usually not necessary to set this value higher than `maxBatchSize`, unless the dataset to load is unsorted, in which case a higher value might improve the average batch size.

Default: **-1**.

#### --batch.enabled _&lt;boolean&gt;_

Whether to enable batching of statements or not.

Default: **true**.

#### --batch.maxBatchSize _&lt;number&gt;_

The maximum batch size.

The ideal batch size depends on how large is the data to be inserted: the larger the data, the smaller this value should be.

The ideal batch size also depends on the batch mode in use. When using **PARTITION_KEY**, it is usually better to use larger batch sizes. When using **REPLICA_SET** however, batches sizes should remain small (below 10).

Default: **32**.

#### --batch.mode _&lt;string&gt;_

The grouping mode. Valid values are:
- **PARTITION_KEY**: Groups together statements that share the same partition key. This is the default mode, and the preferred one.
- **REPLICA_SET**: Groups together statements that share the same replica set. This mode might yield better results for small clusters and lower replication factors, but tends to perform equally well or worse than `PARTITION_KEY` for larger clusters or high replication factors.

Default: **"PARTITION_KEY"**.

<a name="codec"></a>
## Codec Settings

Conversion-specific settings. These settings apply for both load and unload workflows.

When writing, these settings determine how record fields emitted by connectors are parsed.

When unloading, these settings determine how row cells emitted by DSE are formatted.

#### --codec.booleanNumbers _&lt;list&lt;number&gt;&gt;_

How numbers should be mapped to booleans and vice versa.

This list should contain exactly two elements; the first element will be mapped to `true`, the second one to `false`. Note that when mapping from numbers to booleans, all other numbers will be rejected.

Default: **[1,0]**.

#### --codec.booleanWords _&lt;list&lt;string&gt;&gt;_

All representations of true and false supported by dsbulk. Each representation is of the form `true-value:false-value`, case-insensitive.

In load workflows, all representations are taken into account.

In unload workflows, the first true-false pair will be used to format booleans; all other pairs will be ignored.

Default: **["1:0","Y:N","T:F","YES:NO","TRUE:FALSE"]**.

#### --codec.date _&lt;string&gt;_

The temporal pattern to use for `String` to CQL date conversions. This can be either:

- A date-time pattern
- A pre-defined formatter such as `ISO_LOCAL_DATE`

For more information on patterns and pre-defined formatters, see [Patterns for Formatting and Parsing](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns).

Default: **"ISO_LOCAL_DATE"**.

#### --codec.epoch _&lt;string&gt;_

The "Epoch" for all numeric temporal fields, that is, the instant that corresponds to zero.

This setting only applies for CQL timestamp colums, and for `USING TIMESTAMP` clauses; it is used when the input has numerical shape (for example, a string containing only digits) and cannot be parsed using the `codec.timestamp` format. In these situations, this setting determines which point in time the parsed value is relative to.

It defaults to the Unix Epoch (January 1st 1970 GMT).

The value provided here must be a valid timestamp itself, as defined by the `codec.timestamp` format (and the time zone defined by `codec.timeZone`, if the format itself does not include any time zone information).

Default: **"1970-01-01T00:00:00Z"**.

#### -locale,--codec.locale _&lt;string&gt;_

The locale to use for locale-sensitive conversions.

Default: **"en_US"**.

#### --codec.number _&lt;string&gt;_

The `DecimalFormat` pattern to use for `String` to `Number` conversions. See `java.text.DecimalFormat` for details about the pattern syntax to use.

Default: **"#,###.##"**.

#### --codec.time _&lt;string&gt;_

The temporal pattern to use for `String` to CQL time conversions. This can be either:

- A date-time pattern
- A pre-defined formatter such as `ISO_LOCAL_TIME`

For more information on patterns and pre-defined formatters, see [Patterns for Formatting and Parsing](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns).

Default: **"ISO_LOCAL_TIME"**.

#### -timeZone,--codec.timeZone _&lt;string&gt;_

The time zone to use for temporal conversions that do not convey any explicit time zone information.

Default: **"UTC"**.

#### --codec.timestamp _&lt;string&gt;_

The temporal pattern to use for `String` to CQL timestamp conversions. This can be either:

- A date-time pattern
- A pre-defined formatter such as `ISO_DATE_TIME`
- The special value `CQL_DATE_TIME`, which is a special parser that accepts all valid CQL literal formats for the `timestamp` type

For more information on patterns and pre-defined formatters, see [Patterns for Formatting and Parsing](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns).

Default: **"CQL_DATE_TIME"**.

#### --codec.unit _&lt;string&gt;_

The time unit to use when converting numeric fields into CQL timestamps.

This setting only applies for CQL timestamp colums, and for `USING TIMESTAMP` clauses; it is used when the input has numerical shape (for example, a string containing only digits) and cannot be parsed using the `codec.timestamp` format. In these situations, this setting determines the time unit to apply to the parsed value.

Valid values: all `TimeUnit` enum constants.

Default: **"MILLISECONDS"**.

#### --codec.uuidStrategy _&lt;string&gt;_

The strategy to use when generating time-based (version 1) UUIDs from timestamps.

Valid values are:

* `RANDOM`: Default value. Generates UUIDs using a random number in lieu of the local clock sequence and node ID. If you can guarantee that the original timestamps are unique, then you should prefer the `FIXED` strategy, which is faster. If you can't guarantee this however, this strategy will ensure that the generated UUIDs are unique.
* `FIXED`: Generates UUIDs using a fixed local clock sequence and node ID. Note that this strategy is generally only safe to use if you can guarantee that the original timestamps are unique.
* `MIN`: Generates the smallest possible type 1 UUID for a given timestamp. Warning: this strategy doesn't guarantee the uniqueness of the generated UUIDs and should be used with caution.
* `MAX`: Generates the biggest possible type 1 UUID for a given timestamp. Warning: this strategy doesn't guarantee the uniqueness of the generated UUIDs and should be used with caution.

Also please note that, for all strategies outlined above, the clock sequence and the node ID parts of the generated UUIDs are determined on a best-effort basis and are not fully compliant with RFC 4122.

Default: **"RANDOM"**.

<a name="driver"></a>
## Driver Settings

Driver-specific configuration.

#### -h,--driver.hosts _&lt;string&gt;_

The contact points to use for the initial connection to the cluster.

This must be a comma-separated list of hosts, each specified by a host-name or ip address. If the host is a DNS name that resolves to multiple A-records, all the corresponding addresses will be used. Do not use `localhost` as a host-name (since it resolves to both IPv4 and IPv6 addresses on some platforms).

Note that each host entry may optionally be followed by `:port` to specify the port to connect to. When not specified, this value falls back to the *port* setting.

Default: **"127.0.0.1"**.

#### -port,--driver.port _&lt;number&gt;_

The port to connect to at initial contact points.

Note that all nodes in a cluster must accept connections on the same port number.

Default: **9042**.

#### --driver.addressTranslator _&lt;string&gt;_

The simple or fully-qualified class name of the address translator to use.

This is only needed if the nodes are not directly reachable from the driver (for example, the driver is in a different network region and needs to use a public IP, or it connects through a proxy).

Default: **"IdentityTranslator"**.

#### --driver.timestampGenerator _&lt;string&gt;_

The simple or fully-qualified class name of the timestamp generator to use. Built-in options are:

- **AtomicTimestampGenerator**: timestamps are guaranteed to be unique across all client threads.
- **ThreadLocalTimestampGenerator**: timestamps are guaranteed to be unique within each thread only.
- **ServerSideTimestampGenerator**: do not generate timestamps, let the server assign them.

Default: **"AtomicMonotonicTimestampGenerator"**.

<a name="driver.auth"></a>
### Driver Auth Settings

Authentication settings.

#### -u,--driver.auth.username _&lt;string&gt;_

The username to use. Required.

Providers that accept this setting:
 - `PlainTextAuthProvider`
 - `DsePlainTextAuthProvider`

Default: **"cassandra"**.

#### -p,--driver.auth.password _&lt;string&gt;_

The password to use. Required.

Providers that accept this setting:
 - `PlainTextAuthProvider`
 - `DsePlainTextAuthProvider`

Default: **"cassandra"**.

#### --driver.auth.provider _&lt;string&gt;_

The name of the AuthProvider to use.
 - **None**: no authentication.
 - **PlainTextAuthProvider**:
   Uses `com.datastax.driver.core.PlainTextAuthProvider` for authentication. Supports SASL authentication using the `PLAIN` mechanism (plain text authentication).
 - **DsePlainTextAuthProvider**:
   Uses `com.datastax.driver.dse.auth.DsePlainTextAuthProvider` for authentication. Supports SASL authentication to DSE clusters using the `PLAIN` mechanism (plain text authentication), and also supports optional proxy authentication; should be preferred to `PlainTextAuthProvider` when connecting to secured DSE clusters.
 - **DseGSSAPIAuthProvider**:
   Uses `com.datastax.driver.dse.auth.DseGSSAPIAuthProvider` for authentication. Supports SASL authentication to DSE clusters using the `GSSAPI` mechanism (Kerberos authentication), and also supports optional proxy authentication.

   NOTE: When using this provider you may have to set the `java.security.krb5.conf` system property to point to your `krb5.conf` file (e.g. set the `DSBULK_JAVA_OPTS` environment variable to `-Djava.security.krb5.conf=/home/user/krb5.conf`).
   See the [Oracle Java Kerberos documentation](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html) for more details.

Default: **"None"**.

#### --driver.auth.authorizationId _&lt;string&gt;_

The authorization ID to use. Optional.

An authorization ID allows the currently authenticated user to act as a different user (a.k.a. proxy authentication).

Providers that accept this setting:
 - `DsePlainTextAuthProvider`
 - `DseGSSAPIAuthProvider`

Default: **&lt;unspecified&gt;**.

#### --driver.auth.keyTab _&lt;string&gt;_

The path of the Kerberos keytab file to use for authentication. Optional.

If left unspecified, it is assumed that authentication will be done with a ticket cache instead.

Providers that accept this setting:
 - `DseGSSAPIAuthProvider`

Default: **&lt;unspecified&gt;**.

#### --driver.auth.principal _&lt;string&gt;_

The Kerberos principal to use. Required.

Providers that accept this setting:
 - `DseGSSAPIAuthProvider`

This is typically of the form `user@DOMAIN.COM`.

Default: **&lt;unspecified&gt;**.

#### --driver.auth.saslProtocol _&lt;string&gt;_

The SASL protocol name to use. Required.

This value should match the username of the Kerberos service principal used by the DSE server. This information is specified in the `dse.yaml` file by the *service_principal* option under the *kerberos_options* section, and may vary from one DSE installation to another – especially if you installed DSE with an automated package installer.

Providers that accept this setting:
 - `DseGSSAPIAuthProvider`

Default: **"dse"**.

<a name="driver.policy"></a>
### Driver Policy Settings

Settings for various driver policies.

#### -lbp,--driver.policy.lbp.name _&lt;string&gt;_

The name of the load balancing policy.

Supported policies include: `dse`, `dcAwareRoundRobin`, `roundRobin`, `whiteList`, `tokenAware`. Available options for the policies are listed below as appropriate. For more information, refer to the driver documentation for the policy.

If not specified, defaults to the driver's default load balancing policy, which is currently the `DseLoadBalancingPolicy` wrapping a `TokenAwarePolicy`, wrapping a `DcAwareRoundRobinPolicy`.

NOTE: It is critical for a token-aware policy to be used in the chain in order to benefit from batching by partition key.

Default: **&lt;unspecified&gt;**.

#### --driver.policy.lbp.dcAwareRoundRobin.allowRemoteDCsForLocalConsistencyLevel _&lt;boolean&gt;_

Allows the policy to return remote hosts when building query plans for queries having consistency level `LOCAL_ONE` or `LOCAL_QUORUM`.

Default: **false**.

#### --driver.policy.lbp.dcAwareRoundRobin.localDc _&lt;string&gt;_

The name of the DC that is "local" to this client. Requests are sent to nodes in the local data center whenever possible, since the network distance is minimal.

Default: **&lt;unspecified&gt;**.

#### --driver.policy.lbp.dcAwareRoundRobin.usedHostsPerRemoteDc _&lt;number&gt;_

Sets the number of hosts per remote datacenter that the policy should consider.

Default: **0**.

#### --driver.policy.lbp.dse.childPolicy _&lt;string&gt;_

The child policy being wrapped.

It is required to be one of the policies mentioned above.

Default: **"roundRobin"**.

#### --driver.policy.lbp.tokenAware.childPolicy _&lt;string&gt;_

The child policy being wrapped.

It is required to be one of the policies mentioned above.

Default: **"roundRobin"**.

#### --driver.policy.lbp.tokenAware.shuffleReplicas _&lt;boolean&gt;_

Whether or not to shuffle the list of replicas that can process a request. For loads, this has the benefit of distributing writes across nodes.

Default: **true**.

#### --driver.policy.lbp.whiteList.childPolicy _&lt;string&gt;_

The child policy being wrapped.

It is required to be one of the policies mentioned above.

Default: **"roundRobin"**.

#### --driver.policy.lbp.whiteList.hosts _&lt;string&gt;_

List of hosts to white list.

This must be a comma-separated list of hosts, each specified by a host-name or ip address. If the host is a DNS name that resolves to multiple A-records, all the corresponding addresses will be used. Do not use `localhost` as a host-name (since it resolves to both IPv4 and IPv6 addresses on some platforms).

Default: **&lt;unspecified&gt;**.

#### -maxRetries,--driver.policy.maxRetries _&lt;number&gt;_

Maximum number of retries for a timed-out request.

Default: **10**.

<a name="driver.pooling"></a>
### Driver Pooling Settings

Pooling-specific settings.

The driver maintains a connection pool to each node, according to the distance assigned to it by the load balancing policy. If the distance is `IGNORED`, no connections are maintained.

#### --driver.pooling.heartbeat _&lt;string&gt;_

The heartbeat interval. If a connection stays idle for that duration (no reads), the driver sends a dummy message on it to make sure it's still alive. If not, the connection is trashed and replaced.

Default: **"30 seconds"**.

#### --driver.pooling.local.connections _&lt;number&gt;_

The number of connections in the pool for nodes at "local" distance.

Default: **4**.

#### --driver.pooling.local.requests _&lt;number&gt;_

The maximum number of requests that can be executed concurrently on a connection.

This must be between 1 and 32768.

Default: **32768**.

#### --driver.pooling.remote.connections _&lt;number&gt;_

The number of connections in the pool for nodes at "remote" distance.

Default: **1**.

#### --driver.pooling.remote.requests _&lt;number&gt;_

The maximum number of requests that can be executed concurrently on a connection.

This must be between 1 and 32768.

Default: **1024**.

<a name="driver.protocol"></a>
### Driver Protocol Settings

Native Protocol-specific settings.

#### --driver.protocol.compression _&lt;string&gt;_

The compression algorithm to use.
Valid values are: `NONE`, `LZ4`, `SNAPPY`.

Default: **"NONE"**.

<a name="driver.query"></a>
### Driver Query Settings

Query-related settings.

#### -cl,--driver.query.consistency _&lt;string&gt;_

The consistency level to use for both loads and unloads.

Valid values are: `ANY`, `LOCAL_ONE`, `ONE`, `TWO`, `THREE`, `LOCAL_QUORUM`, `QUORUM`, `EACH_QUORUM`, `ALL`.

Default: **"LOCAL_ONE"**.

#### --driver.query.fetchSize _&lt;number&gt;_

The page size. This controls how many rows will be retrieved simultaneously in a single network round trip (the goal being to avoid loading too many results in memory at the same time).

Only applicable in unload scenarios, ignored otherwise.

Default: **5000**.

#### --driver.query.idempotence _&lt;boolean&gt;_

The default idempotence of statements generated by the loader.

Default: **true**.

#### --driver.query.serialConsistency _&lt;string&gt;_

The serial consistency level to use for writes.

Valid values are: `SERIAL` and `LOCAL_SERIAL`.

Only applicable if the data is inserted using lightweight transactions, ignored otherwise.

Default: **"LOCAL_SERIAL"**.

<a name="driver.socket"></a>
### Driver Socket Settings

Socket-related settings.

#### --driver.socket.readTimeout _&lt;string&gt;_

How long the driver waits for a request to complete. This is a global limit on the duration of a `session.execute()` call, including any internal retries the driver might do.

Default: **"60 seconds"**.

<a name="driver.ssl"></a>
### Driver Ssl Settings

Encryption-specific settings.

For more information about how to configure this section, see the Java Secure Socket Extension (JSSE) Reference Guide: http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html. You can also check the DataStax Java driver documentation on SSL: http://docs.datastax.com/en/developer/java-driver-dse/latest/manual/ssl/

#### --driver.ssl.cipherSuites _&lt;list&gt;_

The cipher suites to enable.

Example: `cipherSuites = ["TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"]`

This property is optional. If it is not present, the driver won't explicitly enable cipher suites, which according to the JDK documentation results in "a minimum quality of service".

Default: **[]**.

#### --driver.ssl.keystore.algorithm _&lt;string&gt;_

The algorithm to use.

Valid values are: `SunX509`, `NewSunX509`.

Default: **"SunX509"**.

#### --driver.ssl.keystore.password _&lt;string&gt;_

The keystore password.

Default: **&lt;unspecified&gt;**.

#### --driver.ssl.keystore.path _&lt;string&gt;_

The path of the keystore file.

This setting is optional. If left unspecified, no client authentication will be used.

Default: **&lt;unspecified&gt;**.

#### --driver.ssl.openssl.keyCertChain _&lt;string&gt;_

The path of the certificate chain file.

This setting is optional. If left unspecified, no client authentication will be used.

Default: **&lt;unspecified&gt;**.

#### --driver.ssl.openssl.privateKey _&lt;string&gt;_

The path of the private key file.

Default: **&lt;unspecified&gt;**.

#### --driver.ssl.provider _&lt;string&gt;_

The SSL provider to use.

Valid values are:

- **None**: no SSL.
- **JDK**: uses JDK's SSLContext
- **OpenSSL**: uses Netty's native support for OpenSSL

Using OpenSSL provides better performance and generates less garbage. This is the recommended provider when using SSL.

Default: **"None"**.

#### --driver.ssl.truststore.algorithm _&lt;string&gt;_

The algorithm to use.

Valid values are: `PKIX`, `SunX509`.

Default: **"SunX509"**.

#### --driver.ssl.truststore.password _&lt;string&gt;_

The truststore password.

Default: **&lt;unspecified&gt;**.

#### --driver.ssl.truststore.path _&lt;string&gt;_

The path of the truststore file.

This setting is optional. If left unspecified, server certificates will not be validated.

Default: **&lt;unspecified&gt;**.

<a name="engine"></a>
## Engine Settings

Workflow Engine-specific settings.

#### -dryRun,--engine.dryRun _&lt;boolean&gt;_

Whether or not to run in dry-run mode.

Only applicable for loads, the tool proceeds as normal except it does not actually load any data.

Default: **false**.

#### --engine.executionId _&lt;string&gt;_

A unique identifier to attribute to each execution.

When unspecified or empty, the engine will automatically generate identifiers of the following form:
`<workflow>_<timestamp>`, where :

- `<workflow>` stands for the workflow type (`LOAD`, `UNLOAD`, etc.);
- `<timestamp>` is the current timestamp formatted as `uuuuMMdd-HHmmss-SSSSSS` (see [Patterns for Formatting and Parsing](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns)) in UTC, with microsecond precision if available, and millisecond precision otherwise.

When this identifier is user-supplied, it is important to guarantee its uniqueness; failing to do so may result in execution failures.

It is also possible to provide templates here. Any format compliant with the formatting rules of [`String.format()`](https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html#syntax) is accepted, and can contain the following parameters:

- `%1$s` : the workflow type (`LOAD`, `UNLOAD`, etc.);
- `%2$t` : the current time (with microsecond precision if available, and millisecond precision otherwise);
- `%3$s` : the JVM process PID (this parameter might not be available on some operating systems; if its value cannot be determined, a random integer will be inserted instead).

Default: **&lt;unspecified&gt;**.

<a name="executor"></a>
## Executor Settings

Executor-specific settings.

#### --executor.maxPerSecond _&lt;number&gt;_

The maximum number of concurrent operations per second. This acts as a safeguard against workflows that could overwhelm the cluster with more requests than it can handle. Batch statements count for as many operations as their number of inner statements.

You should reduce this value when your latencies get too high. This is usually a sign that the remote cluster cannot keep up with the throughput, and requests will eventually time out.

Setting this option to any negative value will disable it.

Default: **-1**.

#### --executor.continuousPaging.enabled _&lt;boolean&gt;_

Whether or not continuous paging is enabled.

If the target cluster does not support continuous paging, traditional paging will be used regardless
of this setting.

Default: **true**.

#### --executor.continuousPaging.maxPages _&lt;number&gt;_

The maximum number of pages to retrieve.

Setting this value to zero retrieves all pages available.

Default: **0**.

#### --executor.continuousPaging.maxPagesPerSecond _&lt;number&gt;_

The maximum number of pages per second.

Setting this value to zero indicates no limit.

Default: **0**.

#### --executor.continuousPaging.pageSize _&lt;number&gt;_

The size of the page. The unit to use is determined by the *executor.continuousPaging.pageUnit* setting.


Default: **5000**.

#### --executor.continuousPaging.pageUnit _&lt;string&gt;_

The unit to use for the *executor.continuousPaging.pageSize* setting.

Possible values are: `ROWS`, `BYTES`.

Default: **"ROWS"**.

#### --executor.maxInFlight _&lt;number&gt;_

The maximum number of "in-flight" requests. In other words, sets the maximum number of concurrent uncompleted futures waiting for a response from the server. This acts as a safeguard against workflows that generate more requests than they can handle. Batch statements count for 1 request.

You should reduce this value when the throughput for reads and writes cannot match the throughput of mappers; this is usually a sign that the workflow engine is not well calibrated and will eventually run out of memory.

Setting this option to any negative value will disable it.

Default: **1024**.

<a name="log"></a>
## Log Settings

Log and error management settings.

#### -maxErrors,--log.maxErrors _&lt;number&gt;_

The maximum number of errors or error % to tolerate before aborting the entire operation.

Setting this value to `-1` disables this feature (not recommended).

This can be set to a fixed number like 100, or a percentage like 2%.


Default: **100**.

#### -logDir,--log.directory _&lt;string&gt;_

The directory where all log files will be stored.

Note that this must be a path pointing to a writable directory. URLs are not accepted here (not even `file:/` URLs).

Log files for a specific run, or execution, will be located in a sub-directory inside the directory specified here. Each execution generates a sub-directory identified by an "execution ID". See `engine.executionId` for more information about execution IDs.

Setting this value to `.` denotes the current working directory.

Default: **"./logs"**.

#### --log.stmt.level _&lt;string&gt;_

The desired log level.

Possible values are:
- **ABRIDGED**: only prints basic information in summarized form.
- **NORMAL**: prints basic information in summarized form, and the statement's query string, if available. For batch statements, this verbosity level also prints information about the batch's inner statements.
- **EXTENDED**: prints full information, including the statement's query string, if available, and the statement's bound values, if available. For batch statements, this verbosity level also prints all information available about the batch's inner statements.

Default: **"EXTENDED"**.

#### --log.stmt.maxBoundValueLength _&lt;number&gt;_

The maximum length for a bound value. Bound values longer than this value will be truncated.

Setting this value to `-1` disables this feature (not recommended).

Default: **50**.

#### --log.stmt.maxBoundValues _&lt;number&gt;_

The maximum number of bound values to print. If the statement has more bound values than this limit, the exceeding values will not be printed.

Setting this value to `-1` disables this feature (not recommended).

Default: **50**.

#### --log.stmt.maxInnerStatements _&lt;number&gt;_

The maximum number of inner statements to print for a batch statement.

Only applicable for batch statements, ignored otherwise.

If the batch statement has more children than this value, the exceeding child statements will not be printed.

Setting this value to `-1` disables this feature (not recommended).

Default: **10**.

#### --log.stmt.maxQueryStringLength _&lt;number&gt;_

The maximum length for a query string. Query strings longer than this value will be truncated.

Setting this value to `-1` disables this feature (not recommended).

Default: **500**.

<a name="monitoring"></a>
## Monitoring Settings

Monitoring-specific settings.

#### -reportRate,--monitoring.reportRate _&lt;string&gt;_

The report interval for the console reporter.

The console reporter will print useful metrics about the ongoing operation at this rate.

Durations lesser than one second will be rounded up to 1 second.

Default: **"5 seconds"**.

#### --monitoring.csv _&lt;boolean&gt;_

Whether or not to enable CSV reporting.

If enabled, csv files containing metrics will be generated in the designated log directory.

Default: **false**.

#### --monitoring.durationUnit _&lt;string&gt;_

The time unit to use when printing latency durations.

Valid values: all `TimeUnit` enum constants.

Default: **"MILLISECONDS"**.

#### --monitoring.expectedReads _&lt;number&gt;_

The expected total number of reads.

This information is optional; if present, the console reporter will also print the the overall achievement percentage.

Setting this value to `-1` disables this feature.

Default: **-1**.

#### --monitoring.expectedWrites _&lt;number&gt;_

The expected total number of writes.

This information is optional; if present, the console reporter will also print the the overall achievement percentage.

Setting this value to `-1` disables this feature.

Default: **-1**.

#### -jmx,--monitoring.jmx _&lt;boolean&gt;_

Whether or not to enable JMX reporting.

Note that to enable *remote* JMX reporting, several properties must also be set in the JVM during launch. This is accomplished via the `DSBULK_JAVA_OPTS` environment variable.

Default: **true**.

#### --monitoring.rateUnit _&lt;string&gt;_

The time unit to use when printing throughput rates.

Valid values: all `TimeUnit` enum constants.

Default: **"SECONDS"**.

