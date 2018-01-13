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

Load options from the given file rather than from `<dsbulk_home>/conf/application.conf`.

#### -c,--connector.name _&lt;string&gt;_

The name of the connector to use.

Default: **"csv"**.

#### -url,--connector.csv.url _&lt;string&gt;_

The URL or path of the resource(s) to read from or write to.

Which URL protocols are available depend on which URL stream handlers have been installed, but at least the **file** protocol is guaranteed to be supported for reads and writes, and the **http** and **https** protocols are guaranteed to be supported for reads.

The file protocol can be used with all supported file systems, local or not.
- When reading: the URL can point to a single file, or to an existing directory; in case of a directory, the *fileNamePattern* setting can be used to filter files to read, and the *recursive* setting can be used to control whether or not the connector should look for files in subdirectories as well.
- When writing: the URL will be treated as a directory; if it doesn't exist, the loader will attempt to create it; CSV files will be created inside this directory, and their names can be controlled with the *fileNameFormat* setting.

Note that if the value specified here does not have a protocol, then it is assumed to be a file protocol. Relative URLs will be resolved against the current working directory. Also, for convenience, if the path begins with a tilde (`~`), that symbol will be expanded to the current user's home directory.

In addition the value `-` indicates `stdin` when loading and `stdout` when unloading. This is in line with Unix tools such as tar, which uses `-` to represent stdin/stdout when reading/writing an archive.

Examples:

    url = "/path/to/dir/or/file"           # without protocol
    url = "./path/to/dir/or/file"          # without protocol, relative to working directory
    url = "~/path/to/dir/or/file"          # without protocol, relative to the user's home directory
    url = "file:///path/to/dir/or/file"    # with file protocol
    url = "http://acme.com/file.csv"       # with HTTP protocol
    url = "-"                              # to read csv data from stdin (for load) or
    url = "-"                              # write csv data to stdout (for unload)

For other URLs: the URL will be read or written directly; settings like *fileNamePattern*, *recursive*, and *fileNameFormat* will have no effect.

The default value is `-` (read from `stdin` / write to `stdout`).

Default: **"-"**.

#### -delim,--connector.csv.delimiter _&lt;string&gt;_

The character to use as field delimiter.

Default: **","**.

#### -header,--connector.csv.header _&lt;boolean&gt;_

Enable or disable whether the files to read or write begin with a header line. If enabled for loading, the first non-empty line in every file will assign field names for each record column, in lieu of `schema.mapping`, `fieldA = col1, fieldB = col2, fieldC = col3`. If disabled for loading, records will not contain fields names, only field indexes, `0 = col1, 1 = col2, 2 = col3`. For unloading, if this setting is enabled, each file will begin with a header line, and if disabled, each file will not contain a header line.

Note: This option will apply to all files loaded or unloaded.

Default: **true**.

#### -skipRecords,--connector.csv.skipRecords _&lt;number&gt;_

The number of records to skip from each input file before the parser can begin to execute. Note that if the file contains a header line, that line is not counted as a valid record. This setting is ignored when writing.

Default: **0**.

#### -maxRecords,--connector.csv.maxRecords _&lt;number&gt;_

The maximum number of records to read from or write to each file. When reading, all records past this number will be discarded. When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting. Note that when writing to anything other than a directory, this setting is ignored. This setting takes into account the *header* setting: if a file begins with a header line, that line is not counted as a record. This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### -url,--connector.json.url _&lt;string&gt;_

The URL or path of the resource(s) to read from or write to.

Which URL protocols are available depend on which URL stream handlers have been installed, but at least the **file** protocol is guaranteed to be supported for reads and writes, and the **http** and **https** protocols are guaranteed to be supported for reads.

The file protocol can be used with all supported file systems, local or not.
- When reading: the URL can point to a single file, or to an existing directory; in case of a directory, the *fileNamePattern* setting can be used to filter files to read, and the *recursive* setting can be used to control whether or not the connector should look for files in subdirectories as well.
- When writing: the URL will be treated as a directory; if it doesn't exist, the loader will attempt to create it; json files will be created inside this directory, and their names can be controlled with the *fileNameFormat* setting.

Note that if the value specified here does not have a protocol, then it is assumed to be a file protocol. Relative URLs will be resolved against the current working directory. Also, for convenience, if the path begins with a tilde (`~`), that symbol will be expanded to the current user's home directory.

In addition the value `-` indicates `stdin` when loading and `stdout` when unloading. This is in line with Unix tools such as tar, which uses `-` to represent stdin/stdout when reading/writing an archive.

Examples:

    url = "/path/to/dir/or/file"           # without protocol
    url = "./path/to/dir/or/file"          # without protocol, relative to working directory
    url = "~/path/to/dir/or/file"          # without protocol, relative to the user's home directory
    url = "file:///path/to/dir/or/file"    # with file protocol
    url = "http://acme.com/file.json"      # with HTTP protocol
    url = "-"                              # to read json data from stdin (for load) or
    url = "-"                              # write json data to stdout (for unload)

For other URLs: the URL will be read or written directly; settings like *fileNamePattern*, *recursive*, and *fileNameFormat* will have no effect.

The default value is `-` (read from `stdin` / write to `stdout`).

Default: **"-"**.

#### -skipRecords,--connector.json.skipRecords _&lt;number&gt;_

The number of JSON records to skip from each input file before the parser can begin to execute. This setting is ignored when writing.

Default: **0**.

#### -maxRecords,--connector.json.maxRecords _&lt;number&gt;_

The maximum number of records to read from or write to each file. When reading, all records past this number will be discarded. When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting. Note that when writing to anything other than a directory, this setting is ignored. This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### -k,--schema.keyspace _&lt;string&gt;_

Keyspace used for loading or unloading data. Required option if `schema.query` is not specified; otherwise, optional.

Default: **&lt;unspecified&gt;**.

#### -t,--schema.table _&lt;string&gt;_

Table used for loading or unloading data. Required option if `schema.query` is not specified; otherwise, optional.

Default: **&lt;unspecified&gt;**.

#### -m,--schema.mapping _&lt;string&gt;_

The field-to-column mapping to use, that applies to both loading and unloading. If not specified, the loader will apply a strict one-to-one mapping between the source fields and the database table. If that is not what you want, then you must supply an explicit mapping. Mappings should be specified as a map of the following form:

- Indexed data sources: `0 = col1, 1 = col2, 2 = col3`, where `0`, `1`, `2`, are the zero-based indices of fields in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map the first `n` fields is to simply specify the destination columns: `col1, col2, col3`.
- Mapped data sources: `fieldA = col1, fieldB = col2, fieldC = col3`, where `fieldA`, `fieldB`, `fieldC`, are field names in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.

To specify that a field should be used for the query timestamp or ttl, use the specially named fake columns `__ttl` and `__timestamp`: `fieldA = __ttl`. Note that TTL fields are parsed as integers representing seconds. Timestamp fields can be parsed as:

* An integer representing the number of microseconds since epoch.
* A valid date-time format specified in the options `codec.timestamp` and `codec.timeZone`.

To specify that a column should be populated with the result of a function call, specify the function call as the input field (e.g. `now() = c4`). Note, this is only relevant for load operations. In addition, for mapped data sources, it is also possible to specify that the mapping be partly auto-generated and partly explicitly specified. For example, if a source row has fields `c1`, `c2`, `c3`, and `c5`, and the table has columns `c1`, `c2`, `c3`, `c4`, one can map all like-named columns and specify that `c5` in the source maps to `c4` in the table as follows: `* = *, c5 = c4`.

One can specify that all like-named fields be mapped, except for `c2`: `* = -c2`. To skip `c2` and `c3`: `* = [-c2, -c3]`.

The exact type of mapping to use depends on the connector being used. Some connectors can only produce indexed records; others can only produce mapped ones, while others are capable of producing both indexed and mapped records at the same time. Refer to the connector's documentation to know which kinds of mapping it supports.

Default: **&lt;unspecified&gt;**.

#### -dryRun,--engine.dryRun _&lt;boolean&gt;_

Enable or disable dry-run mode, a test mode that runs the command but does not load data. Not applicable for unloading.

Default: **false**.

#### -h,--driver.hosts _&lt;string&gt;_

The contact points to use for the initial connection to the cluster. This must be a comma-separated list of hosts, each specified by a host-name or ip address. If the host is a DNS name that resolves to multiple A-records, all the corresponding addresses will be used. Do not use `localhost` as a host-name (since it resolves to both IPv4 and IPv6 addresses on some platforms). Note that each host entry may optionally be followed by `:port` to specify the port to connect to. When not specified, this value falls back to the *port* setting.

Default: **"127.0.0.1"**.

#### -port,--driver.port _&lt;number&gt;_

The port to connect to at initial contact points. Note that all nodes in a cluster must accept connections on the same port number.

Default: **9042**.

#### -u,--driver.auth.username _&lt;string&gt;_

The username to use. Providers that accept this setting:

 - `PlainTextAuthProvider`
 - `DsePlainTextAuthProvider`

Default: **"cassandra"**.

#### -p,--driver.auth.password _&lt;string&gt;_

The password to use. Providers that accept this setting:

 - `PlainTextAuthProvider`
 - `DsePlainTextAuthProvider`

Default: **"cassandra"**.

#### -cl,--driver.query.consistency _&lt;string&gt;_

The consistency level to use for both loading and unloading. Valid values are: `ANY`, `LOCAL_ONE`, `ONE`, `TWO`, `THREE`, `LOCAL_QUORUM`, `QUORUM`, `EACH_QUORUM`, `ALL`.

Default: **"LOCAL_ONE"**.

#### --executor.maxPerSecond _&lt;number&gt;_

The maximum number of concurrent operations per second. This acts as a safeguard to prevent more requests than the cluster can handle. Batch statements are counted by the number of statements included. Reduce this setting when the latencies get too high and a remote cluster cannot keep up with throughput, as `dsbulk` requests will eventually time out. Setting this option to any negative value will disable it.

Default: **-1**.

#### -maxErrors,--log.maxErrors _&lt;number&gt;_

The maximum number of errors to tolerate before aborting the entire operation. Set to either a number or a string of the form `N%` where `N` is a decimal number between 0 and 100. Setting this value to `-1` disables this feature (not recommended).

Default: **100**.

#### -logDir,--log.directory _&lt;string&gt;_

The writable directory where all log files will be stored; if the directory specified does not exist, it will be created. URLs are not acceptable (not even `file:/` URLs). Log files for a specific run, or execution, will be located in a sub-directory under the specified directory. Each execution generates a sub-directory identified by an "execution ID". See `engine.executionId` for more information about execution IDs. Relative paths will be resolved against the current working directory. Also, for convenience, if the path begins with a tilde (`~`), that symbol will be expanded to the current user's home directory.

Default: **"./logs"**.

#### -reportRate,--monitoring.reportRate _&lt;string&gt;_

The report interval for the console reporter. The console reporter will print useful metrics about the ongoing operation at this rate. Durations lesser than one second will be rounded up to 1 second.

Default: **"5 seconds"**.

<a name="connector"></a>
## Connector Settings

Connector-specific settings. This section contains settings for the connector to use; it also contains sub-sections, one for each available connector.

#### -c,--connector.name _&lt;string&gt;_

The name of the connector to use.

Default: **"csv"**.

<a name="connector.csv"></a>
### Connector Csv Settings

CSV Connector configuration.

#### -url,--connector.csv.url _&lt;string&gt;_

The URL or path of the resource(s) to read from or write to.

Which URL protocols are available depend on which URL stream handlers have been installed, but at least the **file** protocol is guaranteed to be supported for reads and writes, and the **http** and **https** protocols are guaranteed to be supported for reads.

The file protocol can be used with all supported file systems, local or not.
- When reading: the URL can point to a single file, or to an existing directory; in case of a directory, the *fileNamePattern* setting can be used to filter files to read, and the *recursive* setting can be used to control whether or not the connector should look for files in subdirectories as well.
- When writing: the URL will be treated as a directory; if it doesn't exist, the loader will attempt to create it; CSV files will be created inside this directory, and their names can be controlled with the *fileNameFormat* setting.

Note that if the value specified here does not have a protocol, then it is assumed to be a file protocol. Relative URLs will be resolved against the current working directory. Also, for convenience, if the path begins with a tilde (`~`), that symbol will be expanded to the current user's home directory.

In addition the value `-` indicates `stdin` when loading and `stdout` when unloading. This is in line with Unix tools such as tar, which uses `-` to represent stdin/stdout when reading/writing an archive.

Examples:

    url = "/path/to/dir/or/file"           # without protocol
    url = "./path/to/dir/or/file"          # without protocol, relative to working directory
    url = "~/path/to/dir/or/file"          # without protocol, relative to the user's home directory
    url = "file:///path/to/dir/or/file"    # with file protocol
    url = "http://acme.com/file.csv"       # with HTTP protocol
    url = "-"                              # to read csv data from stdin (for load) or
    url = "-"                              # write csv data to stdout (for unload)

For other URLs: the URL will be read or written directly; settings like *fileNamePattern*, *recursive*, and *fileNameFormat* will have no effect.

The default value is `-` (read from `stdin` / write to `stdout`).

Default: **"-"**.

#### -delim,--connector.csv.delimiter _&lt;string&gt;_

The character to use as field delimiter.

Default: **","**.

#### -header,--connector.csv.header _&lt;boolean&gt;_

Enable or disable whether the files to read or write begin with a header line. If enabled for loading, the first non-empty line in every file will assign field names for each record column, in lieu of `schema.mapping`, `fieldA = col1, fieldB = col2, fieldC = col3`. If disabled for loading, records will not contain fields names, only field indexes, `0 = col1, 1 = col2, 2 = col3`. For unloading, if this setting is enabled, each file will begin with a header line, and if disabled, each file will not contain a header line.

Note: This option will apply to all files loaded or unloaded.

Default: **true**.

#### -skipRecords,--connector.csv.skipRecords _&lt;number&gt;_

The number of records to skip from each input file before the parser can begin to execute. Note that if the file contains a header line, that line is not counted as a valid record. This setting is ignored when writing.

Default: **0**.

#### -maxRecords,--connector.csv.maxRecords _&lt;number&gt;_

The maximum number of records to read from or write to each file. When reading, all records past this number will be discarded. When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting. Note that when writing to anything other than a directory, this setting is ignored. This setting takes into account the *header* setting: if a file begins with a header line, that line is not counted as a record. This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### -quote,--connector.csv.quote _&lt;string&gt;_

The character used for quoting fields when the field delimiter is part of the field value. Only one character can be specified. Note that this setting applies to all files to be read or written.

Default: **"\""**.

#### -comment,--connector.csv.comment _&lt;string&gt;_

The character that represents a line comment when found in the beginning of a line of text. Only one character can be specified. Note that this setting applies to all files to be read or written. This feature is disabled by default (indicated by its `null` character value).

Default: **"\u0000"**.

#### -encoding,--connector.csv.encoding _&lt;string&gt;_

The file encoding to use for all read or written files.

Default: **"UTF-8"**.

#### -escape,--connector.csv.escape _&lt;string&gt;_

The character used for escaping quotes inside an already quoted value. Only one character can be specified. Note that this setting applies to all files to be read or written.

Default: **"\\"**.

#### --connector.csv.fileNameFormat _&lt;string&gt;_

The file name format to use when writing. This setting is ignored when reading and for non-file URLs. The file name must comply with the formatting rules of `String.format()`, and must contain a `%d` format specifier that will be used to increment file name counters.

Default: **"output-%0,6d.csv"**.

#### --connector.csv.fileNamePattern _&lt;string&gt;_

The glob pattern to use when searching for files to read. The syntax to use is the glob syntax, as described in `java.nio.file.FileSystem.getPathMatcher()`. This setting is ignored when writing and for non-file URLs. Only applicable when the *url* setting points to a directory on a known filesystem, ignored otherwise.

Default: **"\*\*/\*.csv"**.

#### --connector.csv.maxCharsPerColumn _&lt;number&gt;_

The maximum number of characters that a field can contain. This setting is used to size internal buffers and to avoid out-of-memory problems. If set to -1, internal buffers will be resized dynamically. While convenient, this can lead to memory problems. It could also hurt throughput, if some large fields require constant resizing; if this is the case, set this value to a fixed positive number that is big enough to contain all field values.

Default: **4096**.

#### -maxConcurrentFiles,--connector.csv.maxConcurrentFiles _&lt;string&gt;_

The maximum number of files that can be written simultaneously. This setting is ignored when reading and when the output URL is anything other than a directory on a filesystem. The special syntax `NC` can be used to specify a number of threads that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 threads.

Default: **"0.25C"**.

#### --connector.csv.recursive _&lt;boolean&gt;_

Enable or disable scanning for files in the root's subdirectories. Only applicable when *url* is set to a directory on a known filesystem. Used for loading only.

Default: **false**.

<a name="connector.json"></a>
### Connector Json Settings

JSON Connector configuration.

#### -url,--connector.json.url _&lt;string&gt;_

The URL or path of the resource(s) to read from or write to.

Which URL protocols are available depend on which URL stream handlers have been installed, but at least the **file** protocol is guaranteed to be supported for reads and writes, and the **http** and **https** protocols are guaranteed to be supported for reads.

The file protocol can be used with all supported file systems, local or not.
- When reading: the URL can point to a single file, or to an existing directory; in case of a directory, the *fileNamePattern* setting can be used to filter files to read, and the *recursive* setting can be used to control whether or not the connector should look for files in subdirectories as well.
- When writing: the URL will be treated as a directory; if it doesn't exist, the loader will attempt to create it; json files will be created inside this directory, and their names can be controlled with the *fileNameFormat* setting.

Note that if the value specified here does not have a protocol, then it is assumed to be a file protocol. Relative URLs will be resolved against the current working directory. Also, for convenience, if the path begins with a tilde (`~`), that symbol will be expanded to the current user's home directory.

In addition the value `-` indicates `stdin` when loading and `stdout` when unloading. This is in line with Unix tools such as tar, which uses `-` to represent stdin/stdout when reading/writing an archive.

Examples:

    url = "/path/to/dir/or/file"           # without protocol
    url = "./path/to/dir/or/file"          # without protocol, relative to working directory
    url = "~/path/to/dir/or/file"          # without protocol, relative to the user's home directory
    url = "file:///path/to/dir/or/file"    # with file protocol
    url = "http://acme.com/file.json"      # with HTTP protocol
    url = "-"                              # to read json data from stdin (for load) or
    url = "-"                              # write json data to stdout (for unload)

For other URLs: the URL will be read or written directly; settings like *fileNamePattern*, *recursive*, and *fileNameFormat* will have no effect.

The default value is `-` (read from `stdin` / write to `stdout`).

Default: **"-"**.

#### -skipRecords,--connector.json.skipRecords _&lt;number&gt;_

The number of JSON records to skip from each input file before the parser can begin to execute. This setting is ignored when writing.

Default: **0**.

#### -maxRecords,--connector.json.maxRecords _&lt;number&gt;_

The maximum number of records to read from or write to each file. When reading, all records past this number will be discarded. When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting. Note that when writing to anything other than a directory, this setting is ignored. This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### --connector.json.mode _&lt;string&gt;_

The mode for loading and unloading JSON documents. Valid values are:

* MULTI_DOCUMENT: Each resource may contain an arbitrary number of successive JSON documents to be mapped to records. For example the format of each JSON document is a single document: `{doc1}`. The root directory for the JSON documents can be specified with `url` and the documents can be read recursively by setting `connector.json.recursive` to true.
* SINGLE_DOCUMENT: Each resource contains a root array whose elements are JSON documents to be mapped to records. For example, the format of the JSON document is an array with embedded JSON documents: `[ {doc1}, {doc2}, {doc3} ]`.

Default: **"MULTI_DOCUMENT"**.

#### -encoding,--connector.json.encoding _&lt;string&gt;_

The file encoding to use for all read or written files.

Default: **"UTF-8"**.

#### --connector.json.fileNameFormat _&lt;string&gt;_

The file name format to use when writing. This setting is ignored when reading and for non-file URLs. The file name must comply with the formatting rules of `String.format()`, and must contain a `%d` format specifier that will be used to increment file name counters.

Default: **"output-%0,6d.json"**.

#### --connector.json.fileNamePattern _&lt;string&gt;_

The glob pattern to use when searching for files to read. The syntax to use is the glob syntax, as described in `java.nio.file.FileSystem.getPathMatcher()`. This setting is ignored when writing and for non-file URLs. Only applicable when the *url* setting points to a directory on a known filesystem, ignored otherwise.

Default: **"\*\*/\*.json"**.

#### --connector.json.generatorFeatures _&lt;list&gt;_

JSON generator features to enable. Valid values are all the enum constants defined in `com.fasterxml.jackson.core.JsonGenerator.Feature`. For example, a value of `[ESCAPE_NON_ASCII, QUOTE_FIELD_NAMES]` will configure the generator to escape all characters beyond 7-bit ASCII and quote field names when generating JSON output. Used for unloading only.

Default: **[]**.

#### -maxConcurrentFiles,--connector.json.maxConcurrentFiles _&lt;string&gt;_

The maximum number of files that can be written simultaneously. This setting is ignored when reading and when the output URL is anything other than a directory on a filesystem. The special syntax `NC` can be used to specify a number of threads that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 threads.

Default: **"0.25C"**.

#### --connector.json.parserFeatures _&lt;list&gt;_

JSON parser features to enable. Valid values are all the enum constants defined in `com.fasterxml.jackson.core.JsonParser.Feature`. For example, a value of `[ALLOW_COMMENTS, ALLOW_SINGLE_QUOTES]` will configure the parser to allow the use of comments and single-quoted strings in JSON data. Used for loading only.

Default: **[]**.

#### --connector.json.prettyPrint _&lt;boolean&gt;_

Enable or disable pretty printing. When enabled, JSON records are written with indents. Used for unloading only.

Note: Can result in much bigger records.

Default: **false**.

#### --connector.json.recursive _&lt;boolean&gt;_

Enable or disable scanning for files in the root's subdirectories. Only applicable when *url* is set to a directory on a known filesystem. Used for loading only.

Default: **false**.

<a name="schema"></a>
## Schema Settings

Schema-specific settings.

#### -k,--schema.keyspace _&lt;string&gt;_

Keyspace used for loading or unloading data. Required option if `schema.query` is not specified; otherwise, optional.

Default: **&lt;unspecified&gt;**.

#### -t,--schema.table _&lt;string&gt;_

Table used for loading or unloading data. Required option if `schema.query` is not specified; otherwise, optional.

Default: **&lt;unspecified&gt;**.

#### -m,--schema.mapping _&lt;string&gt;_

The field-to-column mapping to use, that applies to both loading and unloading. If not specified, the loader will apply a strict one-to-one mapping between the source fields and the database table. If that is not what you want, then you must supply an explicit mapping. Mappings should be specified as a map of the following form:

- Indexed data sources: `0 = col1, 1 = col2, 2 = col3`, where `0`, `1`, `2`, are the zero-based indices of fields in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map the first `n` fields is to simply specify the destination columns: `col1, col2, col3`.
- Mapped data sources: `fieldA = col1, fieldB = col2, fieldC = col3`, where `fieldA`, `fieldB`, `fieldC`, are field names in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.

To specify that a field should be used for the query timestamp or ttl, use the specially named fake columns `__ttl` and `__timestamp`: `fieldA = __ttl`. Note that TTL fields are parsed as integers representing seconds. Timestamp fields can be parsed as:

* An integer representing the number of microseconds since epoch.
* A valid date-time format specified in the options `codec.timestamp` and `codec.timeZone`.

To specify that a column should be populated with the result of a function call, specify the function call as the input field (e.g. `now() = c4`). Note, this is only relevant for load operations. In addition, for mapped data sources, it is also possible to specify that the mapping be partly auto-generated and partly explicitly specified. For example, if a source row has fields `c1`, `c2`, `c3`, and `c5`, and the table has columns `c1`, `c2`, `c3`, `c4`, one can map all like-named columns and specify that `c5` in the source maps to `c4` in the table as follows: `* = *, c5 = c4`.

One can specify that all like-named fields be mapped, except for `c2`: `* = -c2`. To skip `c2` and `c3`: `* = [-c2, -c3]`.

The exact type of mapping to use depends on the connector being used. Some connectors can only produce indexed records; others can only produce mapped ones, while others are capable of producing both indexed and mapped records at the same time. Refer to the connector's documentation to know which kinds of mapping it supports.

Default: **&lt;unspecified&gt;**.

#### -nullStrings,--schema.nullStrings _&lt;string&gt;_

Comma-separated list of strings that should be mapped to `null`. For loading, when a record field value exactly matches one of the specified strings, the value is replaced with `null` before writing to DSE. For unloading, only the first string specified will be used to change a row cell containing `null` to the specified string when written out. By default, empty strings are converted to `null` while loading, and `null` is converted to an empty string while unloading. This setting is applied before `schema.nullToUnset`, hence any `null` produced by a null-string can still be left unset if required.

Default: **&lt;unspecified&gt;**.

#### --schema.nullToUnset _&lt;boolean&gt;_

Specify whether to map `null` input values to "unset" in the database, i.e., don't modify a potentially pre-existing value of this field for this row. Valid for load scenarios, otherwise ignore. Note that setting to false creates tombstones to represent `null`.

Note that this setting is applied after the *schema.nullStrings* setting, and may intercept `null`s produced by that setting.

Default: **true**.

#### -query,--schema.query _&lt;string&gt;_

The query to use. If not specified, then *schema.keyspace* and *schema.table* must be specified, and dsbulk will infer the appropriate statement based on the table's metadata, using all available columns. If `schema.keyspace` is provided, the query need not include the keyspace to qualify the table reference.

For loading, the statement can be any `INSERT` or `UPDATE` statement, but must use named bound variables exclusively; positional bound variables will not work. Bound variable names usually match those of the columns in the destination table, but this is not a strict requirement; it is, however, required that their names match those specified in the mapping.

For unloading, the statement can be any regular `SELECT` statement; it can optionally contain a token range restriction clause of the form: `token(...) > :start and token(...) <= :end`. If such a clause is present, the engine will generate as many statements as there are token ranges in the cluster, thus allowing parallelization of reads while at the same time targeting coordinators that are also replicas. The column names in the SELECT clause will be used to match column names specified in the mapping.

Note: The dsbulk query is parsed to discover which bound variables are present, to map the variable correctly to fields.

See *schema.mapping* setting for more information.

Default: **&lt;unspecified&gt;**.

#### --schema.queryTimestamp _&lt;string&gt;_

The timestamp of inserted/updated cells during load; otherwise, the current time of the system running the tool is used. Not applicable to unloading. The following formats are supported:

* A numeric timestamp that is parsed using the options `codec.unit` and `codec.epoch`.
* A valid date-time format specified in the options `codec.timestamp` and `codec.timeZone`.

Query timestamps for DSE have microsecond resolution; any sub-microsecond information specified is lost. For more information, see the [CQL Reference](https://docs.datastax.com/en/dse/5.1/cql/cql/cql_reference/cql_commands/cqlInsert.html#cqlInsert__timestamp-value).

Default: **&lt;unspecified&gt;**.

#### --schema.queryTtl _&lt;number&gt;_

The Time-To-Live (TTL) of inserted/updated cells during load (seconds); a value of -1 means there is no TTL. Not applicable to unloading. For more information, see the [CQL Reference](https://docs.datastax.com/en/dse/5.1/cql/cql/cql_reference/cql_commands/cqlInsert.html#cqlInsert__ime-value), [Setting the time-to-live (TTL) for value](http://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/useTTL.html) and [Expiring data with time-to-live](http://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/useExpire.html).

Default: **-1**.

<a name="batch"></a>
## Batch Settings

Batch-specific settings.

These settings control how the workflow engine groups together statements before writing them.

Only applicable for loading.

See `com.datastax.dsbulk.executor.api.batch.StatementBatcher` for more information.

#### --batch.bufferSize _&lt;number&gt;_

The buffer size to use for flushing batching statements. Do not set higher than `maxBatchSize` unless the loaded data is unsorted, when a higher value could improve performance. When set to a negative value the buffer size is implicitly set to `maxBatchSize`.

Default: **-1**.

#### --batch.enabled _&lt;boolean&gt;_

Enable or disable statement batching.

Default: **true**.

#### --batch.maxBatchSize _&lt;number&gt;_

The maximum batch size that depends on the size of the data inserted and the batch mode in use. Larger data requires a smaller value. For batch mode, `PARTITION_KEY` requires larger batch sizes, whereas `REPLICA_SET` requires smaller batch sizes, such as below 10.

Default: **32**.

#### --batch.mode _&lt;string&gt;_

The grouping mode. Valid values are:
- PARTITION_KEY: Groups together statements that share the same partition key. This is the default mode, and the preferred one.
- REPLICA_SET: Groups together statements that share the same replica set. This mode might yield better results for small clusters and lower replication factors, but tends to perform equally well or worse than `PARTITION_KEY` for larger clusters or high replication factors.

Default: **"PARTITION_KEY"**.

<a name="codec"></a>
## Codec Settings

Conversion-specific settings. These settings apply for both load and unload workflows.

When writing, these settings determine how record fields emitted by connectors are parsed.

When unloading, these settings determine how row cells emitted by DSE are formatted.

#### --codec.booleanNumbers _&lt;list&lt;number&gt;&gt;_

Set how true and false representations of numbers are interpreted. The representation is of the form `true_value,false_value`. The mapping is reciprocal, so that numbers are mapping to Boolean and vice versa. All numbers unspecified in this setting are rejected.

Default: **[1,0]**.

#### --codec.booleanWords _&lt;list&lt;string&gt;&gt;_

Specify how true and false representations can be used by dsbulk. Each representation is of the form `true-value:false-value`, case-insensitive. For loading, all representations are honored. For unloading, the first representation will be used and all others ignored.

Default: **["1:0","Y:N","T:F","YES:NO","TRUE:FALSE"]**.

#### --codec.date _&lt;string&gt;_

The temporal pattern to use for `String` to CQL date conversions. Valid choices:

- A date-time pattern
- A pre-defined formatter such as `ISO_LOCAL_DATE`

For more information on patterns and pre-defined formatters, see [https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns).

Default: **"ISO_LOCAL_DATE"**.

#### --codec.epoch _&lt;string&gt;_

This setting applies only to CQL timestamp columns, and `USING TIMESTAMP` clauses in queries. If the input is a string containing only digits that cannot be parsed using the `codec.timestamp` format, the specified epoch determines the relative point in time used with the parsed value. The value must be a valid timestamp, defined by options `codec.timestamp` and `codec.timeZone` (if the value does not include a time zone).

Default: **"1970-01-01T00:00:00Z"**.

#### -locale,--codec.locale _&lt;string&gt;_

The locale to use for locale-sensitive conversions.

Default: **"en_US"**.

#### --codec.number _&lt;string&gt;_

The `DecimalFormat` pattern to use for `String` to `Number` conversions. See [java.text.DecimalFormat](https://docs.oracle.com/javase/8/docs/api/java/text/DecimalFormat.html) for details about the pattern syntax to use.

Default: **"#,###.##"**.

#### --codec.time _&lt;string&gt;_

The temporal pattern to use for `String` to CQL time conversions. Valid choices:

- A date-time pattern
- A pre-defined formatter such as `ISO_LOCAL_TIME`

For more information on patterns and pre-defined formatters, see [https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns).

Default: **"ISO_LOCAL_TIME"**.

#### -timeZone,--codec.timeZone _&lt;string&gt;_

The time zone to use for temporal conversions that do not convey any explicit time zone information.

Default: **"UTC"**.

#### --codec.timestamp _&lt;string&gt;_

The temporal pattern to use for `String` to CQL timestamp conversions. Valid choices:

- A date-time pattern
- A pre-defined formatter such as `ISO_DATE_TIME`
- The special formatter `CQL_DATE_TIME`, which is a special parser that accepts all valid CQL literal formats for the `timestamp` type

For more information on patterns and pre-defined formatters, see [https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns).

Default: **"CQL_DATE_TIME"**.

#### --codec.unit _&lt;string&gt;_

This setting applies only to CQL timestamp columns, and `USING TIMESTAMP` clauses in queries. If the input is a string containing only digits that cannot be parsed using the `codec.timestamp` format, the specified time unit is applied to the parsed value. All `TimeUnit` enum constants are valid choices.

Default: **"MILLISECONDS"**.

#### --codec.uuidStrategy _&lt;string&gt;_

Strategy to use when generating time-based (version 1) UUIDs from timestamps. Clock sequence and node ID parts of generated UUIDs are determined on a best-effort basis and are not fully compliant with RFC 4122. Valid values are:

- RANDOM: Generates UUIDs using a random number in lieu of the local clock sequence and node ID. This strategy will ensure that the generated UUIDs are unique, even if the original timestamps are not guaranteed to be unique.
- FIXED: Preferred strategy if original timestamps are guaranteed unique, since it is faster. Generates UUIDs using a fixed local clock sequence and node ID.
- MIN: Generates the smallest possible type 1 UUID for a given timestamp. Warning: this strategy doesn't guarantee uniquely generated UUIDs and should be used with caution.
- MAX: Generates the biggest possible type 1 UUID for a given timestamp. Warning: this strategy doesn't guarantee uniquely generated UUIDs and should be used with caution.

Default: **"RANDOM"**.

<a name="driver"></a>
## Driver Settings

Driver-specific configuration.

#### -h,--driver.hosts _&lt;string&gt;_

The contact points to use for the initial connection to the cluster. This must be a comma-separated list of hosts, each specified by a host-name or ip address. If the host is a DNS name that resolves to multiple A-records, all the corresponding addresses will be used. Do not use `localhost` as a host-name (since it resolves to both IPv4 and IPv6 addresses on some platforms). Note that each host entry may optionally be followed by `:port` to specify the port to connect to. When not specified, this value falls back to the *port* setting.

Default: **"127.0.0.1"**.

#### -port,--driver.port _&lt;number&gt;_

The port to connect to at initial contact points. Note that all nodes in a cluster must accept connections on the same port number.

Default: **9042**.

#### --driver.addressTranslator _&lt;string&gt;_

The simple or fully-qualified class name of the address translator to use. This is only needed if the nodes are not directly reachable from the machine on which dsbulk is running (for example, the dsbulk machine is in a different network region and needs to use a public IP, or it connects through a proxy).

Default: **"IdentityTranslator"**.

#### --driver.timestampGenerator _&lt;string&gt;_

The simple or fully-qualified class name of the timestamp generator to use. Built-in options are:

- AtomicMonotonicTimestampGenerator: timestamps are guaranteed to be unique across all client threads.
- ThreadLocalTimestampGenerator: timestamps are guaranteed to be unique within each thread only.
- ServerSideTimestampGenerator: do not generate timestamps, let the server assign them.

Default: **"AtomicMonotonicTimestampGenerator"**.

<a name="driver.auth"></a>
### Driver Auth Settings

Authentication settings.

#### -u,--driver.auth.username _&lt;string&gt;_

The username to use. Providers that accept this setting:

 - `PlainTextAuthProvider`
 - `DsePlainTextAuthProvider`

Default: **"cassandra"**.

#### -p,--driver.auth.password _&lt;string&gt;_

The password to use. Providers that accept this setting:

 - `PlainTextAuthProvider`
 - `DsePlainTextAuthProvider`

Default: **"cassandra"**.

#### --driver.auth.provider _&lt;string&gt;_

The name of the AuthProvider to use. Valid choices are:

 - None: no authentication.
 - PlainTextAuthProvider: Uses `com.datastax.driver.core.PlainTextAuthProvider` for authentication. Supports SASL authentication using the `PLAIN` mechanism (plain text authentication).
 - DsePlainTextAuthProvider: Uses `com.datastax.driver.dse.auth.DsePlainTextAuthProvider` for authentication. Supports SASL authentication to DSE clusters using the `PLAIN` mechanism (plain text authentication), and also supports optional proxy authentication; should be preferred to `PlainTextAuthProvider` when connecting to secured DSE clusters.
 - DseGSSAPIAuthProvider: Uses `com.datastax.driver.dse.auth.DseGSSAPIAuthProvider` for authentication. Supports SASL authentication to DSE clusters using the `GSSAPI` mechanism (Kerberos authentication), and also supports optional proxy authentication.
   - Note: When using this provider you may have to set the `java.security.krb5.conf` system property to point to your `krb5.conf` file (e.g. set the `DSBULK_JAVA_OPTS` environment variable to `-Djava.security.krb5.conf=/home/user/krb5.conf`). See the [Oracle Java Kerberos documentation](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html) for more details.

Default: **"None"**.

#### --driver.auth.authorizationId _&lt;string&gt;_

An authorization ID allows the currently authenticated user to act as a different user (proxy authentication). Providers that accept this setting:

 - `DsePlainTextAuthProvider`
 - `DseGSSAPIAuthProvider`

Default: **&lt;unspecified&gt;**.

#### --driver.auth.keyTab _&lt;string&gt;_

The path of the Kerberos keytab file to use for authentication. If left unspecified, authentication uses a ticket cache. Providers that accept this setting:

 - `DseGSSAPIAuthProvider`

Default: **&lt;unspecified&gt;**.

#### --driver.auth.principal _&lt;string&gt;_

The Kerberos principal to use. For example, `user@datastax.com`. Providers that accept this setting:
 - `DseGSSAPIAuthProvider`

Default: **&lt;unspecified&gt;**.

#### --driver.auth.saslProtocol _&lt;string&gt;_

The SASL protocol name to use. This value should match the username of the Kerberos service principal used by the DSE server. This information is specified in the `dse.yaml` file by the *service_principal* option under the *kerberos_options* section, and may vary from one DSE installation to another - especially if you installed DSE with an automated package installer. Providers that accept this setting:

 - `DseGSSAPIAuthProvider`

Default: **"dse"**.

<a name="driver.policy"></a>
### Driver Policy Settings

Settings for various driver policies.

#### -lbp,--driver.policy.lbp.name _&lt;string&gt;_

The name of the load balancing policy. Supported policies include: `dse`, `dcAwareRoundRobin`, `roundRobin`, `whiteList`, `tokenAware`. Available options for the policies are listed below as appropriate. For more information, refer to the driver documentation for the policy. If not specified, defaults to the driver's default load balancing policy, which is currently the `DseLoadBalancingPolicy` wrapping a `TokenAwarePolicy`, wrapping a `DcAwareRoundRobinPolicy`.

Note: It is critical for a token-aware policy to be used in the chain in order to benefit from batching by partition key.

Default: **&lt;unspecified&gt;**.

#### --driver.policy.lbp.dcAwareRoundRobin.allowRemoteDCsForLocalConsistencyLevel _&lt;boolean&gt;_

Enable or disable whether to allow remote datacenters to count for local consistency level in round robin awareness.

Default: **false**.

#### --driver.policy.lbp.dcAwareRoundRobin.localDc _&lt;string&gt;_

The datacenter name (commonly dc1, dc2, etc.) local to the machine on which dsbulk is running, so that requests are sent to nodes in the local datacenter whenever possible.

Default: **&lt;unspecified&gt;**.

#### --driver.policy.lbp.dcAwareRoundRobin.usedHostsPerRemoteDc _&lt;number&gt;_

The number of hosts per remote datacenter that the round robin policy should consider.

Default: **0**.

#### --driver.policy.lbp.dse.childPolicy _&lt;string&gt;_

The child policy that the specified `dse` policy wraps.

Default: **"roundRobin"**.

#### --driver.policy.lbp.tokenAware.childPolicy _&lt;string&gt;_

The child policy that the specified `tokenAware` policy wraps.

Default: **"roundRobin"**.

#### --driver.policy.lbp.tokenAware.shuffleReplicas _&lt;boolean&gt;_

Specify whether to shuffle the list of replicas that can process a request. For loading, shuffling can improve performance by distributing writes across nodes.

Default: **true**.

#### --driver.policy.lbp.whiteList.childPolicy _&lt;string&gt;_

The child policy that the specified `whiteList` policy wraps.

Default: **"roundRobin"**.

#### --driver.policy.lbp.whiteList.hosts _&lt;string&gt;_

List of hosts to white list. This must be a comma-separated list of hosts, each specified by a host-name or ip address. If the host is a DNS name that resolves to multiple A-records, all the corresponding addresses will be used. Do not use `localhost` as a host-name (since it resolves to both IPv4 and IPv6 addresses on some platforms).

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

The maximum number of requests (1 to 32768) that can be executed concurrently on a connection.

Default: **32768**.

#### --driver.pooling.remote.connections _&lt;number&gt;_

The number of connections in the pool for remote nodes.

Default: **1**.

#### --driver.pooling.remote.requests _&lt;number&gt;_

The maximum number of requests (1 to 32768) that can be executed concurrently on a connection.

Default: **1024**.

<a name="driver.protocol"></a>
### Driver Protocol Settings

Native Protocol-specific settings.

#### --driver.protocol.compression _&lt;string&gt;_

Specify the compression algorithm to use. Valid values are: `NONE`, `LZ4`, `SNAPPY`.

Default: **"NONE"**.

<a name="driver.query"></a>
### Driver Query Settings

Query-related settings.

#### -cl,--driver.query.consistency _&lt;string&gt;_

The consistency level to use for both loading and unloading. Valid values are: `ANY`, `LOCAL_ONE`, `ONE`, `TWO`, `THREE`, `LOCAL_QUORUM`, `QUORUM`, `EACH_QUORUM`, `ALL`.

Default: **"LOCAL_ONE"**.

#### --driver.query.fetchSize _&lt;number&gt;_

The page size, or how many rows will be retrieved simultaneously in a single network round trip. This setting will limit the number of results loaded into memory simultaneously during unloading. Not applicable for loading.

Default: **5000**.

#### --driver.query.idempotence _&lt;boolean&gt;_

The default idempotence of statements generated by the loader.

Default: **true**.

#### --driver.query.serialConsistency _&lt;string&gt;_

The serial consistency level to use for writes. Only applicable if the data is inserted using lightweight transactions, ignored otherwise. Valid values are: `SERIAL` and `LOCAL_SERIAL`.

Default: **"LOCAL_SERIAL"**.

<a name="driver.socket"></a>
### Driver Socket Settings

Socket-related settings.

#### --driver.socket.readTimeout _&lt;string&gt;_

The time the driver waits for a request to complete. This is a global limit on the duration of a `session.execute()` call, including any internal retries the driver might do.

Default: **"60 seconds"**.

<a name="driver.ssl"></a>
### Driver Ssl Settings

Encryption-specific settings.

For more information about how to configure this section, see the Java Secure Socket Extension (JSSE) Reference Guide: http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html. You can also check the DataStax Java driver documentation on SSL: http://docs.datastax.com/en/developer/java-driver-dse/latest/manual/ssl/

#### --driver.ssl.cipherSuites _&lt;list&gt;_

The cipher suites to enable. For example:

`cipherSuites = ["TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"]`

This property is optional. If it is not present, the driver won't explicitly enable cipher suites, which according to the JDK documentation results in "a minimum quality of service".

Default: **[]**.

#### --driver.ssl.keystore.algorithm _&lt;string&gt;_

The algorithm to use for the SSL keystore. Valid values are: `SunX509`, `NewSunX509`.

Default: **"SunX509"**.

#### --driver.ssl.keystore.password _&lt;string&gt;_

The keystore password.

Default: **&lt;unspecified&gt;**.

#### --driver.ssl.keystore.path _&lt;string&gt;_

The path of the keystore file. This setting is optional. If left unspecified, no client authentication will be used.

Default: **&lt;unspecified&gt;**.

#### --driver.ssl.openssl.keyCertChain _&lt;string&gt;_

The path of the certificate chain file. This setting is optional. If left unspecified, no client authentication will be used.

Default: **&lt;unspecified&gt;**.

#### --driver.ssl.openssl.privateKey _&lt;string&gt;_

The path of the private key file.

Default: **&lt;unspecified&gt;**.

#### --driver.ssl.provider _&lt;string&gt;_

The SSL provider to use. Valid values are:

- **None**: no SSL.
- **JDK**: uses the JDK SSLContext
- **OpenSSL**: uses Netty's native support for OpenSSL. It provides better performance and generates less garbage. This is the recommended provider when using SSL.

Default: **"None"**.

#### --driver.ssl.truststore.algorithm _&lt;string&gt;_

The algorithm to use for the SSL truststore. Valid values are: `PKIX`, `SunX509`.

Default: **"SunX509"**.

#### --driver.ssl.truststore.password _&lt;string&gt;_

The truststore password.

Default: **&lt;unspecified&gt;**.

#### --driver.ssl.truststore.path _&lt;string&gt;_

The path of the truststore file. This setting is optional. If left unspecified, server certificates will not be validated.

Default: **&lt;unspecified&gt;**.

<a name="engine"></a>
## Engine Settings

Workflow Engine-specific settings.

#### -dryRun,--engine.dryRun _&lt;boolean&gt;_

Enable or disable dry-run mode, a test mode that runs the command but does not load data. Not applicable for unloading.

Default: **false**.

#### --engine.executionId _&lt;string&gt;_

A unique identifier to attribute to each execution. When unspecified or empty, the engine will automatically generate identifiers of the following form: *workflow*_*timestamp*, where :

- *workflow* stands for the workflow type (`LOAD`, `UNLOAD`, etc.);
- *timestamp* is the current timestamp formatted as `uuuuMMdd-HHmmss-SSSSSS` (see [https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns)) in UTC, with microsecond precision if available, and millisecond precision otherwise.

When this identifier is user-supplied, it is important to guarantee its uniqueness; failing to do so may result in execution failures. It is also possible to provide templates here. Any format compliant with the formatting rules of [`String.format()`](https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html#syntax) is accepted, and can contain the following parameters:

- `%1$s` : the workflow type (`LOAD`, `UNLOAD`, etc.);
- `%2$t` : the current time (with microsecond precision if available, and millisecond precision otherwise);
- `%3$s` : the JVM process PID (this parameter might not be available on some operating systems; if its value cannot be determined, a random integer will be inserted instead).

Default: **&lt;unspecified&gt;**.

<a name="executor"></a>
## Executor Settings

Executor-specific settings.

#### --executor.maxPerSecond _&lt;number&gt;_

The maximum number of concurrent operations per second. This acts as a safeguard to prevent more requests than the cluster can handle. Batch statements are counted by the number of statements included. Reduce this setting when the latencies get too high and a remote cluster cannot keep up with throughput, as `dsbulk` requests will eventually time out. Setting this option to any negative value will disable it.

Default: **-1**.

#### --executor.continuousPaging.enabled _&lt;boolean&gt;_

Enable or disable continuous paging. If the target cluster does not support continuous paging, traditional paging will be used regardless of this setting.

Default: **true**.

#### --executor.continuousPaging.maxPages _&lt;number&gt;_

The maximum number of pages to retrieve. Setting this value to zero retrieves all pages available.

Default: **0**.

#### --executor.continuousPaging.maxPagesPerSecond _&lt;number&gt;_

The maximum number of pages per second. Setting this value to zero indicates no limit.

Default: **0**.

#### --executor.continuousPaging.pageSize _&lt;number&gt;_

The size of the page. The unit to use is determined by the `pageUnit` setting.

Default: **5000**.

#### --executor.continuousPaging.pageUnit _&lt;string&gt;_

The unit to use for the `pageSize` setting. Possible values are: `ROWS`, `BYTES`.

Default: **"ROWS"**.

#### --executor.maxInFlight _&lt;number&gt;_

The maximum number of "in-flight" requests, or maximum number of concurrent requests waiting for a response from the server. This acts as a safeguard to prevent more requests than the cluster can handle. Batch statements count as one request. Reduce this value when the throughput for reads and writes cannot match the throughput of mappers; this is usually a sign that the workflow engine is not well calibrated and will eventually run out of memory. Setting this option to any negative value will disable it.

Default: **1024**.

<a name="log"></a>
## Log Settings

Log and error management settings.

#### -maxErrors,--log.maxErrors _&lt;number&gt;_

The maximum number of errors to tolerate before aborting the entire operation. Set to either a number or a string of the form `N%` where `N` is a decimal number between 0 and 100. Setting this value to `-1` disables this feature (not recommended).

Default: **100**.

#### -logDir,--log.directory _&lt;string&gt;_

The writable directory where all log files will be stored; if the directory specified does not exist, it will be created. URLs are not acceptable (not even `file:/` URLs). Log files for a specific run, or execution, will be located in a sub-directory under the specified directory. Each execution generates a sub-directory identified by an "execution ID". See `engine.executionId` for more information about execution IDs. Relative paths will be resolved against the current working directory. Also, for convenience, if the path begins with a tilde (`~`), that symbol will be expanded to the current user's home directory.

Default: **"./logs"**.

#### --log.stmt.level _&lt;string&gt;_

The desired log level. Valid values are:

- ABRIDGED: Print only basic information in summarized form.
- NORMAL: Print basic information in summarized form, and the statement's query string, if available. For batch statements, this verbosity level also prints information about the batch's inner statements.
- EXTENDED: Print full information, including the statement's query string, if available, and the statement's bound values, if available. For batch statements, this verbosity level also prints all information available about the batch's inner statements.

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

The maximum number of inner statements to print for a batch statement. Only applicable for batch statements, ignored otherwise. If the batch statement has more children than this value, the exceeding child statements will not be printed.

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

The report interval for the console reporter. The console reporter will print useful metrics about the ongoing operation at this rate. Durations lesser than one second will be rounded up to 1 second.

Default: **"5 seconds"**.

#### --monitoring.csv _&lt;boolean&gt;_

Enable or disable CSV reporting. If enabled, CSV files containing metrics will be generated in the designated log directory.

Default: **false**.

#### --monitoring.durationUnit _&lt;string&gt;_

The time unit used when printing latency durations. Valid values: all `TimeUnit` enum constants.

Default: **"MILLISECONDS"**.

#### --monitoring.expectedReads _&lt;number&gt;_

The expected total number of reads. Optional, but if set, the console reporter will also print the overall achievement percentage. Setting this value to `-1` disables this feature.

Default: **-1**.

#### --monitoring.expectedWrites _&lt;number&gt;_

The expected total number of writes. Optional, but if set, the console reporter will also print the overall achievement percentage. Setting this value to `-1` disables this feature.

Default: **-1**.

#### -jmx,--monitoring.jmx _&lt;boolean&gt;_

Enable or disable JMX reporting. Note that to enable remote JMX reporting, several properties must also be set in the JVM during launch. This is accomplished via the `DSBULK_JAVA_OPTS` environment variable.

Default: **true**.

#### --monitoring.rateUnit _&lt;string&gt;_

The time unit used when printing throughput rates. Valid values: all `TimeUnit` enum constants.

Default: **"SECONDS"**.

