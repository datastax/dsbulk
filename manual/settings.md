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
<a href="#stats">Stats Settings</a><br>
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

If there is an error opening or reading one of the provided URLs, it will be considered recoverable and processing will continue for other URLs. See log.maxErrors setting for more information.

The default value is `-` (read from `stdin` / write to `stdout`).

Default: **"-"**.

#### -skipRecords,--connector.json.skipRecords _&lt;number&gt;_

The number of JSON records to skip from each input file before the parser can begin to execute. This setting is ignored when writing.

Default: **0**.

#### -maxRecords,--connector.json.maxRecords _&lt;number&gt;_

The maximum number of records to read from or write to each file. When reading, all records past this number will be discarded. When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting. Note that when writing to anything other than a directory, this setting is ignored. This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### -k,--schema.keyspace _&lt;string&gt;_

Keyspace used for loading or unloading data. Keyspace names should not be quoted and are case-sensitive. `MyKeyspace` will match a keyspace named `MyKeyspace` but not `mykeyspace`. Required option if `schema.query` is not specified; otherwise, optional.

Default: **null**.

#### -t,--schema.table _&lt;string&gt;_

Table used for loading or unloading data. Table names should not be quoted and are case-sensitive. `MyTable` will match a table named `MyTable` but not `mytable`. Required option if `schema.query` is not specified; otherwise, optional.

Default: **null**.

#### -m,--schema.mapping _&lt;string&gt;_

The field-to-column mapping to use, that applies to both loading and unloading; ignored when counting. If not specified, the loader will apply a strict one-to-one mapping between the source fields and the database table. If that is not what you want, then you must supply an explicit mapping. Mappings should be specified as a map of the following form:

- Indexed data sources: `0 = col1, 1 = col2, 2 = col3`, where `0`, `1`, `2`, are the zero-based indices of fields in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map the first `n` fields is to simply specify the destination columns: `col1, col2, col3`.
- Mapped data sources: `fieldA = col1, fieldB = col2, fieldC = col3`, where `fieldA`, `fieldB`, `fieldC`, are field names in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map fields named like columns is to simply specify the destination columns: `col1, col2, col3`.

To specify that a field should be used as the timestamp (a.k.a. write-time) or ttl (a.k.a. time-to-live) of the inserted row, use the specially named fake columns `__ttl` and `__timestamp`: `fieldA = __timestamp, fieldB = __ttl`. Note that Timestamp fields are parsed as regular CQL timestamp columns and must comply with either `codec.timestamp`, or alternatively, with `codec.unit` + `codec.epoch`. TTL fields are parsed as integers representing durations in seconds, and must comply with `codec.number`.

To specify that a column should be populated with the result of a function call, specify the function call as the input field (e.g. `now() = c4`). Note, this is only relevant for load operations. Similarly, to specify that a field should be populated with the result of a function call, specify the function call as the input column (e.g. `field1 = now()`). This is only relevant for unload operations. Function calls can also be qualified by a keyspace name: `field1 = ks1.max(c1,c2)`.

In addition, for mapped data sources, it is also possible to specify that the mapping be partly auto-generated and partly explicitly specified. For example, if a source row has fields `c1`, `c2`, `c3`, and `c5`, and the table has columns `c1`, `c2`, `c3`, `c4`, one can map all like-named columns and specify that `c5` in the source maps to `c4` in the table as follows: `* = *, c5 = c4`.

One can specify that all like-named fields be mapped, except for `c2`: `* = -c2`. To skip `c2` and `c3`: `* = [-c2, -c3]`.

Any identifier, field or column, that is not strictly alphanumeric (i.e. not matching [a-zA-Z0-9_]+) must be surrounded by double-quotes, just like you would do in CQL: `"Field ""A""" = "Column 2"` (to escape a double-quote, simply double it). Note that, contrary to the CQL grammar, unquoted identifiers will not be lower-cased: an identifier such as `MyColumn1` will match a column named `"MyColumn1"` and not `mycolumn1`.

The exact type of mapping to use depends on the connector being used. Some connectors can only produce indexed records; others can only produce mapped ones, while others are capable of producing both indexed and mapped records at the same time. Refer to the connector's documentation to know which kinds of mapping it supports.

Default: **null**.

#### -dryRun,--engine.dryRun _&lt;boolean&gt;_

Enable or disable dry-run mode, a test mode that runs the command but does not load data. Not applicable for unloading nor counting.

Default: **false**.

#### -h,--driver.hosts _&lt;list&lt;string&gt;&gt;_

The contact points to use for the initial connection to the cluster. This must be a comma-separated list of hosts, each specified by a host-name or ip address. If the host is a DNS name that resolves to multiple A-records, all the corresponding addresses will be used. Do not use `localhost` as a host-name (since it resolves to both IPv4 and IPv6 addresses on some platforms). The port for all hosts must be specified with `driver.port`.

Default: **["127.0.0.1"]**.

#### -port,--driver.port _&lt;number&gt;_

The native transport port to connect to. This must match DSE's [native_transport_port](https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html#configCassandra_yaml_r__native_transport_port) configuration option.

Note that all nodes in a cluster must accept connections on the same port number. Mixed-port clusters are not supported.

Default: **9042**.

#### -u,--driver.auth.username _&lt;string&gt;_

The username to use. Providers that accept this setting:

 - `PlainTextAuthProvider`
 - `DsePlainTextAuthProvider`

Default: **null**.

#### -p,--driver.auth.password _&lt;string&gt;_

The password to use. Providers that accept this setting:

 - `PlainTextAuthProvider`
 - `DsePlainTextAuthProvider`

Default: **null**.

#### -cl,--driver.query.consistency _&lt;string&gt;_

The consistency level to use for all queries. Note that stronger consistency levels usually result in reduced throughput. In addition, any level higher than `ONE` will automatically disable continuous paging, which can dramatically reduce read throughput.

Valid values are: `ANY`, `LOCAL_ONE`, `ONE`, `TWO`, `THREE`, `LOCAL_QUORUM`, `QUORUM`, `EACH_QUORUM`, `ALL`.

Default: **"LOCAL_ONE"**.

#### --executor.maxPerSecond _&lt;number&gt;_

The maximum number of concurrent operations per second. When loading, this means the maximum number of write requests per second; when unloading or counting, this means the maximum number of rows per second. This acts as a safeguard to prevent overloading the cluster. Batch statements are counted by the number of statements included. Reduce this setting when the latencies get too high and a remote cluster cannot keep up with throughput, as `dsbulk` requests will eventually time out. Setting this option to any negative value or zero will disable it.

Default: **-1**.

#### -maxErrors,--log.maxErrors _&lt;number&gt;_

The maximum number of errors to tolerate before aborting the entire operation. This can be expressed either as an absolute number of errors – in which case, set this to an integer greater than or equal to zero; or as a percentage of total rows processed so far – in which case, set this to a string of the form `N%`, where `N` is a decimal number between 0 and 100 exclusive (e.g. "20%"). Setting this value to any negative integer disables this feature (not recommended).

Default: **100**.

#### -logDir,--log.directory _&lt;string&gt;_

The writable directory where all log files will be stored; if the directory specified does not exist, it will be created. URLs are not acceptable (not even `file:/` URLs). Log files for a specific run, or execution, will be located in a sub-directory under the specified directory. Each execution generates a sub-directory identified by an "execution ID". See `engine.executionId` for more information about execution IDs. Relative paths will be resolved against the current working directory. Also, for convenience, if the path begins with a tilde (`~`), that symbol will be expanded to the current user's home directory.

Default: **"./logs"**.

#### -verbosity,--log.verbosity _&lt;number&gt;_

The desired level of verbosity. Valid values are:

- 0 (quiet): DSBulk will only log WARN and ERROR messages.
- 1 (normal): DSBulk will log INFO, WARN and ERROR messages.
- 2 (verbose) DSBulk will log DEBUG, INFO, WARN and ERROR messages.

Default: **1**.

#### -reportRate,--monitoring.reportRate _&lt;string&gt;_

The report interval. DSBulk will print useful metrics about the ongoing operation at this rate. Durations lesser than one second will be rounded up to 1 second.

Default: **"5 seconds"**.

<a name="connector"></a>
## Connector Settings

Connector-specific settings. This section contains settings for the connector to use; it also contains sub-sections, one for each available connector.

This setting is ignored when counting.

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

#### --connector.csv.emptyValue _&lt;string&gt;_

Sets the String representation of an empty value. When reading, if the parser does not read any character from the input, and the input is within quotes, this value will be used instead. This setting is ignored when writing. The default is `""` (empty string).

Default: **&lt;unspecified&gt;**.

#### -encoding,--connector.csv.encoding _&lt;string&gt;_

The file encoding to use for all read or written files.

Default: **"UTF-8"**.

#### -escape,--connector.csv.escape _&lt;string&gt;_

The character used for escaping quotes inside an already quoted value. Only one character can be specified. Note that this setting applies to all files to be read or written.

Default: **"\\"**.

#### --connector.csv.fileNameFormat _&lt;string&gt;_

The file name format to use when writing. This setting is ignored when reading and for non-file URLs. The file name must comply with the formatting rules of `String.format()`, and must contain a `%d` format specifier that will be used to increment file name counters.

Default: **"output-%06d.csv"**.

#### --connector.csv.fileNamePattern _&lt;string&gt;_

The glob pattern to use when searching for files to read. The syntax to use is the glob syntax, as described in `java.nio.file.FileSystem.getPathMatcher()`. This setting is ignored when writing and for non-file URLs. Only applicable when the *url* setting points to a directory on a known filesystem, ignored otherwise.

Default: **"\*\*/\*.csv"**.

#### --connector.csv.flushWindow _&lt;number&gt;_

The flush window size, that is, the number of records to flush to disk or network in a single pass. Only applicable when unloading. The ideal buffer size depends on the size of the rows being unloaded: larger buffer sizes may have a positive impact on throughput for small rows, and vice versa.

Default: **32**.

#### --connector.csv.ignoreLeadingWhitespaces _&lt;boolean&gt;_

Defines whether or not leading whitespaces from values being read/written should be skipped. This setting is honored when reading and writing. Default value is false.

Default: **false**.

#### --connector.csv.ignoreLeadingWhitespacesInQuotes _&lt;boolean&gt;_

Defines whether or not trailing whitespaces from quoted values should be skipped. This setting is only honored when reading; it is ignored when writing. Default value is false.

Default: **false**.

#### --connector.csv.ignoreTrailingWhitespaces _&lt;boolean&gt;_

Defines whether or not trailing whitespaces from values being read/written should be skipped. This setting is honored when reading and writing. Default value is false.

Default: **false**.

#### --connector.csv.ignoreTrailingWhitespacesInQuotes _&lt;boolean&gt;_

Defines whether or not leading whitespaces from quoted values should be skipped. This setting is only honored when reading; it is ignored when writing. Default value is false.

Default: **false**.

#### --connector.csv.maxCharsPerColumn _&lt;number&gt;_

The maximum number of characters that a field can contain. This setting is used to size internal buffers and to avoid out-of-memory problems. If set to -1, internal buffers will be resized dynamically. While convenient, this can lead to memory problems. It could also hurt throughput, if some large fields require constant resizing; if this is the case, set this value to a fixed positive number that is big enough to contain all field values.

Default: **4096**.

#### --connector.csv.maxColumns _&lt;number&gt;_

The maximum number of columns that a record can contain. This setting is used to size internal buffers and to avoid out-of-memory problems.

Default: **512**.

#### -maxConcurrentFiles,--connector.csv.maxConcurrentFiles _&lt;string&gt;_

The maximum number of files that can be written simultaneously. This setting is ignored when reading and when the output URL is anything other than a directory on a filesystem. The special syntax `NC` can be used to specify a number of threads that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 threads.

Default: **"0.25C"**.

#### -newline,--connector.csv.newline _&lt;string&gt;_

The character(s) that represent a line ending. When set to the special value `auto` (default), the system's line separator, as determined by `System.lineSeparator()`, will be used when writing, and auto-detection of line endings will be enabled when reading. Only one or two characters can be specified; beware that most typical line separator characters need to be escaped, e.g. one should specify `\r\n` for the typical line ending on Windows systems (carriage return followed by a new line).

Default: **"auto"**.

#### --connector.csv.normalizeLineEndingsInQuotes _&lt;boolean&gt;_

Defines whether or not line separators should be replaced by a normalized line separator '\n' inside quoted values. This setting is honored when reading and writing. Note: due to a bug in the CSV parsing library, on Windows systems, the line ending detection mechanism may not function properly when this setting is false; in case of problem, set this to true. Default value is false.

Default: **false**.

#### --connector.csv.nullValue _&lt;string&gt;_

Sets the String representation of a null value. When reading, if the parser does not read any character from the input, this value will be used instead. When writing, if the writer has a null object to write to the output, this value will be used instead. The default value is `null`, which means that, when reading, the parser will emit a `null`, and when writing, the writer won't write any character at all to the output.

Default: **null**.

#### --connector.csv.recursive _&lt;boolean&gt;_

Enable or disable scanning for files in the root's subdirectories. Only applicable when *url* is set to a directory on a known filesystem. Used for loading only.

Default: **false**.

#### --connector.csv.urlfile _&lt;string&gt;_

The URL or path of the file that contains the list of resources to read from.

The file specified here should be located on the local filesystem.

This setting and `connector.csv.url` are mutually exclusive. If both are defined and non empty, this setting takes precedence over `connector.csv.url`.

This setting applies only when loading. When unloading, this setting should be left empty or set to null; any non-empty value will trigger a fatal error.

The file with URLs should follow this format:

```
/path/to/file/file.csv
/path/to.dir/
```

Every line should contain one path. You don't need to escape paths in this file.

All the remarks for `connector.csv.url` apply for each line in the file, and especially, settings like `fileNamePattern`, `recursive`, and `fileNameFormat` all apply to each line individually.

You can comment out a line in the URL file by making it start with a # sign:

```
#/path/that/will/be/ignored
```

Such a line will be ignored.

For your convenience, every line in the urlfile will be trimmed - that is, any leading and trailing white space will be removed.

The file should be encoded in UTF-8, and each line should be a valid URL to load.

If there is an error opening or reading one of the provided URLs, it will be considered recoverable and processing will continue for other URLs. See log.maxErrors setting for more information.

The default value is "" - which means that this property is ignored.

Default: **&lt;unspecified&gt;**.

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

If there is an error opening or reading one of the provided URLs, it will be considered recoverable and processing will continue for other URLs. See log.maxErrors setting for more information.

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

#### --connector.json.deserializationFeatures _&lt;map&lt;string,boolean&gt;&gt;_

A map of JSON deserialization features to set. Map keys should be enum constants defined in `com.fasterxml.jackson.databind.DeserializationFeature`. The default value is the only way to guarantee that floating point numbers will not have their precision truncated when parsed, but can result in slightly slower parsing. Used for loading only.

Note that some Jackson features might not be supported, in particular features that operate on the resulting Json tree by filtering elements or altering their contents, since such features conflict with dsbulk's own filtering and formatting capabilities. Instead of trying to modify the resulting tree using Jackson features, you should try to achieve the same result using the settings available under the `codec` and `schema` sections.

#### -encoding,--connector.json.encoding _&lt;string&gt;_

The file encoding to use for all read or written files.

Default: **"UTF-8"**.

#### --connector.json.fileNameFormat _&lt;string&gt;_

The file name format to use when writing. This setting is ignored when reading and for non-file URLs. The file name must comply with the formatting rules of `String.format()`, and must contain a `%d` format specifier that will be used to increment file name counters.

Default: **"output-%06d.json"**.

#### --connector.json.fileNamePattern _&lt;string&gt;_

The glob pattern to use when searching for files to read. The syntax to use is the glob syntax, as described in `java.nio.file.FileSystem.getPathMatcher()`. This setting is ignored when writing and for non-file URLs. Only applicable when the *url* setting points to a directory on a known filesystem, ignored otherwise.

Default: **"\*\*/\*.json"**.

#### --connector.json.flushWindow _&lt;number&gt;_

The flush window size, that is, the number of records to flush to disk or network in a single pass. Only applicable when unloading. The ideal buffer size depends on the size of the rows being unloaded: larger buffer sizes may have a positive impact on throughput for small rows, and vice versa.

Default: **32**.

#### --connector.json.generatorFeatures _&lt;map&lt;string,boolean&gt;&gt;_

JSON generator features to enable. Valid values are all the enum constants defined in `com.fasterxml.jackson.core.JsonGenerator.Feature`. For example, a value of `{ ESCAPE_NON_ASCII : true, QUOTE_FIELD_NAMES : true }` will configure the generator to escape all characters beyond 7-bit ASCII and quote field names when writing JSON output. Used for unloading only.

Note that some Jackson features might not be supported, in particular features that operate on the resulting Json tree by filtering elements or altering their contents, since such features conflict with dsbulk's own filtering and formatting capabilities. Instead of trying to modify the resulting tree using Jackson features, you should try to achieve the same result using the settings available under the `codec` and `schema` sections.

#### -maxConcurrentFiles,--connector.json.maxConcurrentFiles _&lt;string&gt;_

The maximum number of files that can be written simultaneously. This setting is ignored when reading and when the output URL is anything other than a directory on a filesystem. The special syntax `NC` can be used to specify a number of threads that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 threads.

Default: **"0.25C"**.

#### --connector.json.parserFeatures _&lt;map&lt;string,boolean&gt;&gt;_

JSON parser features to enable. Valid values are all the enum constants defined in `com.fasterxml.jackson.core.JsonParser.Feature`. For example, a value of `{ ALLOW_COMMENTS : true, ALLOW_SINGLE_QUOTES : true }` will configure the parser to allow the use of comments and single-quoted strings in JSON data. Used for loading only.

Note that some Jackson features might not be supported, in particular features that operate on the resulting Json tree by filtering elements or altering their contents, since such features conflict with dsbulk's own filtering and formatting capabilities. Instead of trying to modify the resulting tree using Jackson features, you should try to achieve the same result using the settings available under the `codec` and `schema` sections.

#### --connector.json.prettyPrint _&lt;boolean&gt;_

Enable or disable pretty printing. When enabled, JSON records are written with indents. Used for unloading only.

Note: Can result in much bigger records.

Default: **false**.

#### --connector.json.recursive _&lt;boolean&gt;_

Enable or disable scanning for files in the root's subdirectories. Only applicable when *url* is set to a directory on a known filesystem. Used for loading only.

Default: **false**.

#### --connector.json.serializationFeatures _&lt;map&lt;string,boolean&gt;&gt;_

A map of JSON serialization features to set. Map keys should be enum constants defined in `com.fasterxml.jackson.databind.SerializationFeature`. Used for unloading only.

Note that some Jackson features might not be supported, in particular features that operate on the resulting Json tree by filtering elements or altering their contents, since such features conflict with dsbulk's own filtering and formatting capabilities. Instead of trying to modify the resulting tree using Jackson features, you should try to achieve the same result using the settings available under the `codec` and `schema` sections.

#### --connector.json.serializationStrategy _&lt;string&gt;_

The strategy to use for filtering out entries when formatting output. Valid values are enum constants defined in `com.fasterxml.jackson.annotation.JsonInclude.Include` (but beware that the `CUSTOM` strategy cannot be honored). Used for unloading only.

Default: **"ALWAYS"**.

#### --connector.json.urlfile _&lt;string&gt;_

The URL or path of the file that contains the list of resources to read from.

The file specified here should be located on the local filesystem.

This setting and `connector.json.url` are mutually exclusive. If both are defined and non empty, this setting takes precedence over `connector.json.url`.

This setting applies only when loading. When unloading, this setting should be left empty or set to null; any non-empty value will trigger a fatal error.

The file with URLs should follow this format:

```
/path/to/file/file.json
/path/to.dir/
```

Every line should contain one path. You don't need to escape paths in this file.

All the remarks for `connector.csv.json` apply for each line in the file, and especially, settings like `fileNamePattern`, `recursive`, and `fileNameFormat` all apply to each line individually.

You can comment out a line in the URL file by making it start with a # sign:

```
#/path/that/will/be/ignored
```

Such a line will be ignored.

For your convenience, every line in the urlfile will be trimmed - that is, any leading and trailing white space will be removed.

The file should be encoded in UTF-8, and each line should be a valid URL to load.

The default value is "" - which means that this property is ignored.

Default: **&lt;unspecified&gt;**.

<a name="schema"></a>
## Schema Settings

Schema-specific settings.

#### -k,--schema.keyspace _&lt;string&gt;_

Keyspace used for loading or unloading data. Keyspace names should not be quoted and are case-sensitive. `MyKeyspace` will match a keyspace named `MyKeyspace` but not `mykeyspace`. Required option if `schema.query` is not specified; otherwise, optional.

Default: **null**.

#### -t,--schema.table _&lt;string&gt;_

Table used for loading or unloading data. Table names should not be quoted and are case-sensitive. `MyTable` will match a table named `MyTable` but not `mytable`. Required option if `schema.query` is not specified; otherwise, optional.

Default: **null**.

#### -m,--schema.mapping _&lt;string&gt;_

The field-to-column mapping to use, that applies to both loading and unloading; ignored when counting. If not specified, the loader will apply a strict one-to-one mapping between the source fields and the database table. If that is not what you want, then you must supply an explicit mapping. Mappings should be specified as a map of the following form:

- Indexed data sources: `0 = col1, 1 = col2, 2 = col3`, where `0`, `1`, `2`, are the zero-based indices of fields in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map the first `n` fields is to simply specify the destination columns: `col1, col2, col3`.
- Mapped data sources: `fieldA = col1, fieldB = col2, fieldC = col3`, where `fieldA`, `fieldB`, `fieldC`, are field names in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map fields named like columns is to simply specify the destination columns: `col1, col2, col3`.

To specify that a field should be used as the timestamp (a.k.a. write-time) or ttl (a.k.a. time-to-live) of the inserted row, use the specially named fake columns `__ttl` and `__timestamp`: `fieldA = __timestamp, fieldB = __ttl`. Note that Timestamp fields are parsed as regular CQL timestamp columns and must comply with either `codec.timestamp`, or alternatively, with `codec.unit` + `codec.epoch`. TTL fields are parsed as integers representing durations in seconds, and must comply with `codec.number`.

To specify that a column should be populated with the result of a function call, specify the function call as the input field (e.g. `now() = c4`). Note, this is only relevant for load operations. Similarly, to specify that a field should be populated with the result of a function call, specify the function call as the input column (e.g. `field1 = now()`). This is only relevant for unload operations. Function calls can also be qualified by a keyspace name: `field1 = ks1.max(c1,c2)`.

In addition, for mapped data sources, it is also possible to specify that the mapping be partly auto-generated and partly explicitly specified. For example, if a source row has fields `c1`, `c2`, `c3`, and `c5`, and the table has columns `c1`, `c2`, `c3`, `c4`, one can map all like-named columns and specify that `c5` in the source maps to `c4` in the table as follows: `* = *, c5 = c4`.

One can specify that all like-named fields be mapped, except for `c2`: `* = -c2`. To skip `c2` and `c3`: `* = [-c2, -c3]`.

Any identifier, field or column, that is not strictly alphanumeric (i.e. not matching [a-zA-Z0-9_]+) must be surrounded by double-quotes, just like you would do in CQL: `"Field ""A""" = "Column 2"` (to escape a double-quote, simply double it). Note that, contrary to the CQL grammar, unquoted identifiers will not be lower-cased: an identifier such as `MyColumn1` will match a column named `"MyColumn1"` and not `mycolumn1`.

The exact type of mapping to use depends on the connector being used. Some connectors can only produce indexed records; others can only produce mapped ones, while others are capable of producing both indexed and mapped records at the same time. Refer to the connector's documentation to know which kinds of mapping it supports.

Default: **null**.

#### --schema.allowExtraFields _&lt;boolean&gt;_

Specify whether or not to accept records that contain extra fields that are not declared in the mapping. For example, if a record contains three fields A, B, and C, but the mapping only declares fields A and B, then if this option is true, C will be silently ignored and the record will be considered valid, and if false, the record will be rejected. This setting also applies to user-defined types and tuples. Only applicable for loading, ignored otherwise.

This setting is ignored when counting.

Default: **true**.

#### --schema.allowMissingFields _&lt;boolean&gt;_

Specify whether or not to accept records that are missing fields declared in the mapping. For example, if the mapping declares three fields A, B, and C, but a record contains only fields A and B, then if this option is true, C will be silently assigned null and the record will be considered valid, and if false, the record will be rejected. If the missing field is mapped to a primary key column, the record will always be rejected, since the database will reject the record. This setting also applies to user-defined types and tuples. Only applicable for loading, ignored otherwise.

This setting is ignored when counting.

Default: **false**.

#### --schema.nullToUnset _&lt;boolean&gt;_

Specify whether to map `null` input values to "unset" in the database, i.e., don't modify a potentially pre-existing value of this field for this row. Valid for load scenarios, otherwise ignore. Note that setting to false creates tombstones to represent `null`.

Note that this setting is applied after the *codec.nullStrings* setting, and may intercept `null`s produced by that setting.

This setting is ignored when counting. When set to true but the protocol version in use does not support unset values (i.e., all protocol versions lesser than 4), this setting will be forced to false and a warning will be logged.

Default: **true**.

#### -query,--schema.query _&lt;string&gt;_

The query to use. If not specified, then *schema.keyspace* and *schema.table* must be specified, and dsbulk will infer the appropriate statement based on the table's metadata, using all available columns. If `schema.keyspace` is provided, the query need not include the keyspace to qualify the table reference.

For loading, the statement can be any `INSERT`, `UPDATE` or `DELETE` statement. `INSERT` statements are preferred for most load operations, and bound variables should correspond to mapped fields; for example, `INSERT INTO table1 (c1, c2, c3) VALUES (:fieldA, :fieldB, :fieldC)`. `UPDATE` statements are required if the target table is a counter table, and the columns are updated with incremental operations (`SET col1 = col1 + :fieldA` where `fieldA` is a field in the input data). A `DELETE` statement will remove existing data during the load operation.

For unloading and counting, the statement can be any regular `SELECT` statement. If the statement does not contain a WHERE clause, the engine will generate a token range restriction clause of the form: `WHERE token(...) > :start and token(...) <= :end` and will generate as many statements as there are token ranges in the cluster, thus allowing parallelization of reads while at the same time targeting coordinators that are also replicas. If the statement does contain a WHERE clause however, that clause will remain intact; the engine will only be able to parallelize the operation if that WHERE clause also includes a `token(...) > :start and token(...) <= :end` relation (the bound variables can have any name).

Statements can use both named and positional bound variables. Named bound variables should be preferred, unless the protocol version in use does not allow them; they usually have names matching those of the columns in the destination table, but this is not a strict requirement; it is, however, required that their names match those of fields specified in the mapping. Positional variables can also be used, and will be named after their corresponding column in the destination table.

Note: The query is parsed to discover which bound variables are present, and to map the variables correctly to fields.

See *schema.mapping* setting for more information.

Default: **null**.

#### --schema.queryTimestamp _&lt;string&gt;_

The timestamp of inserted/updated cells during load; otherwise, the current time of the system running the tool is used. Not applicable to unloading nor counting. Ignored when `schema.query` is provided. The value must be expressed in [`ISO_ZONED_DATE_TIME`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_ZONED_DATE_TIME) format.

Query timestamps for DSE have microsecond resolution; any sub-microsecond information specified is lost. For more information, see the [CQL Reference](https://docs.datastax.com/en/dse/6.0/cql/cql/cql_reference/cql_commands/cqlInsert.html#cqlInsert__timestamp-value).

Default: **null**.

#### --schema.queryTtl _&lt;number&gt;_

The Time-To-Live (TTL) of inserted/updated cells during load (seconds); a value of -1 means there is no TTL. Not applicable to unloading nor counting. Ignored when `schema.query` is provided. For more information, see the [CQL Reference](https://docs.datastax.com/en/dse/6.0/cql/cql/cql_reference/cql_commands/cqlInsert.html#cqlInsert__ime-value), [Setting the time-to-live (TTL) for value](http://docs.datastax.com/en/dse/6.0/cql/cql/cql_using/useTTL.html), and [Expiring data with time-to-live](http://docs.datastax.com/en/dse/6.0/cql/cql/cql_using/useExpire.html).

Default: **-1**.

#### --schema.splits _&lt;string&gt;_

The number of token range splits in which to divide the token ring. In other words, this setting determines how many read requests will be generated in order to read an entire table. Only used when unloading and counting; ignored otherwise. Note that the actual number of splits may be slightly greater or lesser than the number specified here, depending on the actual cluster topology and token ownership. Also, it is not possible to generate fewer splits than the total number of primary token ranges in the cluster, so the actual number of splits is always equal to or greater than that number. Set this to higher values if you experience timeouts when reading from DSE, specially if paging is disabled. The special syntax `NC` can be used to specify a number that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 splits.

Default: **"8C"**.

<a name="batch"></a>
## Batch Settings

Batch-specific settings.

These settings control how the workflow engine groups together statements before writing them.

Only applicable for loading.

#### --batch.bufferSize _&lt;number&gt;_

The buffer size to use for flushing batched statements. Should be set to a multiple of `maxBatchStatements`, e.g. 2 or 4 times that value; higher values consume more memory and usually do not incur in any noticeable performance gain. When set to a value lesser than or equal to zero, the buffer size is implicitly set to 4 times `maxBatchStatments`.

Default: **-1**.

#### --batch.maxBatchSize _&lt;arg&gt;_

Deprecated - use `maxBatchStatements` instead.

Default: **null**.

#### --batch.maxBatchStatements _&lt;number&gt;_

The maximum number of statements that a batch can contain. The ideal value depends on two factors:
- The data being loaded: the larger the data, the smaller the batches should be.
- The batch mode: when `PARTITION_KEY` is used, larger batches are acceptable, whereas when `REPLICA_SET` is used, smaller batches usually perform better. Also, when using `REPLICA_SET`, it is preferrable to keep this number below the threshold configured server-side for the setting `unlogged_batch_across_partitions_warn_threshold` (the default is 10); failing to do so is likely to trigger query warnings (see `log.maxQueryWarnings` for more information).
When set to a value lesser than or equal to zero, the maximum number of statements is considered unlimited. At least one of `maxBatchStatements` or `maxSizeInBytes` must be set to a positive value when batching is enabled.

Default: **32**.

#### --batch.maxSizeInBytes _&lt;number&gt;_

The maximum data size that a batch can hold. This is the number of bytes required to encode all the data to be persisted, without counting the overhead generated by the native protocol (headers, frames, etc.). The value specified here should be lesser than or equal to the value that has been configured server-side for the option `batch_size_fail_threshold_in_kb` in cassandra.yaml, but note that the heuristic used to compute data sizes is not 100% accurate and sometimes underestimates the actual size. See the documentation for the [https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/config/configCassandra_yaml.html#configCassandra_yaml__advProps](cassandra.yaml configuration file) for more information. When set to a value lesser than or equal to zero, the maximum data size is considered unlimited. At least one of `maxBatchStatements` or `maxSizeInBytes` must be set to a positive value when batching is enabled.

Default: **-1**.

#### --batch.mode _&lt;string&gt;_

The grouping mode. Valid values are:
- `DISABLED`: batching is disabled.
- `PARTITION_KEY`: groups together statements that share the same partition key. This is usually the most performant mode; however it may not work at all if the dataset is unordered, i.e., if partition keys appear randomly and cannot be grouped together.
- `REPLICA_SET`: groups together statements that share the same replica set. This mode works in all cases, but may incur in some throughput and latency degradation, specially with large clusters or high replication factors.
When tuning DSBulk for batching, the recommended approach is as follows:
1. Start with `PARTITION_KEY`;
2. If the average batch size is close to 1, try increasing `bufferSize`;
3. If increasing `bufferSize` doesn't help, switch to `REPLICA_SET` and set `maxBatchStatements` or `maxSizeInBytes` to low values to avoid timeouts or errors;
4. Increase `maxBatchStatements` or `maxSizeInBytes` to get the best throughput while keeping latencies acceptable.
The default is `PARTITION_KEY`.

Default: **"PARTITION_KEY"**.

<a name="codec"></a>
## Codec Settings

Conversion-specific settings. These settings apply for both load and unload workflows.

When writing, these settings determine how record fields emitted by connectors are parsed.

When unloading, these settings determine how row cells emitted by DSE are formatted.

When counting, these settings are ignored.

#### --codec.booleanNumbers _&lt;list&lt;number&gt;&gt;_

Set how true and false representations of numbers are interpreted. The representation is of the form `true_value,false_value`. The mapping is reciprocal, so that numbers are mapping to Boolean and vice versa. All numbers unspecified in this setting are rejected.

Default: **[1,0]**.

#### --codec.booleanStrings _&lt;list&lt;string&gt;&gt;_

Specify how true and false representations can be used by dsbulk. Each representation is of the form `true_value:false_value`, case-insensitive. For loading, all representations are honored: when a record field value exactly matches one of the specified strings, the value is replaced with `true` of `false` before writing to DSE. For unloading, this setting is only applicable for string-based connectors, such as the CSV connector: the first representation will be used to format booleans before they are written out, and all others are ignored.

Default: **["1:0","Y:N","T:F","YES:NO","TRUE:FALSE"]**.

#### --codec.date _&lt;string&gt;_

The temporal pattern to use for `String` to CQL `date` conversion. Valid choices:

- A date-time pattern such as `yyyy-MM-dd`.
- A pre-defined formatter such as `ISO_LOCAL_DATE`. Any public static field in `java.time.format.DateTimeFormatter` can be used.
- The special formatter `UNITS_SINCE_EPOCH`, which is a special parser that reads and writes local dates as numbers representing time units since a given epoch; the unit and the epoch to use can be specified with `codec.unit` and `codec.timestamp`.

For more information on patterns and pre-defined formatters, see [Patterns for Formatting and Parsing](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns) in Oracle Java documentation.

For more information about CQL date, time and timestamp literals, see [Date, time, and timestamp format](https://docs.datastax.com/en/dse/6.0/cql/cql/cql_reference/refDateTimeFormats.html?hl=timestamp).

Default: **"ISO_LOCAL_DATE"**.

#### --codec.epoch _&lt;string&gt;_

This setting is used in the following situations:

- When the target column is of CQL `timestamp` type, or when loading to a `USING TIMESTAMP` clause, or when unloading from a `writetime()` function call, and if `codec.timestamp` is set to `UNITS_SINCE_EPOCH`, then the epoch specified here determines the relative point in time to use to convert numeric data to and from temporals. For example, if the input is 123 and the epoch specified here is `2000-01-01T00:00:00Z`, then the input will be interpreted as N `codec.unit`s since January 1st 2000.
- When loading, and the target CQL type is numeric, but the input is alphanumeric and represents a temporal literal, the time unit specified here will be used to convert the parsed temporal into a numeric value. For example, if the input is `2018-12-10T19:32:45Z` and the epoch specified here is `2000-01-01T00:00:00Z`, then the parsed timestamp will be converted to N `codec.unit`s since January 1st 2000.
- When parsing temporal literals, if the input does not contain a date part, then the date part of the instant specified here will be used instead. For example, if the input is `19:32:45` and the epoch specified here is `2000-01-01T00:00:00Z`, then the input will be interpreted `2000-01-01T19:32:45Z`.

The value must be expressed in [`ISO_ZONED_DATE_TIME`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_ZONED_DATE_TIME) format.

Default: **"1970-01-01T00:00:00Z"**.

#### --codec.formatNumbers _&lt;boolean&gt;_

Whether or not to use the `codec.number` pattern to format numeric output. When set to `true`, the numeric pattern defined by `codec.number` will be applied. This allows for nicely-formatted output, but may result in rounding (see `codec.roundingStrategy`), or alteration of the original decimal's scale. When set to `false`, numbers will be stringified using the `toString()` method, and will never result in rounding or scale alteration. Only applicable when unloading, and only if the connector in use requires stringification, because the connector, such as the CSV connector, does not handle raw numeric data; ignored otherwise.

Default: **false**.

#### -locale,--codec.locale _&lt;string&gt;_

The locale to use for locale-sensitive conversions.

Default: **"en_US"**.

#### -nullStrings,--codec.nullStrings _&lt;list&gt;_

Comma-separated list of case-sensitive strings that should be mapped to `null`. For loading, when a record field value exactly matches one of the specified strings, the value is replaced with `null` before writing to DSE. For unloading, this setting is only applicable for string-based connectors, such as the CSV connector: the first string specified will be used to change a row cell containing `null` to the specified string when written out.

For example, setting this to `["NULL"]` will cause a field containing the word `NULL` to be mapped to `null` while loading, and a column containing `null` to be converted to the word `NULL` while unloading.

The default value is `[]` (no strings are mapped to `null`). In the default mode, DSBulk behaves as follows:
* When loading, if the target CQL type is textual (i.e. text, varchar or ascii), the original field value is left untouched; for other types, if the value is an empty string, it is converted to `null`.
* When unloading, all `null` values are converted to an empty string.

Note that, regardless of this setting, DSBulk will always convert empty strings to `null` if the target CQL type is not textual (i.e. not text, varchar or ascii).

This setting is applied before `schema.nullToUnset`, hence any `null` produced by a null-string can still be left unset if required.

Default: **[]**.

#### --codec.number _&lt;string&gt;_

The `DecimalFormat` pattern to use for conversions between `String` and CQL numeric types.

See [java.text.DecimalFormat](https://docs.oracle.com/javase/8/docs/api/java/text/DecimalFormat.html) for details about the pattern syntax to use.

Most inputs are recognized: optional localized thousands separator, localized decimal separator, or optional exponent. Using locale `en_US`, `1234`, `1,234`, `1234.5678`, `1,234.5678` and `1,234.5678E2` are all valid. For unloading and formatting, rounding may occur and cause precision loss. See `codec.formatNumbers` and `codec.roundingStrategy`.

Default: **"#,###.##"**.

#### --codec.overflowStrategy _&lt;string&gt;_

This setting can mean one of three possibilities:

- The value is outside the range of the target CQL type. For example, trying to convert 128 to a CQL `tinyint` (max value of 127) results in overflow.
- The value is decimal, but the target CQL type is integral. For example, trying to convert 123.45 to a CQL `int` results in overflow.
- The value's precision is too large for the target CQL type. For example, trying to insert 0.1234567890123456789 into a CQL `double` results in overflow, because there are too many significant digits to fit in a 64-bit double.

Valid choices:

- `REJECT`: overflows are considered errors and the data is rejected. This is the default value.
- `TRUNCATE`: the data is truncated to fit in the target CQL type. The truncation algorithm is similar to the narrowing primitive conversion defined in The Java Language Specification, Section 5.1.3, with the following exceptions:
    - If the value is too big or too small, it is rounded up or down to the maximum or minimum value allowed, rather than truncated at bit level. For example, 128 would be rounded down to 127 to fit in a byte, whereas Java would have truncated the exceeding bits and converted to -127 instead.
    - If the value is decimal, but the target CQL type is integral, it is first rounded to an integral using the defined rounding strategy, then narrowed to fit into the target type. This can result in precision loss and should be used with caution.

Only applicable for loading, when parsing numeric inputs; it does not apply for unloading, since formatting never results in overflow.

Default: **"REJECT"**.

#### --codec.roundingStrategy _&lt;string&gt;_

The rounding strategy to use for conversions from CQL numeric types to `String`.

Valid choices: any `java.math.RoundingMode` enum constant name, including: `CEILING`, `FLOOR`, `UP`, `DOWN`, `HALF_UP`, `HALF_EVEN`, `HALF_DOWN`, and `UNNECESSARY`. The precision used when rounding is inferred from the numeric pattern declared under `codec.number`. For example, the default `codec.number` (`#,###.##`) has a rounding precision of 2, and the number 123.456 would be rounded to 123.46 if `roundingStrategy` was set to `UP`. The default value will result in infinite precision, and ignore the `codec.number` setting.

Only applicable when unloading, if `codec.formatNumbers` is true and if the connector in use requires stringification, because the connector, such as the CSV connector, does not handle raw numeric data; ignored otherwise.

Default: **"UNNECESSARY"**.

#### --codec.time _&lt;string&gt;_

The temporal pattern to use for `String` to CQL `time` conversion. Valid choices:

- A date-time pattern, such as `HH:mm:ss`.
- A pre-defined formatter, such as `ISO_LOCAL_TIME`. Any public static field in `java.time.format.DateTimeFormatter` can be used.
- The special formatter `UNITS_SINCE_EPOCH`, which is a special parser that reads and writes local times as numbers representing time units since a given epoch; the unit and the epoch to use can be specified with `codec.unit` and `codec.timestamp`.

For more information on patterns and pre-defined formatters, see [Patterns for formatting and Parsing](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns) in Oracle Java documentation.

For more information about CQL date, time and timestamp literals, see [Date, time, and timestamp format](https://docs.datastax.com/en/dse/6.0/cql/cql/cql_reference/refDateTimeFormats.html?hl=timestamp).

Default: **"ISO_LOCAL_TIME"**.

#### -timeZone,--codec.timeZone _&lt;string&gt;_

The time zone to use for temporal conversions. When loading, the time zone will be used to obtain a timestamp from inputs that do not convey any explicit time zone information. When unloading, the time zone will be used to format all timestamps.

Default: **"UTC"**.

#### --codec.timestamp _&lt;string&gt;_

The temporal pattern to use for `String` to CQL `timestamp` conversion. Valid choices:

- A date-time pattern such as `yyyy-MM-dd HH:mm:ss`.
- A pre-defined formatter such as `ISO_ZONED_DATE_TIME` or `ISO_INSTANT`. Any public static field in `java.time.format.DateTimeFormatter` can be used.
- The special formatter `CQL_TIMESTAMP`, which is a special parser that accepts all valid CQL literal formats for the `timestamp` type.
- The special formatter `UNITS_SINCE_EPOCH`, which is a special parser that reads and writes timestamps as numbers representing time units since a given epoch; the unit and the epoch to use can be specified with `codec.unit` and `codec.timestamp`.

For more information on patterns and pre-defined formatters, see [Patterns for Formatting and Parsing](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns) in Oracle Java documentation.

For more information about CQL date, time and timestamp literals, see [Date, time, and timestamp format](https://docs.datastax.com/en/dse/6.0/cql/cql/cql_reference/refDateTimeFormats.html?hl=timestamp).

The default value is the special `CQL_TIMESTAMP` value. When parsing, this format recognizes all CQL temporal literals; if the input is a local date or date/time, the timestamp is resolved using the time zone specified under `timeZone`. When formatting, this format uses the `ISO_OFFSET_DATE_TIME` pattern, which is compliant with both CQL and ISO-8601.

Default: **"CQL_TIMESTAMP"**.

#### --codec.unit _&lt;string&gt;_

This setting is used in the following situations:

- When the target column is of CQL `timestamp` type, or when loading data through a `USING TIMESTAMP` clause, or when unloading data from a `writetime()` function call, and if `codec.timestamp` is set to `UNITS_SINCE_EPOCH`, then the time unit specified here is used to convert numeric data to and from temporals. For example, if the input is 123 and the time unit specified here is SECONDS, then the input will be interpreted as 123 seconds since `codec.epoch`.
- When loading, and the target CQL type is numeric, but the input is alphanumeric and represents a temporal literal, the time unit specified here will be used to convert the parsed temporal into a numeric value. For example, if the input is `2018-12-10T19:32:45Z` and the time unit specified here is SECONDS, then the parsed temporal will be converted into seconds since `codec.epoch`.

All `TimeUnit` enum constants are valid choices.

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

#### -h,--driver.hosts _&lt;list&lt;string&gt;&gt;_

The contact points to use for the initial connection to the cluster. This must be a comma-separated list of hosts, each specified by a host-name or ip address. If the host is a DNS name that resolves to multiple A-records, all the corresponding addresses will be used. Do not use `localhost` as a host-name (since it resolves to both IPv4 and IPv6 addresses on some platforms). The port for all hosts must be specified with `driver.port`.

Default: **["127.0.0.1"]**.

#### -port,--driver.port _&lt;number&gt;_

The native transport port to connect to. This must match DSE's [native_transport_port](https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html#configCassandra_yaml_r__native_transport_port) configuration option.

Note that all nodes in a cluster must accept connections on the same port number. Mixed-port clusters are not supported.

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

Default: **null**.

#### -p,--driver.auth.password _&lt;string&gt;_

The password to use. Providers that accept this setting:

 - `PlainTextAuthProvider`
 - `DsePlainTextAuthProvider`

Default: **null**.

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

Default: **null**.

#### --driver.auth.keyTab _&lt;string&gt;_

The path of the Kerberos keytab file to use for authentication. If left unspecified, authentication uses a ticket cache. Providers that accept this setting:

 - `DseGSSAPIAuthProvider`

Default: **null**.

#### --driver.auth.principal _&lt;string&gt;_

The Kerberos principal to use. For example, `user@datastax.com`. If left unspecified, the principal is chosen from the first key in the ticket cache or keytab. Providers that accept this setting:

 - `DseGSSAPIAuthProvider`

Default: **null**.

#### --driver.auth.saslService _&lt;string&gt;_

The SASL service name to use. This value should match the username of the Kerberos service principal used by the DSE server. This information is specified in the `dse.yaml` file by the *service_principal* option under the *kerberos_options* section, and may vary from one DSE installation to another - especially if you installed DSE with an automated package installer. Providers that accept this setting:

 - `DseGSSAPIAuthProvider`

Default: **"dse"**.

<a name="driver.policy"></a>
### Driver Policy Settings

Settings for various driver policies.

#### -lbp,--driver.policy.lbp.name _&lt;string&gt;_

The name of the load balancing policy. Supported policies include: `dse`, `dcAwareRoundRobin`, `roundRobin`, `whiteList`, `tokenAware`. Available options for the policies are listed below as appropriate. For more information, refer to the driver documentation for the policy. If not specified, defaults to the driver's default load balancing policy, which is currently the `DseLoadBalancingPolicy` wrapping a `TokenAwarePolicy`, wrapping a `DcAwareRoundRobinPolicy`.

Note: It is critical for a token-aware policy to be used in the chain in order to benefit from batching by partition key.

Default: **null**.

#### --driver.policy.lbp.dcAwareRoundRobin.allowRemoteDCsForLocalConsistencyLevel _&lt;boolean&gt;_

Enable or disable whether to allow remote datacenters to count for local consistency level in round robin awareness.

*DEPRECATED*: This functionality will be removed in the next major release of DSBulk.

Default: **false**.

#### --driver.policy.lbp.dcAwareRoundRobin.localDc _&lt;string&gt;_

The datacenter name (commonly dc1, dc2, etc.) local to the machine on which dsbulk is running, so that requests are sent to nodes in the local datacenter whenever possible.

Default: **null**.

#### --driver.policy.lbp.dcAwareRoundRobin.usedHostsPerRemoteDc _&lt;number&gt;_

The number of hosts per remote datacenter that the round robin policy should consider.

*DEPRECATED*: This functionality will be removed in the next major release of DSBulk.

Default: **0**.

#### --driver.policy.lbp.dse.childPolicy _&lt;string&gt;_

The child policy that the specified `dse` policy wraps.

Default: **"roundRobin"**.

#### --driver.policy.lbp.tokenAware.childPolicy _&lt;string&gt;_

The child policy that the specified `tokenAware` policy wraps.

Default: **"roundRobin"**.

#### --driver.policy.lbp.tokenAware.replicaOrdering _&lt;string&gt;_

Specify how to order replicas.

Valid values are all `TokenAwarePolicy.ReplicaOrdering` enum constants:

- RANDOM: Return replicas in a different, random order for each query plan. This is the default strategy;
for loading, it should be preferred has it can improve performance by distributing writes across replicas.
- TOPOLOGICAL: Order replicas by token ring topology, i.e. always return the "primary" replica first.
- NEUTRAL: Return the replicas in the exact same order in which they appear in the child policy's query plan.

Default: **"RANDOM"**.

#### --driver.policy.lbp.whiteList.childPolicy _&lt;string&gt;_

The child policy that the specified `whiteList` policy wraps.

Default: **"roundRobin"**.

#### --driver.policy.lbp.whiteList.hosts _&lt;list&gt;_

List of hosts to white list. This must be a comma-separated list of hosts, each specified by a host-name or ip address. If the host is a DNS name that resolves to multiple A-records, all the corresponding addresses will be used. Do not use `localhost` as a host-name (since it resolves to both IPv4 and IPv6 addresses on some platforms).

Default: **[]**.

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

Default: **8**.

#### --driver.pooling.local.requests _&lt;number&gt;_

The maximum number of requests (1 to 32768) that can be executed concurrently on a connection. If connecting to legacy clusters using protocol version 1 or 2, any value greater than 128 will be capped at 128 and a warning will be logged.

Default: **32768**.

#### --driver.pooling.remote.connections _&lt;number&gt;_

The number of connections in the pool for remote nodes.

Default: **1**.

#### --driver.pooling.remote.requests _&lt;number&gt;_

The maximum number of requests (1 to 32768) that can be executed concurrently on a connection. If connecting to legacy clusters using protocol version 1 or 2, any value greater than 128 will be capped at 128 and a warning will be logged.

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

The consistency level to use for all queries. Note that stronger consistency levels usually result in reduced throughput. In addition, any level higher than `ONE` will automatically disable continuous paging, which can dramatically reduce read throughput.

Valid values are: `ANY`, `LOCAL_ONE`, `ONE`, `TWO`, `THREE`, `LOCAL_QUORUM`, `QUORUM`, `EACH_QUORUM`, `ALL`.

Default: **"LOCAL_ONE"**.

#### --driver.query.fetchSize _&lt;number&gt;_

The page size, or how many rows will be retrieved simultaneously in a single network round trip. The ideal page size depends on the size of the rows being unloaded: larger page sizes may have a positive impact on throughput for small rows, and vice versa.
This setting will limit the number of results loaded into memory simultaneously during unloading or counting. Setting this value to any negative value will disable paging, i.e., the entire result set will be retrieved in one pass (not recommended). Not applicable for loading. When connecting with a positive page size to legacy clusters with protocol version 1, which does not support paging, paging will be automatically disabled and a warning will be logged. Note that this setting controls paging for regular queries; to customize the page size for continuous queries, use the `executor.continuousPaging.pageSize` setting instead.

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

Default: **null**.

#### --driver.ssl.keystore.path _&lt;string&gt;_

The path of the keystore file. This setting is optional. If left unspecified, no client authentication will be used.

Default: **null**.

#### --driver.ssl.openssl.keyCertChain _&lt;string&gt;_

The path of the certificate chain file. This setting is optional. If left unspecified, no client authentication will be used.

Default: **null**.

#### --driver.ssl.openssl.privateKey _&lt;string&gt;_

The path of the private key file.

Default: **null**.

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

Default: **null**.

#### --driver.ssl.truststore.path _&lt;string&gt;_

The path of the truststore file. This setting is optional. If left unspecified, server certificates will not be validated.

Default: **null**.

<a name="engine"></a>
## Engine Settings

Workflow Engine-specific settings.

#### -dryRun,--engine.dryRun _&lt;boolean&gt;_

Enable or disable dry-run mode, a test mode that runs the command but does not load data. Not applicable for unloading nor counting.

Default: **false**.

#### --engine.executionId _&lt;string&gt;_

A unique identifier to attribute to each execution. When unspecified or empty, the engine will automatically generate identifiers of the following form: *workflow*_*timestamp*, where :

- *workflow* stands for the workflow type (`LOAD`, `UNLOAD`, etc.);
- *timestamp* is the current timestamp formatted as `uuuuMMdd-HHmmss-SSSSSS` (see [https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns)) in UTC, with microsecond precision if available, and millisecond precision otherwise.

When this identifier is user-supplied, it is important to guarantee its uniqueness; failing to do so may result in execution failures. It is also possible to provide templates here. Any format compliant with the formatting rules of [`String.format()`](https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html#syntax) is accepted, and can contain the following parameters:

- `%1$s` : the workflow type (`LOAD`, `UNLOAD`, etc.);
- `%2$t` : the current time (with microsecond precision if available, and millisecond precision otherwise);
- `%3$s` : the JVM process PID (this parameter might not be available on some operating systems; if its value cannot be determined, a random integer will be inserted instead).

Default: **null**.

<a name="executor"></a>
## Executor Settings

Executor-specific settings.

#### --executor.maxPerSecond _&lt;number&gt;_

The maximum number of concurrent operations per second. When loading, this means the maximum number of write requests per second; when unloading or counting, this means the maximum number of rows per second. This acts as a safeguard to prevent overloading the cluster. Batch statements are counted by the number of statements included. Reduce this setting when the latencies get too high and a remote cluster cannot keep up with throughput, as `dsbulk` requests will eventually time out. Setting this option to any negative value or zero will disable it.

Default: **-1**.

#### --executor.continuousPaging.enabled _&lt;boolean&gt;_

Enable or disable continuous paging. If the target cluster does not support continuous paging or if `driver.query.consistency` is not `ONE` or `LOCAL_ONE`, traditional paging will be used regardless of this setting.

Default: **true**.

#### --executor.continuousPaging.maxConcurrentQueries _&lt;number&gt;_

The maximum number of concurrent continuous paging queries that should be carried in parallel. Set this number to a value equal to or lesser than the value configured server-side for `continuous_paging.max_concurrent_sessions` in the cassandra.yaml configuration file (60 by default); otherwise some requests might be rejected. Settting this option to any negative value or zero will disable it.

Default: **60**.

#### --executor.continuousPaging.maxPages _&lt;number&gt;_

The maximum number of pages to retrieve. Setting this value to zero retrieves all pages available.

Default: **0**.

#### --executor.continuousPaging.maxPagesPerSecond _&lt;number&gt;_

The maximum number of pages per second. Setting this value to zero indicates no limit.

Default: **0**.

#### --executor.continuousPaging.pageSize _&lt;number&gt;_

The size of the page. The unit to use is determined by the `pageUnit` setting. The ideal page size depends on the size of the rows being unloaded: larger page sizes may have a positive impact on throughput for small rows, and vice versa.

Default: **5000**.

#### --executor.continuousPaging.pageUnit _&lt;string&gt;_

The unit to use for the `pageSize` setting. Possible values are: `ROWS`, `BYTES`.

Default: **"ROWS"**.

#### --executor.maxInFlight _&lt;number&gt;_

The maximum number of "in-flight" requests, or maximum number of concurrent requests waiting for a response from the server. This acts as a safeguard to prevent more requests than the cluster can handle. Batch statements count as one request. Reduce this value when the throughput for reads and writes cannot match the throughput of mappers; this is usually a sign that the workflow engine is not well calibrated and will eventually run out of memory. Setting this option to any negative value or zero will disable it.

Default: **1024**.

<a name="log"></a>
## Log Settings

Log and error management settings.

#### -maxErrors,--log.maxErrors _&lt;number&gt;_

The maximum number of errors to tolerate before aborting the entire operation. This can be expressed either as an absolute number of errors – in which case, set this to an integer greater than or equal to zero; or as a percentage of total rows processed so far – in which case, set this to a string of the form `N%`, where `N` is a decimal number between 0 and 100 exclusive (e.g. "20%"). Setting this value to any negative integer disables this feature (not recommended).

Default: **100**.

#### -logDir,--log.directory _&lt;string&gt;_

The writable directory where all log files will be stored; if the directory specified does not exist, it will be created. URLs are not acceptable (not even `file:/` URLs). Log files for a specific run, or execution, will be located in a sub-directory under the specified directory. Each execution generates a sub-directory identified by an "execution ID". See `engine.executionId` for more information about execution IDs. Relative paths will be resolved against the current working directory. Also, for convenience, if the path begins with a tilde (`~`), that symbol will be expanded to the current user's home directory.

Default: **"./logs"**.

#### -verbosity,--log.verbosity _&lt;number&gt;_

The desired level of verbosity. Valid values are:

- 0 (quiet): DSBulk will only log WARN and ERROR messages.
- 1 (normal): DSBulk will log INFO, WARN and ERROR messages.
- 2 (verbose) DSBulk will log DEBUG, INFO, WARN and ERROR messages.

Default: **1**.

#### --log.ansiMode _&lt;string&gt;_

Whether or not to use ANSI colors and other escape sequences in log messages printed to the console. Valid values are:

- `normal`: this is the default option. DSBulk will only use ANSI when the terminal is:
  - compatible with ANSI escape sequences; all common terminals on *nix and BSD systems, including MacOS, are ANSI-compatible, and some popular terminals for Windows (Mintty, MinGW);
  - a standard Windows DOS command prompt (ANSI sequences are translated on the fly).
- `force`: DSBulk will use ANSI, even if the terminal has not been detected as ANSI-compatible.
- `disable`: DSBulk will not use ANSI.

Note to Windows users: ANSI support on Windows works best when the Microsoft Visual C++ 2008 SP1 Redistributable Package is installed; you can download it [here](https://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=5582).

Default: **"normal"**.

#### --log.maxQueryWarnings _&lt;number&gt;_

The maximum number of query warnings to log before muting them. Query warnings are sent by the server (for example, if the number of statements in a batch is greater than the warning threshold configured on the server). They are useful to diagnose suboptimal configurations but tend to be too invasive, which is why DSBulk by default will only log the 50 first query warnings; any subsequent warnings will be muted and won't be logged at all. Setting this value to any negative integer disables this feature (not recommended).

Default: **50**.

#### --log.row.maxResultSetValueLength _&lt;number&gt;_

The maximum length for a result set value. Result set values longer than this value will be truncated.

Setting this value to `-1` makes the maximum length for a result set value unlimited (not recommended).

Default: **50**.

#### --log.row.maxResultSetValues _&lt;number&gt;_

The maximum number of result set values to print. If the row has more result set values than this limit, the exceeding values will not be printed.

Setting this value to `-1` makes the maximum number of result set values unlimited (not recommended).

Default: **50**.

#### --log.stmt.level _&lt;string&gt;_

The desired log level. Valid values are:

- ABRIDGED: Print only basic information in summarized form.
- NORMAL: Print basic information in summarized form, and the statement's query string, if available. For batch statements, this verbosity level also prints information about the batch's inner statements.
- EXTENDED: Print full information, including the statement's query string, if available, and the statement's bound values, if available. For batch statements, this verbosity level also prints all information available about the batch's inner statements.

Default: **"EXTENDED"**.

#### --log.stmt.maxBoundValueLength _&lt;number&gt;_

The maximum length for a bound value. Bound values longer than this value will be truncated.

Setting this value to `-1` makes the maximum length for a bound value unlimited (not recommended).

Default: **50**.

#### --log.stmt.maxBoundValues _&lt;number&gt;_

The maximum number of bound values to print. If the statement has more bound values than this limit, the exceeding values will not be printed.

Setting this value to `-1` makes the maximum number of bound values unlimited (not recommended).

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

The report interval. DSBulk will print useful metrics about the ongoing operation at this rate. Durations lesser than one second will be rounded up to 1 second.

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

<a name="stats"></a>
## Stats Settings

Settings applicable for the count workflow, ignored otherwise.

#### -stats,--stats.modes _&lt;list&lt;string&gt;&gt;_

Which kind(s) of statistics to compute. Only applicaple for the count workflow, ignored otherwise. Possible values are:
* `global`: count the total number of rows in the table.
* `ranges`: count the total number of rows per token range in the table.
* `hosts`: count the total number of rows per hosts in the table.
* `partitions`: count the total number of rows in the N biggest partitions in the table. When using this mode, you can chose how many partitions to track with the `numPartitions` setting.
The default value is `[global]`.

Default: **["global"]**.

#### -partitions,--stats.numPartitions _&lt;number&gt;_

The number of distinct partitions to count rows for. Only applicaple for the count workflow when `stats.mode` is `partitions`, ignored otherwise.

Default: **10**.

