# DataStax Bulk Loader v1.8.0 Options

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
<a href="#engine">Engine Settings</a><br>
<a href="#executor">Executor Settings</a><br>
<a href="#log">Log Settings</a><br>
<a href="#monitoring">Monitoring Settings</a><br>
<a href="#runner">Runner Settings</a><br>
<a href="#stats">Stats Settings</a><br>
<a href="#datastax-java-driver">Driver Settings</a><br>
<a name="Common"></a>
## Common Settings

#### -f _&lt;string&gt;_

Load options from the given file rather than from `<dsbulk_home>/conf/application.conf`.

#### -c,<br />--connector.name<br />--dsbulk.connector.name _&lt;string&gt;_

The name of the connector to use.

Default: **"csv"**.

#### -url,<br />--connector.csv.url<br />--dsbulk.connector.csv.url _&lt;string&gt;_

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

#### -delim,<br />--connector.csv.delimiter<br />--dsbulk.connector.csv.delimiter _&lt;string&gt;_

The character(s) to use as field delimiter. Field delimiters containing more than one character are accepted.

Default: **","**.

#### -header,<br />--connector.csv.header<br />--dsbulk.connector.csv.header _&lt;boolean&gt;_

Enable or disable whether the files to read or write begin with a header line. If enabled for loading, the first non-empty line in every file will assign field names for each record column, in lieu of `schema.mapping`, `fieldA = col1, fieldB = col2, fieldC = col3`. If disabled for loading, records will not contain fields names, only field indexes, `0 = col1, 1 = col2, 2 = col3`. For unloading, if this setting is enabled, each file will begin with a header line, and if disabled, each file will not contain a header line.

Note: This option will apply to all files loaded or unloaded.

Default: **true**.

#### -skipRecords,<br />--connector.csv.skipRecords<br />--dsbulk.connector.csv.skipRecords _&lt;number&gt;_

The number of records to skip from each input file before the parser can begin to execute. Note that if the file contains a header line, that line is not counted as a valid record. This setting is ignored when writing.

Default: **0**.

#### -maxRecords,<br />--connector.csv.maxRecords<br />--dsbulk.connector.csv.maxRecords _&lt;number&gt;_

The maximum number of records to read from or write to each file. When reading, all records past this number will be discarded. When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting. Note that when writing to anything other than a directory, this setting is ignored. This setting takes into account the *header* setting: if a file begins with a header line, that line is not counted as a record. This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### -url,<br />--connector.json.url<br />--dsbulk.connector.json.url _&lt;string&gt;_

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

#### -skipRecords,<br />--connector.json.skipRecords<br />--dsbulk.connector.json.skipRecords _&lt;number&gt;_

The number of JSON records to skip from each input file before the parser can begin to execute. This setting is ignored when writing.

Default: **0**.

#### -maxRecords,<br />--connector.json.maxRecords<br />--dsbulk.connector.json.maxRecords _&lt;number&gt;_

The maximum number of records to read from or write to each file. When reading, all records past this number will be discarded. When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting. Note that when writing to anything other than a directory, this setting is ignored. This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### -h,<br />--driver.basic.contact-points<br />--datastax-java-driver.basic.contact-points _&lt;list&lt;string&gt;&gt;_

The contact points to use for the initial connection to the cluster.

These are addresses of Cassandra nodes that the driver uses to discover the cluster topology. Only one contact point is required (the driver will retrieve the address of the other nodes automatically), but it is usually a good idea to provide more than one contact point, because if that single contact point is unavailable, the driver cannot initialize itself correctly.

This must be a list of strings with each contact point specified as "host" or "host:port". If the specified host doesn't have a port, the default port specified in `basic.default-port` will be used. Note that Cassandra 3 and below and DSE 6.7 and below require all nodes in a cluster to share the same port (see CASSANDRA-7544).

Valid examples of contact points are:
- IPv4 addresses with ports: `[ "192.168.0.1:9042", "192.168.0.2:9042" ]`
- IPv4 addresses without ports: `[ "192.168.0.1", "192.168.0.2" ]`
- IPv6 addresses with ports: `[ "fe80:0:0:0:f861:3eff:fe1d:9d7b:9042", "fe80:0:0:f861:3eff:fe1d:9d7b:9044:9042" ]`
- IPv6 addresses without ports: `[ "fe80:0:0:0:f861:3eff:fe1d:9d7b", "fe80:0:0:f861:3eff:fe1d:9d7b:9044" ]`
- Host names with ports: `[ "host1.com:9042", "host2.com:9042" ]`
- Host names without ports: `[ "host1.com", "host2.com:" ]`

If the host is a DNS name that resolves to multiple A-records, all the corresponding addresses will be used. Avoid using "localhost" as the host name (since it resolves to both IPv4 and IPv6 addresses on some platforms).

The heuristic to determine whether or not a contact point is in the form "host" or "host:port" is not 100% accurate for some IPv6 addresses; you should avoid ambiguous IPv6 addresses such as `fe80::f861:3eff:fe1d:1234`, because such a string can be seen either as a combination of IP `fe80::f861:3eff:fe1d` with port 1234, or as IP `fe80::f861:3eff:fe1d:1234` without port. In such cases, DSBulk will not change the contact point. This issue can be avoided by providing IPv6 addresses in full form, e.g. if instead of `fe80::f861:3eff:fe1d:1234` you provide `fe80:0:0:0:0:f861:3eff:fe1d:1234`, then the string is unequivocally parsed as IP `fe80:0:0:0:0:f861:3eff:fe1d` with port 1234.

Note: on Cloud deployments, DSBulk automatically sets this option to an empty list, as contact points are not allowed to be explicitly provided when connecting to DataStax Astra databases.

Default: **["127.0.0.1:9042"]**.

#### -port,<br />--driver.basic.default-port<br />--datastax-java-driver.basic.default-port _&lt;number&gt;_

The default port to use for `basic.contact-points`, when a host is specified without port. Note that Cassandra 3 and below and DSE 6.7 and below require all nodes in a cluster to share the same port (see CASSANDRA-7544). If this setting is not specified, the default port will be 9042.

Default: **9042**.

#### -b,<br />--driver.basic.cloud.secure-connect-bundle<br />--datastax-java-driver.basic.cloud.secure-connect-bundle _&lt;string&gt;_

The location of the secure bundle used to connect to a Datastax Astra database. This setting must be a path on the local filesystem or a valid URL.

Examples:

    "/path/to/bundle.zip"          # path on *Nix systems
    "./path/to/bundle.zip"         # path on *Nix systems, relative to workding directory
    "~/path/to/bundle.zip"         # path on *Nix systems, relative to home directory
    "C:\\path\\to\\bundle.zip"     # path on Windows systems,
                                   # note that you need to escape backslashes in HOCON
    "file:/a/path/to/bundle.zip"   # URL with file protocol
    "http://host.com/bundle.zip"   # URL with HTTP protocol

Note: if you set this to a non-null value, DSBulk assumes that you are connecting to an DataStax Astra database; in this case, you should not set any of the following settings because they are not compatible with Cloud deployments:

- `datastax-java-driver.basic.contact-points`
- `datastax-java-driver.basic.request.consistency`
- `datastax-java-driver.advanced.ssl-engine-factory.*`

If you do so, a log will be emitted and the setting will be ignored.

Default: **null**.

#### -u,<br />--driver.advanced.auth-provider.username<br />--datastax-java-driver.advanced.auth-provider.username _&lt;string&gt;_

The username to use to authenticate against a cluster with authentication enabled. Providers that accept this setting:

 - `PlainTextAuthProvider`


Default: **null**.

#### -p,<br />--driver.advanced.auth-provider.password<br />--datastax-java-driver.advanced.auth-provider.password _&lt;string&gt;_

The password to use to authenticate against a cluster with authentication enabled. Providers that accept this setting:

 - `PlainTextAuthProvider`


Default: **null**.

#### -cl,<br />--driver.basic.request.consistency<br />--datastax-java-driver.basic.request.consistency _&lt;string&gt;_

The consistency level to use for all queries. Note that stronger consistency levels usually result in reduced throughput. In addition, any level higher than `ONE` will automatically disable continuous paging, which can dramatically reduce read throughput.

Valid values are: `ANY`, `LOCAL_ONE`, `ONE`, `TWO`, `THREE`, `LOCAL_QUORUM`, `QUORUM`, `EACH_QUORUM`, `ALL`.

Note: on Cloud deployments, the only accepted consistency level when writing is `LOCAL_QUORUM`. Therefore, the default value is `LOCAL_ONE`, except when loading in Cloud deployments, in which case the default is automatically changed to `LOCAL_QUORUM`.

Default: **"LOCAL_ONE"**.

#### -dc,<br />--driver.basic.load-balancing-policy.local-datacenter<br />--datastax-java-driver.basic.load-balancing-policy.local-datacenter _&lt;string&gt;_

The datacenter that is considered "local": the default load balancing policy will only include nodes from this datacenter in its query plans. Set this to a non-null value if you want to force the local datacenter; otherwise, the `DcInferringLoadBalancingPolicy` used by default by DSBulk will infer the local datacenter from the provided contact points.

Default: **null**.

#### -k,<br />--schema.keyspace<br />--dsbulk.schema.keyspace _&lt;string&gt;_

Keyspace used for loading or unloading data. Keyspace names should not be quoted and are case-sensitive. `MyKeyspace` will match a keyspace named `MyKeyspace` but not `mykeyspace`. Either `keyspace` or `graph` is required if `query` is not specified or is not qualified with a keyspace name.

Default: **null**.

#### -t,<br />--schema.table<br />--dsbulk.schema.table _&lt;string&gt;_

Table used for loading or unloading data. Table names should not be quoted and are case-sensitive. `MyTable` will match a table named `MyTable` but not `mytable`. Either `table`, `vertex` or `edge` is required if `query` is not specified.

Default: **null**.

#### -m,<br />--schema.mapping<br />--dsbulk.schema.mapping _&lt;string&gt;_

The field-to-column mapping to use, that applies to both loading and unloading; ignored when counting. If not specified, the loader will apply a strict one-to-one mapping between the source fields and the database table. If that is not what you want, then you must supply an explicit mapping. Mappings should be specified as a map of the following form:

- Indexed data sources: `0 = col1, 1 = col2, 2 = col3`, where `0`, `1`, `2`, are the zero-based indices of fields in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map the first `n` fields is to simply specify the destination columns: `col1, col2, col3`.
- Mapped data sources: `fieldA = col1, fieldB = col2, fieldC = col3`, where `fieldA`, `fieldB`, `fieldC`, are field names in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map fields named like columns is to simply specify the destination columns: `col1, col2, col3`.

To specify that a field should be used as the timestamp (a.k.a. write-time) or ttl (a.k.a. time-to-live) of the inserted row, use the specially named fake columns `__ttl` and `__timestamp`: `fieldA = __timestamp, fieldB = __ttl`. Note that Timestamp fields are parsed as regular CQL timestamp columns and must comply with either `codec.timestamp`, or alternatively, with `codec.unit` + `codec.epoch`. TTL fields are parsed as integers representing durations in seconds, and must comply with `codec.number`.

To specify that a column should be populated with the result of a function call, specify the function call as the input field (e.g. `now() = c4`). Note, this is only relevant for load operations. Similarly, to specify that a field should be populated with the result of a function call, specify the function call as the input column (e.g. `field1 = now()`). This is only relevant for unload operations. Function calls can also be qualified by a keyspace name: `field1 = ks1.max(c1,c2)`.

In addition, for mapped data sources, it is also possible to specify that the mapping be partly auto-generated and partly explicitly specified. For example, if a source row has fields `c1`, `c2`, `c3`, and `c5`, and the table has columns `c1`, `c2`, `c3`, `c4`, one can map all like-named columns and specify that `c5` in the source maps to `c4` in the table as follows: `* = *, c5 = c4`.

One can specify that all like-named fields be mapped, except for `c2`: `* = -c2`. To skip `c2` and `c3`: `* = [-c2, -c3]`.

Any identifier, field or column, that is not strictly alphanumeric (i.e. not matching `[a-zA-Z0-9_]+`) must be surrounded by double-quotes, just like you would do in CQL: `"Field ""A""" = "Column 2"` (to escape a double-quote, simply double it). Note that, contrary to the CQL grammar, unquoted identifiers will not be lower-cased: an identifier such as `MyColumn1` will match a column named `"MyColumn1"` and not `mycolumn1`.

The exact type of mapping to use depends on the connector being used. Some connectors can only produce indexed records; others can only produce mapped ones, while others are capable of producing both indexed and mapped records at the same time. Refer to the connector's documentation to know which kinds of mapping it supports.

Default: **null**.

#### -dryRun,<br />--engine.dryRun<br />--dsbulk.engine.dryRun _&lt;boolean&gt;_

Enable or disable dry-run mode, a test mode that runs the command but does not load data. Not applicable for unloading nor counting.

Default: **false**.

#### -maxConcurrentQueries,<br />--engine.maxConcurrentQueries<br />--dsbulk.engine.maxConcurrentQueries _&lt;string&gt;_

The maximum number of concurrent queries that should be carried in parallel.

This acts as a safeguard to prevent more queries executing in parallel than the cluster can handle, or to regulate throughput when latencies get too high. Batch statements count as one query.

When using continuous paging, also make sure to set this number to a value equal to or lesser than the number of nodes in the local datacenter multiplied by the value configured server-side for `continuous_paging.max_concurrent_sessions` in the cassandra.yaml configuration file (60 by default); otherwise some requests might be rejected.

The special syntax `NC` can be used to specify a number that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 concurrent queries.

The default value is 'AUTO'; with this special value, DSBulk will optimize the number of concurrent queries according to the number of available cores, and the operation being executed. The actual value usually ranges from the number of cores to eight times that number.

Default: **"AUTO"**.

#### -maxErrors,<br />--log.maxErrors<br />--dsbulk.log.maxErrors _&lt;number&gt;_

The maximum number of errors to tolerate before aborting the entire operation. This can be expressed either as an absolute number of errors - in which case, set this to an integer greater than or equal to zero; or as a percentage of total rows processed so far - in which case, set this to a string of the form `N%`, where `N` is a decimal number between 0 and 100 exclusive (e.g. "20%"). Setting this value to any negative integer disables this feature (not recommended).

Default: **100**.

#### -logDir,<br />--log.directory<br />--dsbulk.log.directory _&lt;string&gt;_

The writable directory where all log files will be stored; if the directory specified does not exist, it will be created. URLs are not acceptable (not even `file:/` URLs). Log files for a specific run, or execution, will be located in a sub-directory under the specified directory. Each execution generates a sub-directory identified by an "execution ID". See `engine.executionId` for more information about execution IDs. Relative paths will be resolved against the current working directory. Also, for convenience, if the path begins with a tilde (`~`), that symbol will be expanded to the current user's home directory.

Default: **"./logs"**.

#### -verbosity,<br />--log.verbosity<br />--dsbulk.log.verbosity _&lt;number&gt;_

The desired level of verbosity. Valid values are:

- 0 (quiet): DSBulk will only log WARN and ERROR messages.
- 1 (normal): DSBulk will log INFO, WARN and ERROR messages.
- 2 (verbose) DSBulk will log DEBUG, INFO, WARN and ERROR messages.

Default: **1**.

#### -reportRate,<br />--monitoring.reportRate<br />--dsbulk.monitoring.reportRate _&lt;string&gt;_

The report interval. DSBulk will print useful metrics about the ongoing operation at this rate; for example, if this value is set to 10 seconds, then DSBulk will print metrics every ten seconds. Valid values: any value specified in [HOCON duration syntax](https://github.com/lightbend/config/blob/master/HOCON.md#duration-format), but durations lesser than one second will be rounded up to 1 second.

Default: **"5 seconds"**.

<a name="connector"></a>
## Connector Settings

Connector-specific settings. This section contains settings for the connector to use; it also contains sub-sections, one for each available connector.

This setting is ignored when counting.

#### -c,<br />--connector.name<br />--dsbulk.connector.name _&lt;string&gt;_

The name of the connector to use.

Default: **"csv"**.

<a name="connector.csv"></a>
### Connector Csv Settings

CSV Connector configuration.

#### -url,<br />--connector.csv.url<br />--dsbulk.connector.csv.url _&lt;string&gt;_

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

#### -delim,<br />--connector.csv.delimiter<br />--dsbulk.connector.csv.delimiter _&lt;string&gt;_

The character(s) to use as field delimiter. Field delimiters containing more than one character are accepted.

Default: **","**.

#### -header,<br />--connector.csv.header<br />--dsbulk.connector.csv.header _&lt;boolean&gt;_

Enable or disable whether the files to read or write begin with a header line. If enabled for loading, the first non-empty line in every file will assign field names for each record column, in lieu of `schema.mapping`, `fieldA = col1, fieldB = col2, fieldC = col3`. If disabled for loading, records will not contain fields names, only field indexes, `0 = col1, 1 = col2, 2 = col3`. For unloading, if this setting is enabled, each file will begin with a header line, and if disabled, each file will not contain a header line.

Note: This option will apply to all files loaded or unloaded.

Default: **true**.

#### -skipRecords,<br />--connector.csv.skipRecords<br />--dsbulk.connector.csv.skipRecords _&lt;number&gt;_

The number of records to skip from each input file before the parser can begin to execute. Note that if the file contains a header line, that line is not counted as a valid record. This setting is ignored when writing.

Default: **0**.

#### -maxRecords,<br />--connector.csv.maxRecords<br />--dsbulk.connector.csv.maxRecords _&lt;number&gt;_

The maximum number of records to read from or write to each file. When reading, all records past this number will be discarded. When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting. Note that when writing to anything other than a directory, this setting is ignored. This setting takes into account the *header* setting: if a file begins with a header line, that line is not counted as a record. This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### -quote,<br />--connector.csv.quote<br />--dsbulk.connector.csv.quote _&lt;string&gt;_

The character used for quoting fields when the field delimiter is part of the field value. Only one character can be specified. Note that this setting applies to all files to be read or written.

Default: **"\""**.

#### -comment,<br />--connector.csv.comment<br />--dsbulk.connector.csv.comment _&lt;string&gt;_

The character that represents a line comment when found in the beginning of a line of text. Only one character can be specified. Note that this setting applies to all files to be read or written. This feature is disabled by default (indicated by its `null` character value).

Default: **"\u0000"**.

#### --connector.csv.compression<br />--dsbulk.connector.csv.compression _&lt;string&gt;_

The compression that will be used for writing or reading files. Supported values are (for both reading and writing): `none`, `xz`, `gzip`, `bzip2`, `zstd`, `lz4`, `lzma`, `snappy`, `deflate`.  For reading only, supported values are: `brotli`, `z`, `deflate64`.

Default: **"none"**.

#### --connector.csv.emptyValue<br />--dsbulk.connector.csv.emptyValue _&lt;string&gt;_

Sets the String representation of an empty value. When reading, if the parser does not read any character from the input, and the input is within quotes, this value will be used instead. When writing, if the writer has an empty string to write to the output, this value will be used instead. The default value is `AUTO`, which means that, when reading, the parser will emit an empty string, and when writing, the writer will write a quoted empty field to the output.

Default: **"AUTO"**.

#### -encoding,<br />--connector.csv.encoding<br />--dsbulk.connector.csv.encoding _&lt;string&gt;_

The file encoding to use for all read or written files.

Default: **"UTF-8"**.

#### -escape,<br />--connector.csv.escape<br />--dsbulk.connector.csv.escape _&lt;string&gt;_

The character used for escaping quotes inside an already quoted value. Only one character can be specified. Note that this setting applies to all files to be read or written.

Default: **"\\"**.

#### --connector.csv.fileNameFormat<br />--dsbulk.connector.csv.fileNameFormat _&lt;string&gt;_

The file name format to use when writing. This setting is ignored when reading and for non-file URLs. The file name must comply with the formatting rules of `String.format()`, and must contain a `%d` format specifier that will be used to increment file name counters.

If compression is enabled, the default value for this setting will be modified to include the default suffix for the selected compression method. For example, if compression is `gzip`, the default file name format will be `output-%06d.csv.gz`.

Default: **"output-%06d.csv"**.

#### --connector.csv.fileNamePattern<br />--dsbulk.connector.csv.fileNamePattern _&lt;string&gt;_

The glob pattern to use when searching for files to read. The syntax to use is the glob syntax, as described in `java.nio.file.FileSystem.getPathMatcher()`. This setting is ignored when writing and for non-file URLs. Only applicable when the *url* setting points to a directory on a known filesystem, ignored otherwise.

If compression is enabled, the default value for this setting will be modified to include the default suffix for the selected compression method. For example, if compression is `gzip`, the default glob pattern will be `**/*.csv.gz`.

Default: **"\*\*/\*.csv"**.

#### --connector.csv.ignoreLeadingWhitespaces<br />--dsbulk.connector.csv.ignoreLeadingWhitespaces _&lt;boolean&gt;_

Defines whether or not leading whitespaces from values being read/written should be skipped. This setting is honored when reading and writing. Default value is false.

Default: **false**.

#### --connector.csv.ignoreLeadingWhitespacesInQuotes<br />--dsbulk.connector.csv.ignoreLeadingWhitespacesInQuotes _&lt;boolean&gt;_

Defines whether or not trailing whitespaces from quoted values should be skipped. This setting is only honored when reading; it is ignored when writing. Default value is false.

Default: **false**.

#### --connector.csv.ignoreTrailingWhitespaces<br />--dsbulk.connector.csv.ignoreTrailingWhitespaces _&lt;boolean&gt;_

Defines whether or not trailing whitespaces from values being read/written should be skipped. This setting is honored when reading and writing. Default value is false.

Default: **false**.

#### --connector.csv.ignoreTrailingWhitespacesInQuotes<br />--dsbulk.connector.csv.ignoreTrailingWhitespacesInQuotes _&lt;boolean&gt;_

Defines whether or not leading whitespaces from quoted values should be skipped. This setting is only honored when reading; it is ignored when writing. Default value is false.

Default: **false**.

#### --connector.csv.maxCharsPerColumn<br />--dsbulk.connector.csv.maxCharsPerColumn _&lt;number&gt;_

The maximum number of characters that a field can contain. This setting is used to size internal buffers and to avoid out-of-memory problems. If set to -1, internal buffers will be resized dynamically. While convenient, this can lead to memory problems. It could also hurt throughput, if some large fields require constant resizing; if this is the case, set this value to a fixed positive number that is big enough to contain all field values.

Default: **4096**.

#### --connector.csv.maxColumns<br />--dsbulk.connector.csv.maxColumns _&lt;number&gt;_

The maximum number of columns that a record can contain. This setting is used to size internal buffers and to avoid out-of-memory problems.

Default: **512**.

#### -maxConcurrentFiles,<br />--connector.csv.maxConcurrentFiles<br />--dsbulk.connector.csv.maxConcurrentFiles _&lt;string&gt;_

The maximum number of files that can be read or written simultaneously. This setting is effective only when reading from or writing to many resources in parallel, such as a collection of files in a root directory; it is ignored otherwise. The special syntax `NC` can be used to specify a number of threads that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 threads.

The default value is the special value AUTO; with this value, the connector will decide the best number of files.

Default: **"AUTO"**.

#### -newline,<br />--connector.csv.newline<br />--dsbulk.connector.csv.newline _&lt;string&gt;_

The character(s) that represent a line ending. When set to the special value `auto` (default), the system's line separator, as determined by `System.lineSeparator()`, will be used when writing, and auto-detection of line endings will be enabled when reading. Only one or two characters can be specified; beware that most typical line separator characters need to be escaped, e.g. one should specify `\r\n` for the typical line ending on Windows systems (carriage return followed by a new line).

Default: **"auto"**.

#### --connector.csv.normalizeLineEndingsInQuotes<br />--dsbulk.connector.csv.normalizeLineEndingsInQuotes _&lt;boolean&gt;_

Defines whether or not line separators should be replaced by a normalized line separator '\n' inside quoted values. This setting is honored when reading and writing. Note: due to a bug in the CSV parsing library, on Windows systems, the line ending detection mechanism may not function properly when this setting is false; in case of problem, set this to true. Default value is false.

Default: **false**.

#### --connector.csv.nullValue<br />--dsbulk.connector.csv.nullValue _&lt;string&gt;_

Sets the String representation of a null value. When reading, if the parser does not read any character from the input, this value will be used instead. When writing, if the writer has a null object to write to the output, this value will be used instead. The default value is `AUTO`, which means that, when reading, the parser will emit a `null`, and when writing, the writer won't write any character at all to the output.

Default: **"AUTO"**.

#### --connector.csv.recursive<br />--dsbulk.connector.csv.recursive _&lt;boolean&gt;_

Enable or disable scanning for files in the root's subdirectories. Only applicable when *url* is set to a directory on a known filesystem. Used for loading only.

Default: **false**.

#### --connector.csv.urlfile<br />--dsbulk.connector.csv.urlfile _&lt;string&gt;_

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

The default value is "" - which means that this property is ignored.

Default: **&lt;unspecified&gt;**.

<a name="connector.json"></a>
### Connector Json Settings

JSON Connector configuration.

#### -url,<br />--connector.json.url<br />--dsbulk.connector.json.url _&lt;string&gt;_

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

#### -skipRecords,<br />--connector.json.skipRecords<br />--dsbulk.connector.json.skipRecords _&lt;number&gt;_

The number of JSON records to skip from each input file before the parser can begin to execute. This setting is ignored when writing.

Default: **0**.

#### -maxRecords,<br />--connector.json.maxRecords<br />--dsbulk.connector.json.maxRecords _&lt;number&gt;_

The maximum number of records to read from or write to each file. When reading, all records past this number will be discarded. When writing, a file will contain at most this number of records; if more records remain to be written, a new file will be created using the *fileNameFormat* setting. Note that when writing to anything other than a directory, this setting is ignored. This feature is disabled by default (indicated by its `-1` value).

Default: **-1**.

#### --connector.json.mode<br />--dsbulk.connector.json.mode _&lt;string&gt;_

The mode for loading and unloading JSON documents. Valid values are:

* MULTI_DOCUMENT: Each resource may contain an arbitrary number of successive JSON documents to be mapped to records. For example the format of each JSON document is a single document: `{doc1}`. The root directory for the JSON documents can be specified with `url` and the documents can be read recursively by setting `connector.json.recursive` to true.
* SINGLE_DOCUMENT: Each resource contains a root array whose elements are JSON documents to be mapped to records. For example, the format of the JSON document is an array with embedded JSON documents: `[ {doc1}, {doc2}, {doc3} ]`.

Default: **"MULTI_DOCUMENT"**.

#### --connector.json.compression<br />--dsbulk.connector.json.compression _&lt;string&gt;_

The compression that will be used for writing or reading files. Supported values are (for both reading and writing): `none`, `xz`, `gzip`, `bzip2`, `zstd`, `lz4`, `lzma`, `snappy`, `deflate`.  For reading only, supported values are: `brotli`, `z`, `deflate64`.

Default: **"none"**.

#### --connector.json.deserializationFeatures<br />--dsbulk.connector.json.deserializationFeatures _&lt;map&lt;string,boolean&gt;&gt;_

A map of JSON deserialization features to set. Map keys should be enum constants defined in `com.fasterxml.jackson.databind.DeserializationFeature`. The default value is the only way to guarantee that floating point numbers will not have their precision truncated when parsed, but can result in slightly slower parsing. Used for loading only.

Note that some Jackson features might not be supported, in particular features that operate on the resulting Json tree by filtering elements or altering their contents, since such features conflict with dsbulk's own filtering and formatting capabilities. Instead of trying to modify the resulting tree using Jackson features, you should try to achieve the same result using the settings available under the `codec` and `schema` sections.

#### -encoding,<br />--connector.json.encoding<br />--dsbulk.connector.json.encoding _&lt;string&gt;_

The file encoding to use for all read or written files.

Default: **"UTF-8"**.

#### --connector.json.fileNameFormat<br />--dsbulk.connector.json.fileNameFormat _&lt;string&gt;_

The file name format to use when writing. This setting is ignored when reading and for non-file URLs. The file name must comply with the formatting rules of `String.format()`, and must contain a `%d` format specifier that will be used to increment file name counters.

If compression is enabled, the default value for this setting will be modified to include the default suffix for the selected compression method. For example, if compression is `gzip`, the default file name format will be `output-%06d.json.gz`.

Default: **"output-%06d.json"**.

#### --connector.json.fileNamePattern<br />--dsbulk.connector.json.fileNamePattern _&lt;string&gt;_

The glob pattern to use when searching for files to read. The syntax to use is the glob syntax, as described in `java.nio.file.FileSystem.getPathMatcher()`. This setting is ignored when writing and for non-file URLs. Only applicable when the *url* setting points to a directory on a known filesystem, ignored otherwise.

If compression is enabled, the default value for this setting will be modified to include the default suffix for the selected compression method. For example, if compression is `gzip`, the default glob pattern will be `**/*.json.gz`.

Default: **"\*\*/\*.json"**.

#### --connector.json.generatorFeatures<br />--dsbulk.connector.json.generatorFeatures _&lt;map&lt;string,boolean&gt;&gt;_

JSON generator features to enable. Valid values are all the enum constants defined in `com.fasterxml.jackson.core.JsonGenerator.Feature`. For example, a value of `{ ESCAPE_NON_ASCII : true, QUOTE_FIELD_NAMES : true }` will configure the generator to escape all characters beyond 7-bit ASCII and quote field names when writing JSON output. Used for unloading only.

Note that some Jackson features might not be supported, in particular features that operate on the resulting Json tree by filtering elements or altering their contents, since such features conflict with dsbulk's own filtering and formatting capabilities. Instead of trying to modify the resulting tree using Jackson features, you should try to achieve the same result using the settings available under the `codec` and `schema` sections.

#### -maxConcurrentFiles,<br />--connector.json.maxConcurrentFiles<br />--dsbulk.connector.json.maxConcurrentFiles _&lt;string&gt;_

The maximum number of files that can be read or written simultaneously. This setting is effective only when reading from or writing to many resources in parallel, such as a collection of files in a root directory; it is ignored otherwise. The special syntax `NC` can be used to specify a number of threads that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 threads.

The default value is the special value AUTO; with this value, the connector will decide the best number of files.

Default: **"AUTO"**.

#### --connector.json.parserFeatures<br />--dsbulk.connector.json.parserFeatures _&lt;map&lt;string,boolean&gt;&gt;_

JSON parser features to enable. Valid values are all the enum constants defined in `com.fasterxml.jackson.core.JsonParser.Feature`. For example, a value of `{ ALLOW_COMMENTS : true, ALLOW_SINGLE_QUOTES : true }` will configure the parser to allow the use of comments and single-quoted strings in JSON data. Used for loading only.

Note that some Jackson features might not be supported, in particular features that operate on the resulting Json tree by filtering elements or altering their contents, since such features conflict with dsbulk's own filtering and formatting capabilities. Instead of trying to modify the resulting tree using Jackson features, you should try to achieve the same result using the settings available under the `codec` and `schema` sections.

#### --connector.json.prettyPrint<br />--dsbulk.connector.json.prettyPrint _&lt;boolean&gt;_

Enable or disable pretty printing. When enabled, JSON records are written with indents. Used for unloading only.

Note: Can result in much bigger records.

Default: **false**.

#### --connector.json.recursive<br />--dsbulk.connector.json.recursive _&lt;boolean&gt;_

Enable or disable scanning for files in the root's subdirectories. Only applicable when *url* is set to a directory on a known filesystem. Used for loading only.

Default: **false**.

#### --connector.json.serializationFeatures<br />--dsbulk.connector.json.serializationFeatures _&lt;map&lt;string,boolean&gt;&gt;_

A map of JSON serialization features to set. Map keys should be enum constants defined in `com.fasterxml.jackson.databind.SerializationFeature`. Used for unloading only.

Note that some Jackson features might not be supported, in particular features that operate on the resulting Json tree by filtering elements or altering their contents, since such features conflict with dsbulk's own filtering and formatting capabilities. Instead of trying to modify the resulting tree using Jackson features, you should try to achieve the same result using the settings available under the `codec` and `schema` sections.

#### --connector.json.serializationStrategy<br />--dsbulk.connector.json.serializationStrategy _&lt;string&gt;_

The strategy to use for filtering out entries when formatting output. Valid values are enum constants defined in `com.fasterxml.jackson.annotation.JsonInclude.Include` (but beware that the `CUSTOM` strategy cannot be honored). Used for unloading only.

Default: **"ALWAYS"**.

#### --connector.json.urlfile<br />--dsbulk.connector.json.urlfile _&lt;string&gt;_

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

#### -k,<br />--schema.keyspace<br />--dsbulk.schema.keyspace _&lt;string&gt;_

Keyspace used for loading or unloading data. Keyspace names should not be quoted and are case-sensitive. `MyKeyspace` will match a keyspace named `MyKeyspace` but not `mykeyspace`. Either `keyspace` or `graph` is required if `query` is not specified or is not qualified with a keyspace name.

Default: **null**.

#### -t,<br />--schema.table<br />--dsbulk.schema.table _&lt;string&gt;_

Table used for loading or unloading data. Table names should not be quoted and are case-sensitive. `MyTable` will match a table named `MyTable` but not `mytable`. Either `table`, `vertex` or `edge` is required if `query` is not specified.

Default: **null**.

#### -m,<br />--schema.mapping<br />--dsbulk.schema.mapping _&lt;string&gt;_

The field-to-column mapping to use, that applies to both loading and unloading; ignored when counting. If not specified, the loader will apply a strict one-to-one mapping between the source fields and the database table. If that is not what you want, then you must supply an explicit mapping. Mappings should be specified as a map of the following form:

- Indexed data sources: `0 = col1, 1 = col2, 2 = col3`, where `0`, `1`, `2`, are the zero-based indices of fields in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map the first `n` fields is to simply specify the destination columns: `col1, col2, col3`.
- Mapped data sources: `fieldA = col1, fieldB = col2, fieldC = col3`, where `fieldA`, `fieldB`, `fieldC`, are field names in the source data; and `col1`, `col2`, `col3` are bound variable names in the insert statement.
    - A shortcut to map fields named like columns is to simply specify the destination columns: `col1, col2, col3`.

To specify that a field should be used as the timestamp (a.k.a. write-time) or ttl (a.k.a. time-to-live) of the inserted row, use the specially named fake columns `__ttl` and `__timestamp`: `fieldA = __timestamp, fieldB = __ttl`. Note that Timestamp fields are parsed as regular CQL timestamp columns and must comply with either `codec.timestamp`, or alternatively, with `codec.unit` + `codec.epoch`. TTL fields are parsed as integers representing durations in seconds, and must comply with `codec.number`.

To specify that a column should be populated with the result of a function call, specify the function call as the input field (e.g. `now() = c4`). Note, this is only relevant for load operations. Similarly, to specify that a field should be populated with the result of a function call, specify the function call as the input column (e.g. `field1 = now()`). This is only relevant for unload operations. Function calls can also be qualified by a keyspace name: `field1 = ks1.max(c1,c2)`.

In addition, for mapped data sources, it is also possible to specify that the mapping be partly auto-generated and partly explicitly specified. For example, if a source row has fields `c1`, `c2`, `c3`, and `c5`, and the table has columns `c1`, `c2`, `c3`, `c4`, one can map all like-named columns and specify that `c5` in the source maps to `c4` in the table as follows: `* = *, c5 = c4`.

One can specify that all like-named fields be mapped, except for `c2`: `* = -c2`. To skip `c2` and `c3`: `* = [-c2, -c3]`.

Any identifier, field or column, that is not strictly alphanumeric (i.e. not matching `[a-zA-Z0-9_]+`) must be surrounded by double-quotes, just like you would do in CQL: `"Field ""A""" = "Column 2"` (to escape a double-quote, simply double it). Note that, contrary to the CQL grammar, unquoted identifiers will not be lower-cased: an identifier such as `MyColumn1` will match a column named `"MyColumn1"` and not `mycolumn1`.

The exact type of mapping to use depends on the connector being used. Some connectors can only produce indexed records; others can only produce mapped ones, while others are capable of producing both indexed and mapped records at the same time. Refer to the connector's documentation to know which kinds of mapping it supports.

Default: **null**.

#### --schema.allowExtraFields<br />--dsbulk.schema.allowExtraFields _&lt;boolean&gt;_

Specify whether or not to accept records that contain extra fields that are not declared in the mapping. For example, if a record contains three fields A, B, and C, but the mapping only declares fields A and B, then if this option is true, C will be silently ignored and the record will be considered valid, and if false, the record will be rejected. This setting also applies to user-defined types and tuples. Only applicable for loading, ignored otherwise.

This setting is ignored when counting.

Default: **true**.

#### --schema.allowMissingFields<br />--dsbulk.schema.allowMissingFields _&lt;boolean&gt;_

Specify whether or not to accept records that are missing fields declared in the mapping. For example, if the mapping declares three fields A, B, and C, but a record contains only fields A and B, then if this option is true, C will be silently assigned null and the record will be considered valid, and if false, the record will be rejected. If the missing field is mapped to a primary key column, the record will always be rejected, since the database will reject the record. This setting also applies to user-defined types and tuples. Only applicable for loading, ignored otherwise.

This setting is ignored when counting.

Default: **false**.

#### -e,<br />--schema.edge<br />--dsbulk.schema.edge _&lt;string&gt;_

Edge label used for loading or unloading graph data. This option can only be used for modern graphs created with the Native engine (DSE 6.8+). The edge label must correspond to an existing table created with the `WITH EDGE LABEL` option; also, when `edge` is specified, then `from` and `to` must be specified as well. Edge labels should not be quoted and are case-sensitive. `MyEdge` will match a label named `MyEdge` but not `myedge`. Either `table`, `vertex` or `edge` is required if `query` is not specified.

Default: **null**.

#### -from,<br />--schema.from<br />--dsbulk.schema.from _&lt;string&gt;_

The name of the edge's incoming vertex label, for loading or unloading graph data. This option can only be used for modern graphs created with the Native engine (DSE 6.8+). This option is mandatory when `edge` is specified; ignored otherwise. Vertex labels should not be quoted and are case-sensitive. `MyVertex` will match a label named `MyVertex` but not `myvertex`.

Default: **null**.

#### -g,<br />--schema.graph<br />--dsbulk.schema.graph _&lt;string&gt;_

Graph name used for loading or unloading graph data. This option can only be used for modern graphs created with the Native engine (DSE 6.8+). Graph names should not be quoted and are case-sensitive. `MyGraph` will match a graph named `MyGraph` but not `mygraph`. Either `keyspace` or `graph` is required if `query` is not specified or is not qualified with a keyspace name.

Default: **null**.

#### --schema.nullToUnset<br />--dsbulk.schema.nullToUnset _&lt;boolean&gt;_

Specify whether to map `null` input values to "unset" in the database, i.e., don't modify a potentially pre-existing value of this field for this row. Valid for load scenarios, otherwise ignore. Note that setting to false creates tombstones to represent `null`.

Note that this setting is applied after the *codec.nullStrings* setting, and may intercept `null`s produced by that setting.

This setting is ignored when counting. When set to true but the protocol version in use does not support unset values (i.e., all protocol versions lesser than 4), this setting will be forced to false and a warning will be logged.

Default: **true**.

#### -timestamp,<br />--schema.preserveTimestamp<br />--dsbulk.schema.preserveTimestamp _&lt;boolean&gt;_

Whether to preserve cell timestamps when loading and unloading. Ignored when `schema.query` is provided, or when the target table is a counter table. If true, the following rules will be applied to generated queries:

- When loading, instead of a single INSERT statement, the generated query will be a BATCH query; this is required in order to preserve individual column timestamps for each row.
- When unloading, the generated SELECT statement will export each column along with its individual timestamp.

For both loading and unlaoding, DSBulk will import and export timestamps using field names such as `"writetime(<column>)"`, where `<column>` is the column's internal CQL name; for example, if the table has a column named `"MyCol"`, its corresponding timestamp would be exported as `"writetime(MyCol)"` in the generated query and in the resulting connector record. If you intend to use this feature to export and import tables letting DSBulk generate the appropriate queries, these names are fine and need not be changed. If, however, you would like to export or import data to or from external sources that use different field names, you could do so by using the function `writetime` in a schema.mapping entry; for example, the following mapping would map `col1` along with its timestamp to two distinct fields, `field1` and `field1_writetime`: `field1 = col1, field1_writetime = writetime(col1)`.

Default: **false**.

#### -ttl,<br />--schema.preserveTtl<br />--dsbulk.schema.preserveTtl _&lt;boolean&gt;_

Whether to preserve cell TTLs when loading and unloading. Ignored when `schema.query` is provided, or when the target table is a counter table. If true, the following rules will be applied to generated queries:

- When loading, instead of a single INSERT statement, the generated query will be a BATCH query; this is required in order to preserve individual column TTLs for each row.
- When unloading, the generated SELECT statement will export each column along with its individual TTL.

For both loading and unlaoding, DSBulk will import and export TTLs using field names such as `"ttl(<column>)"`, where `<column>` is the column's internal CQL name; for example, if the table has a column named `"MyCol"`, its corresponding TTL would be exported as `"ttl(MyCol)"` in the generated query and in the resulting connector record. If you intend to use this feature to export and import tables letting DSBulk generate the appropriate queries, these names are fine and need not be changed. If, however, you would like to export or import data to or from external sources that use different field names, you could do so by using the function `ttl` in a schema.mapping entry; for example, the following mapping would map `col1` along with its TTL to two distinct fields, `field1` and `field1_ttl`: `field1 = col1, field1_ttl = ttl(col1)`.

Default: **false**.

#### -query,<br />--schema.query<br />--dsbulk.schema.query _&lt;string&gt;_

The query to use. If not specified, then *schema.keyspace* and *schema.table* must be specified, and dsbulk will infer the appropriate statement based on the table's metadata, using all available columns. If `schema.keyspace` is provided, the query need not include the keyspace to qualify the table reference.

For loading, the statement can be any `INSERT`, `UPDATE` or `DELETE` statement. `INSERT` statements are preferred for most load operations, and bound variables should correspond to mapped fields; for example, `INSERT INTO table1 (c1, c2, c3) VALUES (:fieldA, :fieldB, :fieldC)`. `UPDATE` statements are required if the target table is a counter table, and the columns are updated with incremental operations (`SET col1 = col1 + :fieldA` where `fieldA` is a field in the input data). A `DELETE` statement will remove existing data during the load operation.

For unloading and counting, the statement can be any regular `SELECT` statement. If the statement does not contain any WHERE, ORDER BY, GROUP BY, or LIMIT clause, the engine will generate a token range restriction clause of the form: `WHERE token(...) > :start and token(...) <= :end` and will generate range read statements, thus allowing parallelization of reads while at the same time targeting coordinators that are also replicas (see schema.splits). If the statement does contain WHERE, ORDER BY, GROUP BY or LIMIT clauses however, the query will be executed as is; the engine will only be able to parallelize the operation if the query includes a WHERE clause including the following relations: `token(...) > :start AND token(...) <= :end` (the bound variables can have any name). Note that, unlike LIMIT clauses, PER PARTITION LIMIT clauses can be parallelized.

Statements can use both named and positional bound variables. Named bound variables should be preferred, unless the protocol version in use does not allow them; they usually have names matching those of the columns in the destination table, but this is not a strict requirement; it is, however, required that their names match those of fields specified in the mapping. Positional variables can also be used, and will be named after their corresponding column in the destination table.

When loading and unloading graph data, the query must be provided in plain CQL; Gremlin queries are not supported.

Note: The query is parsed to discover which bound variables are present, and to map the variables correctly to fields.

See *mapping* setting for more information.

Default: **null**.

#### --schema.queryTimestamp<br />--dsbulk.schema.queryTimestamp _&lt;string&gt;_

The timestamp of inserted/updated cells during load; otherwise, the current time of the system running the tool is used. Not applicable to unloading nor counting. Ignored when `schema.query` is provided. The value must be expressed in [`ISO_ZONED_DATE_TIME`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_ZONED_DATE_TIME) format.

Query timestamps for Cassandra have microsecond resolution; any sub-microsecond information specified is lost. For more information, see the [CQL Reference](https://docs.datastax.com/en/dse/6.0/cql/cql/cql_reference/cql_commands/cqlInsert.html#cqlInsert__timestamp-value).

Default: **null**.

#### --schema.queryTtl<br />--dsbulk.schema.queryTtl _&lt;number&gt;_

The Time-To-Live (TTL) of inserted/updated cells during load (seconds); a value of -1 means there is no TTL. Not applicable to unloading nor counting. Ignored when `schema.query` is provided. For more information, see the [CQL Reference](https://docs.datastax.com/en/dse/6.0/cql/cql/cql_reference/cql_commands/cqlInsert.html#cqlInsert__ime-value), [Setting the time-to-live (TTL) for value](http://docs.datastax.com/en/dse/6.0/cql/cql/cql_using/useTTL.html), and [Expiring data with time-to-live](http://docs.datastax.com/en/dse/6.0/cql/cql/cql_using/useExpire.html).

Default: **-1**.

#### --schema.splits<br />--dsbulk.schema.splits _&lt;string&gt;_

The number of token range splits in which to divide the token ring. In other words, this setting determines how many read requests will be generated in order to read an entire table. Only used when unloading and counting; ignored otherwise. Note that the actual number of splits may be slightly greater or lesser than the number specified here, depending on the actual cluster topology and token ownership. Also, it is not possible to generate fewer splits than the total number of primary token ranges in the cluster, so the actual number of splits is always equal to or greater than that number. Set this to higher values if you experience timeouts when reading from the database, specially if paging is disabled. This setting should also be greater than `engine.maxConcurrentQueries`. The special syntax `NC` can be used to specify a number that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 splits.

Default: **"8C"**.

#### -to,<br />--schema.to<br />--dsbulk.schema.to _&lt;string&gt;_

The name of the edge's outgoing vertex label, for loading or unloading graph data. This option can only be used for modern graphs created with the Native engine (DSE 6.8+). This option is mandatory when `edge` is specified; ignored otherwise. Vertex labels should not be quoted and are case-sensitive. `MyVertex` will match a label named `MyVertex` but not `myvertex`.

Default: **null**.

#### -v,<br />--schema.vertex<br />--dsbulk.schema.vertex _&lt;string&gt;_

Vertex label used for loading or unloading graph data. This option can only be used for modern graphs created with the Native engine (DSE 6.8+). The vertex label must correspond to an existing table created with the `WITH VERTEX LABEL` option. Vertex labels should not be quoted and are case-sensitive. `MyVertex` will match a label named `MyVertex` but not `myvertex`. Either `table`, `vertex` or `edge` is required if `query` is not specified.

Default: **null**.

<a name="batch"></a>
## Batch Settings

Batch-specific settings.

These settings control how the workflow engine groups together statements before writing them.

Only applicable for loading.

#### --batch.bufferSize<br />--dsbulk.batch.bufferSize _&lt;number&gt;_

The buffer size to use for flushing batched statements. Should be set to a multiple of `maxBatchStatements`, e.g. 2 or 4 times that value; higher values consume more memory and usually do not incur in any noticeable performance gain. When set to a value lesser than or equal to zero, the buffer size is implicitly set to 4 times `maxBatchStatments`.

Default: **-1**.

#### --batch.maxBatchSize<br />--dsbulk.batch.maxBatchSize _&lt;number&gt;_

**DEPRECATED**. Use `maxBatchStatements` instead.

Default: **null**.

#### --batch.maxBatchStatements<br />--dsbulk.batch.maxBatchStatements _&lt;number&gt;_

The maximum number of statements that a batch can contain. The ideal value depends on two factors:
- The data being loaded: the larger the data, the smaller the batches should be.
- The batch mode: when `PARTITION_KEY` is used, larger batches are acceptable, whereas when `REPLICA_SET` is used, smaller batches usually perform better. Also, when using `REPLICA_SET`, it is preferrable to keep this number below the threshold configured server-side for the setting `unlogged_batch_across_partitions_warn_threshold` (the default is 10); failing to do so is likely to trigger query warnings (see `log.maxQueryWarnings` for more information).
When set to a value lesser than or equal to zero, the maximum number of statements is considered unlimited. At least one of `maxBatchStatements` or `maxSizeInBytes` must be set to a positive value when batching is enabled.

Default: **32**.

#### --batch.maxSizeInBytes<br />--dsbulk.batch.maxSizeInBytes _&lt;number&gt;_

The maximum data size that a batch can hold. This is the number of bytes required to encode all the data to be persisted, without counting the overhead generated by the native protocol (headers, frames, etc.). The value specified here should be lesser than or equal to the value that has been configured server-side for the option `batch_size_fail_threshold_in_kb` in cassandra.yaml, but note that the heuristic used to compute data sizes is not 100% accurate and sometimes underestimates the actual size. See the documentation for the [cassandra.yaml configuration file](https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/config/configCassandra_yaml.html#configCassandra_yaml__advProps) for more information. When set to a value lesser than or equal to zero, the maximum data size is considered unlimited. At least one of `maxBatchStatements` or `maxSizeInBytes` must be set to a positive value when batching is enabled.

Default: **-1**.

#### --batch.mode<br />--dsbulk.batch.mode _&lt;string&gt;_

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

#### --codec.binary<br />--dsbulk.codec.binary _&lt;string&gt;_

Strategy to use when converting binary data to strings. Only applicable when unloading columns of CQL type `blob`, or columns of geometry types, if the value of `codec.geo` is `WKB`; and only if the connector in use requires stringification. Valid values are:

- BASE64: Encode the binary data into a Base-64 string. This is the default strategy.
- HEX: Encode the binary data as CQL blob literals. CQL blob literals follow the general syntax: `0[xX][0-9a-fA-F]+`, that is, `0x` followed by hexadecimal characters, for example: `0xcafebabe`. This format produces lengthier strings than BASE64, but is also the only format compatible with CQLSH.

Default: **"BASE64"**.

#### --codec.booleanNumbers<br />--dsbulk.codec.booleanNumbers _&lt;list&lt;number&gt;&gt;_

Set how true and false representations of numbers are interpreted. The representation is of the form `true_value,false_value`. The mapping is reciprocal, so that numbers are mapping to Boolean and vice versa. All numbers unspecified in this setting are rejected.

Default: **[1,0]**.

#### --codec.booleanStrings<br />--dsbulk.codec.booleanStrings _&lt;list&lt;string&gt;&gt;_

Specify how true and false representations can be used by dsbulk. Each representation is of the form `true_value:false_value`, case-insensitive. For loading, all representations are honored: when a record field value exactly matches one of the specified strings, the value is replaced with `true` of `false` before writing to the database. For unloading, this setting is only applicable for string-based connectors, such as the CSV connector: the first representation will be used to format booleans before they are written out, and all others are ignored.

Default: **["1:0","Y:N","T:F","YES:NO","TRUE:FALSE"]**.

#### --codec.date<br />--dsbulk.codec.date _&lt;string&gt;_

The temporal pattern to use for `String` to CQL `date` conversion. Valid choices:

- A date-time pattern such as `yyyy-MM-dd`.
- A pre-defined formatter such as `ISO_LOCAL_DATE`. Any public static field in `java.time.format.DateTimeFormatter` can be used.
- The special formatter `UNITS_SINCE_EPOCH`, which is a special parser that reads and writes local dates as numbers representing time units since a given epoch; the unit and the epoch to use can be specified with `codec.unit` and `codec.timestamp`.

For more information on patterns and pre-defined formatters, see [Patterns for Formatting and Parsing](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns) in Oracle Java documentation.

For more information about CQL date, time and timestamp literals, see [Date, time, and timestamp format](https://docs.datastax.com/en/dse/6.0/cql/cql/cql_reference/refDateTimeFormats.html?hl=timestamp).

Default: **"ISO_LOCAL_DATE"**.

#### --codec.epoch<br />--dsbulk.codec.epoch _&lt;string&gt;_

This setting is used in the following situations:

- When the target column is of CQL `timestamp` type, or when loading to a `USING TIMESTAMP` clause, or when unloading from a `writetime()` function call, and if `codec.timestamp` is set to `UNITS_SINCE_EPOCH`, then the epoch specified here determines the relative point in time to use to convert numeric data to and from temporals. For example, if the input is 123 and the epoch specified here is `2000-01-01T00:00:00Z`, then the input will be interpreted as N `codec.unit`s since January 1st 2000.
- When loading, and the target CQL type is numeric, but the input is alphanumeric and represents a temporal literal, the time unit specified here will be used to convert the parsed temporal into a numeric value. For example, if the input is `2018-12-10T19:32:45Z` and the epoch specified here is `2000-01-01T00:00:00Z`, then the parsed timestamp will be converted to N `codec.unit`s since January 1st 2000.
- When parsing temporal literals, if the input does not contain a date part, then the date part of the instant specified here will be used instead. For example, if the input is `19:32:45` and the epoch specified here is `2000-01-01T00:00:00Z`, then the input will be interpreted `2000-01-01T19:32:45Z`.

The value must be expressed in [`ISO_ZONED_DATE_TIME`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_ZONED_DATE_TIME) format.

Default: **"1970-01-01T00:00:00Z"**.

#### --codec.formatNumbers<br />--dsbulk.codec.formatNumbers _&lt;boolean&gt;_

Whether or not to use the `codec.number` pattern to format numeric output. When set to `true`, the numeric pattern defined by `codec.number` will be applied. This allows for nicely-formatted output, but may result in rounding (see `codec.roundingStrategy`), or alteration of the original decimal's scale. When set to `false`, numbers will be stringified using the `toString()` method, and will never result in rounding or scale alteration. Only applicable when unloading, and only if the connector in use requires stringification, because the connector, such as the CSV connector, does not handle raw numeric data; ignored otherwise.

Default: **false**.

#### --codec.geo<br />--dsbulk.codec.geo _&lt;string&gt;_

Strategy to use when converting geometry types to strings. Geometry types are only available in DataStax Enterprise (DSE) 5.0 or higher. Only applicable when unloading columns of CQL type `Point`, `LineString` or `Polygon`, and only if the connector in use requires stringification. Valid values are:

- WKT: Encode the data in Well-known text format. This is the default strategy.
- WKB: Encode the data in Well-known binary format. The actual encoding will depend on the value chosen for the `codec.binary` setting (HEX or BASE64).
- JSON: Encode the data in GeoJson format.

Default: **"WKT"**.

#### -locale,<br />--codec.locale<br />--dsbulk.codec.locale _&lt;string&gt;_

The locale to use for locale-sensitive conversions.

Default: **"en_US"**.

#### -nullStrings,<br />--codec.nullStrings<br />--dsbulk.codec.nullStrings _&lt;list&gt;_

Comma-separated list of case-sensitive strings that should be mapped to `null`. For loading, when a record field value exactly matches one of the specified strings, the value is replaced with `null` before writing to the database. For unloading, this setting is only applicable for string-based connectors, such as the CSV connector: the first string specified will be used to change a row cell containing `null` to the specified string when written out.

For example, setting this to `["NULL"]` will cause a field containing the word `NULL` to be mapped to `null` while loading, and a column containing `null` to be converted to the word `NULL` while unloading.

The default value is `[]` (no strings are mapped to `null`). In the default mode, DSBulk behaves as follows:
* When loading, if the target CQL type is textual (i.e. text, varchar or ascii), the original field value is left untouched; for other types, if the value is an empty string, it is converted to `null`.
* When unloading, `null` values are left untouched.

Note that, regardless of this setting, DSBulk will always convert empty strings to `null` if the target CQL type is not textual when loading (i.e. not text, varchar or ascii).

This setting is applied before `schema.nullToUnset`, hence any `null` produced by a null-string can still be left unset if required.

Default: **[]**.

#### --codec.number<br />--dsbulk.codec.number _&lt;string&gt;_

The `DecimalFormat` pattern to use for conversions between `String` and CQL numeric types.

See [java.text.DecimalFormat](https://docs.oracle.com/javase/8/docs/api/java/text/DecimalFormat.html) for details about the pattern syntax to use.

Most inputs are recognized: optional localized thousands separator, localized decimal separator, or optional exponent. Using locale `en_US`, `1234`, `1,234`, `1234.5678`, `1,234.5678` and `1,234.5678E2` are all valid. For unloading and formatting, rounding may occur and cause precision loss. See `codec.formatNumbers` and `codec.roundingStrategy`.

Default: **"#,###.##"**.

#### --codec.overflowStrategy<br />--dsbulk.codec.overflowStrategy _&lt;string&gt;_

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

#### --codec.roundingStrategy<br />--dsbulk.codec.roundingStrategy _&lt;string&gt;_

The rounding strategy to use for conversions from CQL numeric types to `String`.

Valid choices: any `java.math.RoundingMode` enum constant name, including: `CEILING`, `FLOOR`, `UP`, `DOWN`, `HALF_UP`, `HALF_EVEN`, `HALF_DOWN`, and `UNNECESSARY`. The precision used when rounding is inferred from the numeric pattern declared under `codec.number`. For example, the default `codec.number` (`#,###.##`) has a rounding precision of 2, and the number 123.456 would be rounded to 123.46 if `roundingStrategy` was set to `UP`. The default value will result in infinite precision, and ignore the `codec.number` setting.

Only applicable when unloading, if `codec.formatNumbers` is true and if the connector in use requires stringification, because the connector, such as the CSV connector, does not handle raw numeric data; ignored otherwise.

Default: **"UNNECESSARY"**.

#### --codec.time<br />--dsbulk.codec.time _&lt;string&gt;_

The temporal pattern to use for `String` to CQL `time` conversion. Valid choices:

- A date-time pattern, such as `HH:mm:ss`.
- A pre-defined formatter, such as `ISO_LOCAL_TIME`. Any public static field in `java.time.format.DateTimeFormatter` can be used.
- The special formatter `UNITS_SINCE_EPOCH`, which is a special parser that reads and writes local times as numbers representing time units since a given epoch; the unit and the epoch to use can be specified with `codec.unit` and `codec.timestamp`.

For more information on patterns and pre-defined formatters, see [Patterns for formatting and Parsing](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns) in Oracle Java documentation.

For more information about CQL date, time and timestamp literals, see [Date, time, and timestamp format](https://docs.datastax.com/en/dse/6.0/cql/cql/cql_reference/refDateTimeFormats.html?hl=timestamp).

Default: **"ISO_LOCAL_TIME"**.

#### -timeZone,<br />--codec.timeZone<br />--dsbulk.codec.timeZone _&lt;string&gt;_

The time zone to use for temporal conversions. When loading, the time zone will be used to obtain a timestamp from inputs that do not convey any explicit time zone information. When unloading, the time zone will be used to format all timestamps.

Default: **"UTC"**.

#### --codec.timestamp<br />--dsbulk.codec.timestamp _&lt;string&gt;_

The temporal pattern to use for `String` to CQL `timestamp` conversion. Valid choices:

- A date-time pattern such as `yyyy-MM-dd HH:mm:ss`.
- A pre-defined formatter such as `ISO_ZONED_DATE_TIME` or `ISO_INSTANT`. Any public static field in `java.time.format.DateTimeFormatter` can be used.
- The special formatter `CQL_TIMESTAMP`, which is a special parser that accepts all valid CQL literal formats for the `timestamp` type.
- The special formatter `UNITS_SINCE_EPOCH`, which is a special parser that reads and writes timestamps as numbers representing time units since a given epoch; the unit and the epoch to use can be specified with `codec.unit` and `codec.timestamp`.

For more information on patterns and pre-defined formatters, see [Patterns for Formatting and Parsing](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns) in Oracle Java documentation.

For more information about CQL date, time and timestamp literals, see [Date, time, and timestamp format](https://docs.datastax.com/en/dse/6.0/cql/cql/cql_reference/refDateTimeFormats.html?hl=timestamp).

The default value is the special `CQL_TIMESTAMP` value. When parsing, this format recognizes all CQL temporal literals; if the input is a local date or date/time, the timestamp is resolved using the time zone specified under `timeZone`. When formatting, this format uses the `ISO_OFFSET_DATE_TIME` pattern, which is compliant with both CQL and ISO-8601.

Default: **"CQL_TIMESTAMP"**.

#### --codec.unit<br />--dsbulk.codec.unit _&lt;string&gt;_

This setting is used in the following situations:

- When the target column is of CQL `timestamp` type, or when loading data through a `USING TIMESTAMP` clause, or when unloading data from a `writetime()` function call, and if `codec.timestamp` is set to `UNITS_SINCE_EPOCH`, then the time unit specified here is used to convert numeric data to and from temporals. For example, if the input is 123 and the time unit specified here is SECONDS, then the input will be interpreted as 123 seconds since `codec.epoch`.
- When loading, and the target CQL type is numeric, but the input is alphanumeric and represents a temporal literal, the time unit specified here will be used to convert the parsed temporal into a numeric value. For example, if the input is `2018-12-10T19:32:45Z` and the time unit specified here is SECONDS, then the parsed temporal will be converted into seconds since `codec.epoch`.

All `TimeUnit` enum constants are valid choices.

Default: **"MILLISECONDS"**.

#### --codec.uuidStrategy<br />--dsbulk.codec.uuidStrategy _&lt;string&gt;_

Strategy to use when generating time-based (version 1) UUIDs from timestamps. Clock sequence and node ID parts of generated UUIDs are determined on a best-effort basis and are not fully compliant with RFC 4122. Valid values are:

- RANDOM: Generates UUIDs using a random number in lieu of the local clock sequence and node ID. This strategy will ensure that the generated UUIDs are unique, even if the original timestamps are not guaranteed to be unique.
- FIXED: Preferred strategy if original timestamps are guaranteed unique, since it is faster. Generates UUIDs using a fixed local clock sequence and node ID.
- MIN: Generates the smallest possible type 1 UUID for a given timestamp. Warning: this strategy doesn't guarantee uniquely generated UUIDs and should be used with caution.
- MAX: Generates the biggest possible type 1 UUID for a given timestamp. Warning: this strategy doesn't guarantee uniquely generated UUIDs and should be used with caution.

Default: **"RANDOM"**.

<a name="engine"></a>
## Engine Settings

Engine-specific settings. Engine settings control how workflows are configured, and notably, what is their execution ID, whether they should run in Dry-run mode, and the desired amount of concurrency.

#### -dryRun,<br />--engine.dryRun<br />--dsbulk.engine.dryRun _&lt;boolean&gt;_

Enable or disable dry-run mode, a test mode that runs the command but does not load data. Not applicable for unloading nor counting.

Default: **false**.

#### -maxConcurrentQueries,<br />--engine.maxConcurrentQueries<br />--dsbulk.engine.maxConcurrentQueries _&lt;string&gt;_

The maximum number of concurrent queries that should be carried in parallel.

This acts as a safeguard to prevent more queries executing in parallel than the cluster can handle, or to regulate throughput when latencies get too high. Batch statements count as one query.

When using continuous paging, also make sure to set this number to a value equal to or lesser than the number of nodes in the local datacenter multiplied by the value configured server-side for `continuous_paging.max_concurrent_sessions` in the cassandra.yaml configuration file (60 by default); otherwise some requests might be rejected.

The special syntax `NC` can be used to specify a number that is a multiple of the number of available cores, e.g. if the number of cores is 8, then 0.5C = 0.5 * 8 = 4 concurrent queries.

The default value is 'AUTO'; with this special value, DSBulk will optimize the number of concurrent queries according to the number of available cores, and the operation being executed. The actual value usually ranges from the number of cores to eight times that number.

Default: **"AUTO"**.

#### --engine.dataSizeSamplingEnabled<br />--dsbulk.engine.dataSizeSamplingEnabled _&lt;boolean&gt;_

Specify whether DSBulk should use data size sampling to optimize its execution engine. Only applicable for loading, ignored otherwise.

Data size sampling is done by reading a few records from the connector; in this case, the connector will be invoked twice: once to sample the data size, then again to read the entire data. This is only possible if the data source can be rewinded and read again from the beginning. If your data source does not support this – for example, because it can only be read once – then you should set this option to false.

Note that when loading from standard input, DSBulk will never perform data size sampling, regardless of the value set here.

The default value is 'true', meaning that data size sampling is enabled.

Default: **true**.

#### --engine.executionId<br />--dsbulk.engine.executionId _&lt;string&gt;_

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

Executor-specific settings. Executor settings control how the DataStax Java driver is used by DSBulk, and notably, the desired amount of driver-level concurrency and throughput. These settings are for advanced users.

#### --executor.continuousPaging.enabled<br />--dsbulk.executor.continuousPaging.enabled _&lt;boolean&gt;_

Enable or disable continuous paging. If the target cluster does not support continuous paging or if `driver.query.consistency` is not `ONE` or `LOCAL_ONE`, traditional paging will be used regardless of this setting.

Default: **true**.

#### --executor.continuousPaging.maxConcurrentQueries<br />--dsbulk.executor.continuousPaging.maxConcurrentQueries _&lt;number&gt;_

The maximum number of concurrent continuous paging queries that should be carried in parallel. Set this number to a value equal to or lesser than the value configured server-side for `continuous_paging.max_concurrent_sessions` in the cassandra.yaml configuration file (60 by default); otherwise some requests might be rejected. Settting this option to any negative value or zero will disable it.

**DEPRECATED**: use `engine.maxConcurrentQueries` instead.

Default: **60**.

#### --executor.continuousPaging.maxPages<br />--dsbulk.executor.continuousPaging.maxPages _&lt;number&gt;_

The maximum number of pages to retrieve. Setting this value to zero retrieves all pages available.

**DEPRECATED**. Use `datastax-java-driver.advanced.continuous-paging.max-pages` instead.

Default: **0**.

#### --executor.continuousPaging.maxPagesPerSecond<br />--dsbulk.executor.continuousPaging.maxPagesPerSecond _&lt;number&gt;_

The maximum number of pages per second. Setting this value to zero indicates no limit.

**DEPRECATED**. Use `datastax-java-driver.advanced.continuous-paging.max-pages-per-second` instead.

Default: **0**.

#### --executor.continuousPaging.pageSize<br />--dsbulk.executor.continuousPaging.pageSize _&lt;number&gt;_

The size of the page. The unit to use is determined by the `pageUnit` setting. The ideal page size depends on the size of the rows being unloaded: larger page sizes may have a positive impact on throughput for small rows, and vice versa.

**DEPRECATED**. Use `datastax-java-driver.advanced.continuous-paging.page-size` instead.

Default: **5000**.

#### --executor.continuousPaging.pageUnit<br />--dsbulk.executor.continuousPaging.pageUnit _&lt;string&gt;_

The unit to use for the `pageSize` setting. Possible values are: `ROWS`, `BYTES`.

**DEPRECATED**. Use `datastax-java-driver.advanced.continuous-paging.page-size-in-bytes` instead.

Default: **"ROWS"**.

#### --executor.maxInFlight<br />--dsbulk.executor.maxInFlight _&lt;number&gt;_

The maximum number of "in-flight" queries, or maximum number of concurrent requests waiting for a response from the server. When writing to the database, batch statements count as one request. When reading from the database, each request for the next pages count as one request.

This acts as a safeguard to prevent overloading the cluster. Reduce this value when the throughput for reads and writes cannot match the throughput of connectors, and latencies get too high; this is usually a sign that the workflow engine is not well calibrated and will eventually run out of memory, or some queries will timeout.

This setting applies a "soft" limit to the gloabl throughput, without capping it at a fixed value. If you need a fixed maximum throughput, you should use `maxPerSecond` instead.

Note that this setting is implemented by a semaphore and may block application threads if there are too many in-flight requests.

Setting this option to any negative value or zero will disable it.

Default: **-1**.

#### --executor.maxPerSecond<br />--dsbulk.executor.maxPerSecond _&lt;number&gt;_

The maximum number of concurrent operations per second. When writing to the database, this means the maximum number of writes per second (batch statements are counted by the number of statements included); when reading from the database, this means the maximum number of rows per second.

This acts as a safeguard to prevent overloading the cluster. Reduce this value when the throughput for reads and writes cannot match the throughput of connectors, and latencies get too high; this is usually a sign that the workflow engine is not well calibrated and will eventually run out of memory, or some queries will timeout.

This setting applies a "hard" limit to the gloabl throughput, capping it at a fixed value. If you need a a soft throughput limit, you should use `maxInFlight` instead.

Note that this setting is implemented by a semaphore and may block application threads if there are too many in-flight requests.

Setting this option to any negative value or zero will disable it.

Default: **-1**.

<a name="log"></a>
## Log Settings

Log and error management settings.

#### -maxErrors,<br />--log.maxErrors<br />--dsbulk.log.maxErrors _&lt;number&gt;_

The maximum number of errors to tolerate before aborting the entire operation. This can be expressed either as an absolute number of errors - in which case, set this to an integer greater than or equal to zero; or as a percentage of total rows processed so far - in which case, set this to a string of the form `N%`, where `N` is a decimal number between 0 and 100 exclusive (e.g. "20%"). Setting this value to any negative integer disables this feature (not recommended).

Default: **100**.

#### -logDir,<br />--log.directory<br />--dsbulk.log.directory _&lt;string&gt;_

The writable directory where all log files will be stored; if the directory specified does not exist, it will be created. URLs are not acceptable (not even `file:/` URLs). Log files for a specific run, or execution, will be located in a sub-directory under the specified directory. Each execution generates a sub-directory identified by an "execution ID". See `engine.executionId` for more information about execution IDs. Relative paths will be resolved against the current working directory. Also, for convenience, if the path begins with a tilde (`~`), that symbol will be expanded to the current user's home directory.

Default: **"./logs"**.

#### -verbosity,<br />--log.verbosity<br />--dsbulk.log.verbosity _&lt;number&gt;_

The desired level of verbosity. Valid values are:

- 0 (quiet): DSBulk will only log WARN and ERROR messages.
- 1 (normal): DSBulk will log INFO, WARN and ERROR messages.
- 2 (verbose) DSBulk will log DEBUG, INFO, WARN and ERROR messages.

Default: **1**.

#### --log.ansiMode<br />--dsbulk.log.ansiMode _&lt;string&gt;_

Whether or not to use ANSI colors and other escape sequences in log messages printed to the console. Valid values are:

- `normal`: this is the default option. DSBulk will only use ANSI when the terminal is:
  - compatible with ANSI escape sequences; all common terminals on *nix and BSD systems, including MacOS, are ANSI-compatible, and some popular terminals for Windows (Mintty, MinGW);
  - a standard Windows DOS command prompt (ANSI sequences are translated on the fly).
- `force`: DSBulk will use ANSI, even if the terminal has not been detected as ANSI-compatible.
- `disable`: DSBulk will not use ANSI.

Note to Windows users: ANSI support on Windows works best when the Microsoft Visual C++ 2008 SP1 Redistributable Package is installed; you can download it [here](https://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=5582).

Default: **"normal"**.

#### --log.maxQueryWarnings<br />--dsbulk.log.maxQueryWarnings _&lt;number&gt;_

The maximum number of query warnings to log before muting them. Query warnings are sent by the server (for example, if the number of statements in a batch is greater than the warning threshold configured on the server). They are useful to diagnose suboptimal configurations but tend to be too invasive, which is why DSBulk by default will only log the 50 first query warnings; any subsequent warnings will be muted and won't be logged at all. Setting this value to any negative integer disables this feature (not recommended).

Default: **50**.

#### --log.row.maxResultSetValueLength<br />--dsbulk.log.row.maxResultSetValueLength _&lt;number&gt;_

The maximum length for a result set value. Result set values longer than this value will be truncated.

Setting this value to `-1` makes the maximum length for a result set value unlimited (not recommended).

Default: **50**.

#### --log.row.maxResultSetValues<br />--dsbulk.log.row.maxResultSetValues _&lt;number&gt;_

The maximum number of result set values to print. If the row has more result set values than this limit, the exceeding values will not be printed.

Setting this value to `-1` makes the maximum number of result set values unlimited (not recommended).

Default: **50**.

#### --log.sources<br />--dsbulk.log.sources _&lt;boolean&gt;_

Whether to print record sources in debug files. When set to true (the default), debug files will contain, for each record that failed to be processed, its original source, such as the text line that the record was parsed from.

Furthermore, when loading, enabling this option also enables the creation of so-called "bad files", that is, files containing the original lines that could not be inserted; these files could then be used as the data source of a subsequent load operation that would load only the failed records.

This feature is useful to locate failed records more easily and diagnose processing failures – especially if the original data source is a remote one, such as an FTP or HTTP URL.

But for this feature to be possible, record sources must be kept in memory until the record is fully processed. For large record sizes (over 1 megabyte per record), retaining record sources in memory could put a high pressure on the JVM heap, thus exposing the operation to out-of-memory errors. This phenomenon is exacerbated when batching is enabled. If you are experiencing such errors, consider disabling this option.

Note that, regardless of the value of this option, DSBulk will always print the record's *resource* – that is, the file name or the database table where it came from – and the record's *position* – that is, the ordinal position of the record inside the resource, when available (for example, this could be the line number in a CSV file).

Default: **true**.

#### --log.stmt.level<br />--dsbulk.log.stmt.level _&lt;string&gt;_

The desired log level. Valid values are:

- ABRIDGED: Print only basic information in summarized form.
- NORMAL: Print basic information in summarized form, and the statement's query string, if available. For batch statements, this verbosity level also prints information about the batch's inner statements.
- EXTENDED: Print full information, including the statement's query string, if available, and the statement's bound values, if available. For batch statements, this verbosity level also prints all information available about the batch's inner statements.

Default: **"EXTENDED"**.

#### --log.stmt.maxBoundValueLength<br />--dsbulk.log.stmt.maxBoundValueLength _&lt;number&gt;_

The maximum length for a bound value. Bound values longer than this value will be truncated.

Setting this value to `-1` makes the maximum length for a bound value unlimited (not recommended).

Default: **50**.

#### --log.stmt.maxBoundValues<br />--dsbulk.log.stmt.maxBoundValues _&lt;number&gt;_

The maximum number of bound values to print. If the statement has more bound values than this limit, the exceeding values will not be printed.

Setting this value to `-1` makes the maximum number of bound values unlimited (not recommended).

Default: **50**.

#### --log.stmt.maxInnerStatements<br />--dsbulk.log.stmt.maxInnerStatements _&lt;number&gt;_

The maximum number of inner statements to print for a batch statement. Only applicable for batch statements, ignored otherwise. If the batch statement has more children than this value, the exceeding child statements will not be printed.

Setting this value to `-1` disables this feature (not recommended).

Default: **10**.

#### --log.stmt.maxQueryStringLength<br />--dsbulk.log.stmt.maxQueryStringLength _&lt;number&gt;_

The maximum length for a query string. Query strings longer than this value will be truncated.

Setting this value to `-1` disables this feature (not recommended).

Default: **500**.

<a name="monitoring"></a>
## Monitoring Settings

Monitoring-specific settings.

#### -reportRate,<br />--monitoring.reportRate<br />--dsbulk.monitoring.reportRate _&lt;string&gt;_

The report interval. DSBulk will print useful metrics about the ongoing operation at this rate; for example, if this value is set to 10 seconds, then DSBulk will print metrics every ten seconds. Valid values: any value specified in [HOCON duration syntax](https://github.com/lightbend/config/blob/master/HOCON.md#duration-format), but durations lesser than one second will be rounded up to 1 second.

Default: **"5 seconds"**.

#### --monitoring.console<br />--dsbulk.monitoring.console _&lt;boolean&gt;_

Enable or disable console reporting. If enabled, DSBulk will print useful metrics about the ongoing operation to standard error; the metrics will be refreshed at `reportRate`. Displayed information includes: total records, failed records, throughput, latency, and if available, average batch size. Note that when `log.verbosity` is set to quiet (0), DSBulk will disable the console reporter regardless of the value specified here. The default is true (print ongoing metrics to the console).

Default: **true**.

#### --monitoring.csv<br />--dsbulk.monitoring.csv _&lt;boolean&gt;_

Enable or disable CSV reporting. If enabled, CSV files containing metrics will be generated in the designated log directory.

Default: **false**.

#### --monitoring.durationUnit<br />--dsbulk.monitoring.durationUnit _&lt;string&gt;_

The time unit used when printing latency durations. For example, if this unit is MILLISECONDS, then the latencies will be displayed in milliseconds. Valid values: all `TimeUnit` enum constants.

Default: **"MILLISECONDS"**.

#### --monitoring.expectedReads<br />--dsbulk.monitoring.expectedReads _&lt;number&gt;_

The expected total number of reads. Optional, but if set, the console reporter will also print the overall achievement percentage. Setting this value to `-1` disables this feature.

Default: **-1**.

#### --monitoring.expectedWrites<br />--dsbulk.monitoring.expectedWrites _&lt;number&gt;_

The expected total number of writes. Optional, but if set, the console reporter will also print the overall achievement percentage. Setting this value to `-1` disables this feature.

Default: **-1**.

#### -jmx,<br />--monitoring.jmx<br />--dsbulk.monitoring.jmx _&lt;boolean&gt;_

Enable or disable JMX reporting. Note that to enable remote JMX reporting, several properties must also be set in the JVM during launch. This is accomplished via the `DSBULK_JAVA_OPTS` environment variable.

Default: **true**.

#### --monitoring.rateUnit<br />--dsbulk.monitoring.rateUnit _&lt;string&gt;_

The time unit used when printing throughput rates. For example, if this unit is SECONDS, then the throughput will be displayed in rows per second. Valid values: all `TimeUnit` enum constants.

Default: **"SECONDS"**.

#### --monitoring.trackBytes<br />--dsbulk.monitoring.trackBytes _&lt;boolean&gt;_

Whether or not to track the throughput in bytes. When enabled, DSBulk will track and display the number of bytes sent or received per second. While useful to evaluate how much data is actually being transferred, computing such metrics is CPU-intensive and may slow down the operation. This is why it is disabled by default. Also note that the heuristic used to compute data sizes is not 100% accurate and sometimes underestimates the actual size.

Default: **false**.

<a name="runner"></a>
## Runner Settings

Runner-specific settings. Runner settings control how DSBulk parses command lines and reads its configuration.

#### --runner.promptForPasswords<br />--dsbulk.runner.promptForPasswords _&lt;boolean&gt;_

Whether to prompt for passwords when they are missing from configuration files. When this option is true (the default value), if a login or username is present in the configuration, but not its corresponding password, DSBulk will prompt for it.

Prompting from passwords require interactive shells; if the standard input is not connected to a terminal, no passwords will be prompted, even if this option is true. You should only disable this feature if DSBulk mistankenly assumes that it is running in an interactive shell.

Default: **true**.

<a name="stats"></a>
## Stats Settings

Settings applicable for the count workflow, ignored otherwise.

#### -stats,<br />--stats.modes<br />--dsbulk.stats.modes _&lt;list&lt;string&gt;&gt;_

Which kind(s) of statistics to compute. Only applicaple for the count workflow, ignored otherwise. Possible values are:
* `global`: count the total number of rows in the table.
* `ranges`: count the total number of rows per token range in the table.
* `hosts`: count the total number of rows per hosts in the table.
* `partitions`: count the total number of rows in the N biggest partitions in the table. When using this mode, you can chose how many partitions to track with the `numPartitions` setting.

Default: **["global"]**.

#### -partitions,<br />--stats.numPartitions<br />--dsbulk.stats.numPartitions _&lt;number&gt;_

The number of distinct partitions to count rows for. Only applicaple for the count workflow when `stats.modes` contains `partitions`, ignored otherwise.

Default: **10**.

<a name="datastax-java-driver"></a>
## Driver Settings

The settings below are just a subset of all the configurable options of the driver, and provide an optimal driver configuration for DSBulk for most use cases.

See the [Java Driver configuration reference](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration) for instructions on how to configure the driver properly.

Note: driver settings always start with prefix `datastax-java-driver`; on the command line only, it is possible to abbreviate this prefix to just `driver`, as shown below.

#### --driver.basic.session-name<br />--datastax-java-driver.basic.session-name _&lt;string&gt;_

The driver session name. DSBulk simply calls it "driver". The session name is printed by all driver log messages, between square brackets.

Default: **"driver"**.

#### -h,<br />--driver.basic.contact-points<br />--datastax-java-driver.basic.contact-points _&lt;list&lt;string&gt;&gt;_

The contact points to use for the initial connection to the cluster.

These are addresses of Cassandra nodes that the driver uses to discover the cluster topology. Only one contact point is required (the driver will retrieve the address of the other nodes automatically), but it is usually a good idea to provide more than one contact point, because if that single contact point is unavailable, the driver cannot initialize itself correctly.

This must be a list of strings with each contact point specified as "host" or "host:port". If the specified host doesn't have a port, the default port specified in `basic.default-port` will be used. Note that Cassandra 3 and below and DSE 6.7 and below require all nodes in a cluster to share the same port (see CASSANDRA-7544).

Valid examples of contact points are:
- IPv4 addresses with ports: `[ "192.168.0.1:9042", "192.168.0.2:9042" ]`
- IPv4 addresses without ports: `[ "192.168.0.1", "192.168.0.2" ]`
- IPv6 addresses with ports: `[ "fe80:0:0:0:f861:3eff:fe1d:9d7b:9042", "fe80:0:0:f861:3eff:fe1d:9d7b:9044:9042" ]`
- IPv6 addresses without ports: `[ "fe80:0:0:0:f861:3eff:fe1d:9d7b", "fe80:0:0:f861:3eff:fe1d:9d7b:9044" ]`
- Host names with ports: `[ "host1.com:9042", "host2.com:9042" ]`
- Host names without ports: `[ "host1.com", "host2.com:" ]`

If the host is a DNS name that resolves to multiple A-records, all the corresponding addresses will be used. Avoid using "localhost" as the host name (since it resolves to both IPv4 and IPv6 addresses on some platforms).

The heuristic to determine whether or not a contact point is in the form "host" or "host:port" is not 100% accurate for some IPv6 addresses; you should avoid ambiguous IPv6 addresses such as `fe80::f861:3eff:fe1d:1234`, because such a string can be seen either as a combination of IP `fe80::f861:3eff:fe1d` with port 1234, or as IP `fe80::f861:3eff:fe1d:1234` without port. In such cases, DSBulk will not change the contact point. This issue can be avoided by providing IPv6 addresses in full form, e.g. if instead of `fe80::f861:3eff:fe1d:1234` you provide `fe80:0:0:0:0:f861:3eff:fe1d:1234`, then the string is unequivocally parsed as IP `fe80:0:0:0:0:f861:3eff:fe1d` with port 1234.

Note: on Cloud deployments, DSBulk automatically sets this option to an empty list, as contact points are not allowed to be explicitly provided when connecting to DataStax Astra databases.

Default: **["127.0.0.1:9042"]**.

#### -port,<br />--driver.basic.default-port<br />--datastax-java-driver.basic.default-port _&lt;number&gt;_

The default port to use for `basic.contact-points`, when a host is specified without port. Note that Cassandra 3 and below and DSE 6.7 and below require all nodes in a cluster to share the same port (see CASSANDRA-7544). If this setting is not specified, the default port will be 9042.

Default: **9042**.

#### -b,<br />--driver.basic.cloud.secure-connect-bundle<br />--datastax-java-driver.basic.cloud.secure-connect-bundle _&lt;string&gt;_

The location of the secure bundle used to connect to a Datastax Astra database. This setting must be a path on the local filesystem or a valid URL.

Examples:

    "/path/to/bundle.zip"          # path on *Nix systems
    "./path/to/bundle.zip"         # path on *Nix systems, relative to workding directory
    "~/path/to/bundle.zip"         # path on *Nix systems, relative to home directory
    "C:\\path\\to\\bundle.zip"     # path on Windows systems,
                                   # note that you need to escape backslashes in HOCON
    "file:/a/path/to/bundle.zip"   # URL with file protocol
    "http://host.com/bundle.zip"   # URL with HTTP protocol

Note: if you set this to a non-null value, DSBulk assumes that you are connecting to an DataStax Astra database; in this case, you should not set any of the following settings because they are not compatible with Cloud deployments:

- `datastax-java-driver.basic.contact-points`
- `datastax-java-driver.basic.request.consistency`
- `datastax-java-driver.advanced.ssl-engine-factory.*`

If you do so, a log will be emitted and the setting will be ignored.

Default: **null**.

#### --driver.basic.request.timeout<br />--datastax-java-driver.basic.request.timeout _&lt;string&gt;_

How long the driver waits for a request to complete. This is a global limit on the duration of a session.execute() call, including any internal retries the driver might do. By default, this value is set very high because DSBulk is optimized for good throughput, rather than good latencies.

Default: **"5 minutes"**.

#### -cl,<br />--driver.basic.request.consistency<br />--datastax-java-driver.basic.request.consistency _&lt;string&gt;_

The consistency level to use for all queries. Note that stronger consistency levels usually result in reduced throughput. In addition, any level higher than `ONE` will automatically disable continuous paging, which can dramatically reduce read throughput.

Valid values are: `ANY`, `LOCAL_ONE`, `ONE`, `TWO`, `THREE`, `LOCAL_QUORUM`, `QUORUM`, `EACH_QUORUM`, `ALL`.

Note: on Cloud deployments, the only accepted consistency level when writing is `LOCAL_QUORUM`. Therefore, the default value is `LOCAL_ONE`, except when loading in Cloud deployments, in which case the default is automatically changed to `LOCAL_QUORUM`.

Default: **"LOCAL_ONE"**.

#### --driver.basic.request.serial-consistency<br />--datastax-java-driver.basic.request.serial-consistency _&lt;string&gt;_

The serial consistency level. The allowed values are `SERIAL` and `LOCAL_SERIAL`.

Default: **"LOCAL_SERIAL"**.

#### --driver.basic.request.default-idempotence<br />--datastax-java-driver.basic.request.default-idempotence _&lt;boolean&gt;_

The default idempotence for all queries executed in DSBulk. Setting this to false will cause all write failures to not be retried.

Default: **true**.

#### --driver.basic.request.page-size<br />--datastax-java-driver.basic.request.page-size _&lt;number&gt;_

The page size. This controls how many rows will be retrieved simultaneously in a single network roundtrip (the goal being to avoid loading too many results in memory at the same time). If there are more results, additional requests will be used to retrieve them (either automatically if you iterate with the sync API, or explicitly with the async API's `fetchNextPage` method). If the value is 0 or negative, it will be ignored and the request will not be paged.

Default: **5000**.

#### --driver.basic.load-balancing-policy.class<br />--datastax-java-driver.basic.load-balancing-policy.class _&lt;string&gt;_

The load balancing policy class to use. If it is not qualified, the driver assumes that it resides in the package `com.datastax.oss.driver.internal.core.loadbalancing`.

DSBulk uses a special policy that infers the local datacenter from the contact points. You can also specify a custom class that implements `LoadBalancingPolicy` and has a public constructor with two arguments: the `DriverContext` and a `String` representing the profile name.

Default: **"com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy"**.

#### -dc,<br />--driver.basic.load-balancing-policy.local-datacenter<br />--datastax-java-driver.basic.load-balancing-policy.local-datacenter _&lt;string&gt;_

The datacenter that is considered "local": the default load balancing policy will only include nodes from this datacenter in its query plans. Set this to a non-null value if you want to force the local datacenter; otherwise, the `DcInferringLoadBalancingPolicy` used by default by DSBulk will infer the local datacenter from the provided contact points.

Default: **null**.

#### --driver.basic.load-balancing-policy.filter.class<br />--datastax-java-driver.basic.load-balancing-policy.filter.class _&lt;string&gt;_

An optional custom filter to include/exclude nodes. If present, it must be the fully-qualified name of a class that implements `java.util.function.Predicate<Node>`, and has a public constructor taking two arguments: a `DriverContext` instance and a String representing the current execution profile name.

The predicate's `test(Node)` method will be invoked each time the policy processes a topology or state change: if it returns false, the node will be set at distance `IGNORED` (meaning the driver won't ever connect to it), and never included in any query plan.

By default, DSBulk ships with a node filter implementation that honors the following settings:
- `datastax-java-driver.basic.load-balancing-policy.filter.allow`: a list of host names or host addresses that should be allowed.
- `datastax-java-driver.basic.load-balancing-policy.filter.deny`: a list of host names or host addresses that should be denied.

See the description of the above settings for more details.

Default: **"com.datastax.oss.dsbulk.workflow.commons.policies.lbp.SimpleNodeFilter"**.

#### -allow,<br />--driver.basic.load-balancing-policy.filter.allow<br />--datastax-java-driver.basic.load-balancing-policy.filter.allow _&lt;list&lt;string&gt;&gt;_

An optional list of host names or host addresses that should be allowed to connect. See `datastax-java-driver.basic.contact-points` for a full description of accepted formats.

This option only has effect when the setting `datastax-java-driver.basic.load-balancing-policy.filter.class` refers to DSBulk's default node filter implementation: `com.datastax.oss.dsbulk.workflow.commons.policies.lbp.SimpleNodeFilter`.

Note: this option is not compatible with DataStax Astra databases.

Default: **[]**.

#### -deny,<br />--driver.basic.load-balancing-policy.filter.deny<br />--datastax-java-driver.basic.load-balancing-policy.filter.deny _&lt;list&lt;string&gt;&gt;_

An optional list of host names or host addresses that should be denied to connect. See `datastax-java-driver.basic.contact-points` for a full description of accepted formats.

This option only has effect when the setting `datastax-java-driver.basic.load-balancing-policy.filter.class` refers to DSBulk's default node filter implementation: `com.datastax.oss.dsbulk.workflow.commons.policies.lbp.SimpleNodeFilter`.

Note: this option is not compatible with DataStax Astra databases.

Default: **[]**.

#### --driver.advanced.protocol.version<br />--datastax-java-driver.advanced.protocol.version _&lt;string&gt;_

The native protocol version to use. If this option is absent, the driver looks up the versions of the nodes at startup (by default in `system.peers.release_version`), and chooses the highest common protocol version. For example, if you have a mixed cluster with Apache Cassandra 2.1 nodes (protocol v3) and Apache Cassandra 3.0 nodes (protocol v3 and v4), then protocol v3 is chosen. If the nodes don't have a common protocol version, initialization fails. If this option is set, then the given version will be used for all connections, without any negotiation or downgrading. If any of the contact points doesn't support it, that contact point will be skipped. Once the protocol version is set, it can't change for the rest of the driver's lifetime; if an incompatible node joins the cluster later, connection will fail and the driver will force it down (i.e. never try to connect to it again).

Default: **null**.

#### --driver.advanced.protocol.compression<br />--datastax-java-driver.advanced.protocol.compression _&lt;string&gt;_

The name of the algorithm used to compress protocol frames. The possible values are: `lz4`, `snappy` or `none` to indicate no compression (this is functionally equivalent to omitting the option).

Default: **"none"**.

#### --driver.advanced.connection.connect-timeout<br />--datastax-java-driver.advanced.connection.connect-timeout _&lt;string&gt;_

The timeout to use when establishing driver connections. This timeout is for controlling how long the driver will wait for the underlying channel to actually connect to the server. This is not the time limit for completing protocol negotiations, only the time limit for establishing a channel connection.

Default: **"30 seconds"**.

#### --driver.advanced.connection.init-query-timeout<br />--datastax-java-driver.advanced.connection.init-query-timeout _&lt;string&gt;_

The timeout to use for internal queries that run as part of the initialization process, just after we open a connection. If this timeout fires, the initialization of the connection will fail. If this is the first connection ever, the driver will fail to initialize as well, otherwise it will retry the connection later.

Default: **"30 seconds"**.

#### --driver.advanced.connection.pool.local.size<br />--datastax-java-driver.advanced.connection.pool.local.size _&lt;number&gt;_

The number of connections in the pool for nodes considered as local.

Default: **8**.

#### --driver.advanced.connection.pool.remote.size<br />--datastax-java-driver.advanced.connection.pool.remote.size _&lt;number&gt;_

The number of connections in the pool for nodes considered as remote. Note that the default load balancing policy used by DSBulk never considers remote nodes, so this setting has no effect when using the default load balancing policy.

Default: **8**.

#### --driver.advanced.connection.max-requests-per-connection<br />--datastax-java-driver.advanced.connection.max-requests-per-connection _&lt;number&gt;_

The maximum number of requests that can be executed concurrently on a connection. This must be between 1 and 32768.

Default: **32768**.

#### --driver.advanced.auth-provider.class<br />--datastax-java-driver.advanced.auth-provider.class _&lt;arg&gt;_

The class of the authentication provider. If it is not qualified, the driver assumes that it resides in one of the following packages:
- `com.datastax.oss.driver.internal.core.auth`
- `com.datastax.dse.driver.internal.core.auth`

The DSE driver provides 3 implementations out of the box:
- `PlainTextAuthProvider`: uses plain-text credentials. It requires the `username` and `password` options, and optionally, an `authorization-id` (for DSE clusters only).
- `DseGssApiAuthProvider`: provides GSSAPI authentication for DSE clusters secured with `DseAuthenticator`. Read the javadocs of this authenticator for detailed instructions.

You can also specify a custom class that implements `AuthProvider` and has a public constructor with a `DriverContext` argument (to simplify this, the driver provides two abstract classes that can be extended: `PlainTextAuthProviderBase` and `DseGssApiAuthProviderBase`).

Default: **null**.

#### -u,<br />--driver.advanced.auth-provider.username<br />--datastax-java-driver.advanced.auth-provider.username _&lt;string&gt;_

The username to use to authenticate against a cluster with authentication enabled. Providers that accept this setting:

 - `PlainTextAuthProvider`


Default: **null**.

#### -p,<br />--driver.advanced.auth-provider.password<br />--datastax-java-driver.advanced.auth-provider.password _&lt;string&gt;_

The password to use to authenticate against a cluster with authentication enabled. Providers that accept this setting:

 - `PlainTextAuthProvider`


Default: **null**.

#### --driver.advanced.auth-provider.authorization-id<br />--datastax-java-driver.advanced.auth-provider.authorization-id _&lt;string&gt;_

An authorization ID allows the currently authenticated user to act as a different user (proxy authentication). Providers that accept this setting:

 - `DsePlainTextAuthProvider`
 - `DseGssApiAuthProvider`


Default: **null**.

#### --driver.advanced.ssl-engine-factory.class<br />--datastax-java-driver.advanced.ssl-engine-factory.class _&lt;string&gt;_

The class of the SSL engine factory. If it is not qualified, the driver assumes that it resides in the package `com.datastax.oss.driver.internal.core.ssl`. The driver provides a single implementation out of the box: `DefaultSslEngineFactory`, that uses the JDK's built-in SSL implementation.

You can also specify a custom class that implements `SslEngineFactory` and has a public constructor with a `DriverContext` argument.

Default: **null**.

#### --driver.advanced.ssl-engine-factory.cipher-suites<br />--datastax-java-driver.advanced.ssl-engine-factory.cipher-suites _&lt;list&lt;string&gt;&gt;_

The cipher suites to enable when creating an SSLEngine for a connection. This setting is only required when using the default SSL factory. If it is not present, the driver won't explicitly enable cipher suites on the engine, which according to the JDK documentations results in "a minimum quality of service".

Default: **null**.

#### --driver.advanced.ssl-engine-factory.hostname-validation<br />--datastax-java-driver.advanced.ssl-engine-factory.hostname-validation _&lt;boolean&gt;_

Whether or not to require validation that the hostname of the server certificate's common name matches the hostname of the server being connected to. This setting is only required when using the default SSL factory. If not set, defaults to true.

Default: **true**.

#### --driver.advanced.ssl-engine-factory.truststore-path<br />--datastax-java-driver.advanced.ssl-engine-factory.truststore-path _&lt;string&gt;_

The locations used to access truststore contents. If either truststore-path or keystore-path are specified, the driver builds an SSLContext from these files. This setting is only required when using the default SSL factory. This setting is only required when using the default SSL factory. If neither option is specified, the default SSLContext is used, which is based on system property configuration.

Default: **null**.

#### --driver.advanced.ssl-engine-factory.truststore-password<br />--datastax-java-driver.advanced.ssl-engine-factory.truststore-password _&lt;string&gt;_

The password used to access truststore contents. This setting is only required when using the default SSL factory.

Default: **null**.

#### --driver.advanced.ssl-engine-factory.keystore-path<br />--datastax-java-driver.advanced.ssl-engine-factory.keystore-path _&lt;string&gt;_

The locations used to access keystore contents. If either truststore-path or keystore-path are specified, the driver builds an SSLContext from these files. This setting is only required when using the default SSL factory. If neither option is specified, the default SSLContext is used, which is based on system property configuration.

Default: **null**.

#### --driver.advanced.ssl-engine-factory.keystore-password<br />--datastax-java-driver.advanced.ssl-engine-factory.keystore-password _&lt;string&gt;_

The password used to access keystore contents. This setting is only required when using the default SSL factory.

Default: **null**.

#### --driver.advanced.retry-policy.class<br />--datastax-java-driver.advanced.retry-policy.class _&lt;string&gt;_

The class of the retry policy. If it is not qualified, the driver assumes that it resides in the package `com.datastax.oss.driver.internal.core.retry`. DSBulk uses by default a special retry policy that opinionately retries most errors up to `max-retries` times.

You can also specify a custom class that implements `RetryPolicy` and has a public constructor with two arguments: the `DriverContext` and a `String` representing the profile name.

Default: **"com.datastax.oss.dsbulk.workflow.commons.policies.retry.MultipleRetryPolicy"**.

#### -maxRetries,<br />--driver.advanced.retry-policy.max-retries<br />--datastax-java-driver.advanced.retry-policy.max-retries _&lt;number&gt;_

How many times to retry a failed query. Only valid for use with DSBulk's default retry policy (`MultipleRetryPolicy`).

Default: **10**.

#### --driver.advanced.resolve-contact-points<br />--datastax-java-driver.advanced.resolve-contact-points _&lt;boolean&gt;_

Whether to resolve the addresses passed to `basic.contact-points`.

If this is true, addresses are created with `InetSocketAddress(String, int)`: the host name will be resolved the first time, and the driver will use the resolved IP address for all subsequent connection attempts. If this is false, addresses are created with `InetSocketAddress.createUnresolved()`: the host name will be resolved again every time the driver opens a new connection. This is useful for containerized environments where DNS records are more likely to change over time (note that the JVM and OS have their own DNS caching mechanisms, so you might need additional configuration beyond the driver).

This option only applies to the contact points specified in the configuration. It has no effect on dynamically discovered peers: the driver relies on Cassandra system tables, which expose raw IP addresses. Use a custom address translator (see `advanced.address-translator`) to convert them to unresolved addresses (if you're in a containerized environment, you probably already need address translation anyway).

Default: **true**.

#### --driver.advanced.address-translator.class<br />--datastax-java-driver.advanced.address-translator.class _&lt;string&gt;_

The class of the translator. If it is not qualified, the driver assumes that it resides in the package `com.datastax.oss.driver.internal.core.addresstranslation`.

The driver provides the following implementations out of the box:
- `PassThroughAddressTranslator`: returns all addresses unchanged

You can also specify a custom class that implements `AddressTranslator` and has a public constructor with a `DriverContext` argument.

Default: **"PassThroughAddressTranslator"**.

#### --driver.advanced.timestamp-generator.class<br />--datastax-java-driver.advanced.timestamp-generator.class _&lt;string&gt;_

The class of the microsecond timestamp generator. If it is not qualified, the driver assumes that it resides in the package `com.datastax.oss.driver.internal.core.time`.

The driver provides the following implementations out of the box:
- `AtomicTimestampGenerator`: timestamps are guaranteed to be unique across all client threads.
- `ThreadLocalTimestampGenerator`: timestamps that are guaranteed to be unique within each
  thread only.
- `ServerSideTimestampGenerator`: do not generate timestamps, let the server assign them.

You can also specify a custom class that implements `TimestampGenerator` and has a public constructor with two arguments: the `DriverContext` and a `String` representing the profile name.

Default: **"AtomicTimestampGenerator"**.

#### --driver.advanced.continuous-paging.page-size<br />--datastax-java-driver.advanced.continuous-paging.page-size _&lt;number&gt;_

The page size. The value specified here can be interpreted in number of rows or in number of bytes, depending on the unit defined with page-unit (see below). It controls how many rows (or how much data) will be retrieved simultaneously in a single network roundtrip (the goal being to avoid loading too many results in memory at the same time). If there are more results, additional requests will be used to retrieve them. The default is the same as the driver's normal request page size, i.e., 5000 (rows).

Default: **5000**.

#### --driver.advanced.continuous-paging.page-size-in-bytes<br />--datastax-java-driver.advanced.continuous-paging.page-size-in-bytes _&lt;boolean&gt;_

Whether the page-size option should be interpreted in number of rows or bytes. The default is false, i.e., the page size will be interpreted in number of rows.

Default: **false**.

#### --driver.advanced.continuous-paging.max-pages<br />--datastax-java-driver.advanced.continuous-paging.max-pages _&lt;number&gt;_

The maximum number of pages to return. The default is zero, which means retrieve all pages.

Default: **0**.

#### --driver.advanced.continuous-paging.max-pages-per-second<br />--datastax-java-driver.advanced.continuous-paging.max-pages-per-second _&lt;number&gt;_

Returns the maximum number of pages per second. The default is zero, which means no limit.

Default: **0**.

#### --driver.advanced.continuous-paging.max-enqueued-pages<br />--datastax-java-driver.advanced.continuous-paging.max-enqueued-pages _&lt;number&gt;_

The maximum number of pages that can be stored in the local queue. This value must be positive. The default is 4.

Default: **4**.

#### --driver.advanced.continuous-paging.timeout.first-page<br />--datastax-java-driver.advanced.continuous-paging.timeout.first-page _&lt;string&gt;_

How long to wait for the coordinator to send the first page.

Default: **"5 minutes"**.

#### --driver.advanced.continuous-paging.timeout.other-pages<br />--datastax-java-driver.advanced.continuous-paging.timeout.other-pages _&lt;string&gt;_

How long to wait for the coordinator to send subsequent pages.

Default: **"5 minutes"**.

#### --driver.advanced.heartbeat.interval<br />--datastax-java-driver.advanced.heartbeat.interval _&lt;string&gt;_

The heartbeat interval. If a connection stays idle for that duration (no reads), the driver sends a dummy message on it to make sure it's still alive. If not, the connection is trashed and replaced.

Default: **"1 minute"**.

#### --driver.advanced.heartbeat.timeout<br />--datastax-java-driver.advanced.heartbeat.timeout _&lt;string&gt;_

How long the driver waits for the response to a heartbeat. If this timeout fires, the heartbeat is considered failed.

Default: **"1 minute"**.

