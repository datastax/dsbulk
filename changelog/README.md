## Changelog

## 1.9.0 (in progress)

- [improvement] Upgrade driver to 4.14.0.
- [new feature] [#400](https://github.com/datastax/dsbulk/issues/400): Add ability to unwrap BATCH queries.
- [new feature] [#405](https://github.com/datastax/dsbulk/issues/405): Add support for Prometheus.

## 1.8.0

- [improvement] Upgrade driver to 4.10.0.
- [bug] Fix incorrect error message when read concurrency is < 1.
- [bug] Prevent connectors from reporting read concurrency zero when no file is available for reading.
- [bug] DAT-627: Share CodecRegistry instance between DSBulk and driver.
- [improvement] Accept Well-known Binary (WKB) input formats for Geometry types.
- [improvement] Make Json connector sensitive to the configured binary format.
- [improvement] Make Geometry formats configurable.
- [new feature] DAT-331: Add option to automatically preserve TTL and timestamp.

## 1.7.0

- [bug] Correctly display durations lesser than 1 second (#369).
- [bug] Skip invalid lines when parsing the output of java -version (#372, fixes #371).
- [bug] DAT-612: Fix null pointer exception when reading from Stdin.
- [improvement] DAT-591: Ability to disable the console reporter.
- [improvement] DAT-605: Write empty strings as quoted empty fields by default in CSV connector.
- [bug] DAT-596: Do not display stty errors.
- [improvement] DAT-614: Ability to discard record sources.


## 1.6.0

- [bug] DAT-558: Count operations should not modify user-provided queries.
- [improvement] DAT-402: Create an abstract RecordReader leveraging Flux.generate.
- [bug] DAT-569: Do not parallelize queries containing GROUP BY, ORDER BY or LIMIT clauses.
- [bug] DAT-573: Convert empty strings to zero-length arrays when loading blobs.
- [bug] DAT-581: Resolve configuration references after the final config has been computed.
- [bug] DAT-585: Apply per-file limits when reading multiple resources.
- [new feature] DAT-588: Add option to unload blob data in hex format.
- [improvement] DAT-593: Remove thread pools in LogManager.
- [bug] DAT-410: Validate that monitoring report rate is greater than or equal to 1 second.
- [improvement] DAT-346: Add support for multichar field delimiters in CSV connector.
- [improvement] DAT-469: Raise the precision of kb/row to 3 decimal digits.
- [improvement] DAT-547: Ability to specify a list of allowed or denied hosts.
- [improvement] DAT-472: Ability to prompt for passwords when none specified in config.
- [improvement] DAT-571: Generalize maxConcurrentQueries to all operations.
- [improvement] DAT-575: Automatically detect the best concurrency level by sampling data.
- [improvement] DAT-578: Raise driver timeouts.
- [improvement] DAT-454: Revisit threading model for Unload workflow.
- [improvement] DAT-438: Apply maxConcurrentFiles to loads.


## 1.5.0

- [improvement] DAT-525: Update DSBulk to unified driver.
- [new feature] DAT-354: Add support for DSE Graph to DSBulk.
- [improvement] DAT-361: Display metrics in vertices or edges per second in graph mode.


## 1.4.1

- [bug] DAT-516: Always quote comment character when unloading.
- [new feature] DAT-519: Remove restriction preventing DSBulk use with OSS Cassandra.


## 1.4.0

- [improvement] DAT-303: Update DSBulk to Java DSE Driver 2.x.
- [new feature] DAT-449: Add support for loading and unloading from/to compressed files.
- [new feature] DAT-412: Add support for DataStax Cloud.
- [improvement] DAT-492: Add CLI shortcut for secure connect bundle.
- [improvement] DAT-490: Allow secure connect bundle to be provided from external URL.
- [improvement] DAT-481: Improve NodeComparator algorithm.
- [improvement] DAT-499: Warn if cloud enabled and incompatible settings are defined.
- [improvement] DAT-495: Allow driver to be configured directly.


### 1.3.4

- [bug] DAT-414: DSBulk should allow INSERT queries without clustering column when all columns are static.
- [improvement] DAT-411: Improve error message when row fails to decode.
- [improvement] DAT-383: Show version option in DSBulk help.
- [bug] DAT-333: Logging option "-maxErrors 0" doesn't abort the operation.
- [improvement] DAT-431: Replace Guava's Range with a custom range implementation.
- [improvement] DAT-435: Make connectors accept a list of URLs when loading.
- [improvement] DAT-443: Default output file name format should not include thousands separator.
- [improvement] DAT-428: Add support for UNITS_SINCE_EPOCH to CQL date and time.
- [improvement] DAT-451: Driver query warnings should not spam the console.
- [task] DAT-455: Update jackson-databind to 2.9.9.1 to fix CVE-2019-12814.
- [improvement] DAT-432: Ability to disable bytes/sec metrics.
- [bug] DAT-427: DSBulk should handle empty column names in CSV headers.
- [bug] DAT-427: DSBulk should handle empty or duplicate column names in CSV headers.
- [bug] DAT-441: Quote execution IDs when they generate invalid MBean names.


### 1.3.3

- [bug] DAT-400: Export of varchar column containing JSON may truncate data.


### 1.3.2

- [task] DAT-398: Remove unwanted dependencies or dependencies with offending licenses.


### 1.3.1

- [improvement] DAT-385: Implement unload and count for materialized views.
- [improvement] DAT-389: batch.bufferSize should be a multiple of batch.maxBatchStatements.
- [improvement] DAT-310: Simplify computation of resource positions.
- [improvement] DAT-388: Revisit "parallel" flows.
- [bug] DAT-392: Setting executor.maxInFlight to a negative value triggers fatal error.
- [improvement] DAT-384: Improve support for lightweight transactions.


### 1.3.0

- [bug] DAT-334: Murmur3TokenRangeSplitter should allow long overflows when splitting ranges.
- [improvement] DAT-336: Extend DSBulk's rate limiting capability to reads.
- [improvement] DAT-362: Improve readability of error messages printed to the console.
- [improvement] DAT-352: Calculate batch size dynamically - Adaptive Batch Sizing.
- [bug] DAT-339: CSV connector trims trailing whitespace when reading data.
- [improvement] DAT-344: Expose settings to control how to interpret empty fields in CSV files.
- [improvement] DAT-305: DSBulk help should show description of available commands.
- [improvement] DAT-327: Allow functions to appear in mapping variables
- [bug] DAT-368: Avoid overflows in CodecUtils.numberToInstant.
- [improvement] DAT-367: Detect writetime variable when unloading.
- [new feature] DAY-364: Ability to unload timestamps as units since an epoch.
- [improvement] DAT-308: Relax constraints on queries for the Count workflow.
- [improvement] DAT-319: Automatically add token range restriction to WHERE clauses.
- [bug] DAT-369: Call to ArrayBackedRow.toString() causes fatal NPE.
- [improvement] DAT-316: JsonNodeToUDTCodec should allow a Json array input.
- [new feature] DAT-340: Print basic information about the cluster.
- [improvement] DAT-372: Improve WHERE clause token range restriction detection.
- [improvement] DAT-370: Remove record location URI.
- [improvement] DAT-373: Allow columns and fields to be mapped more than once.
- [improvement] DAT-315: UDT and tuple codecs should respect allowExtraFields and allowMissingFields.
- [improvement] DAT-312: Add support for DSE 4.8 and lower.
- [improvement] DAT-378: Add support for keyspace-qualified UDFs in mappings.
- [improvement] DAT-379: Allow fields to appear as function parameters on the left side of mapping entries.
- [improvement] DAT-309: Improve handling of search queries.
- [improvement] DAT-380: Ability to hard-limit the number of concurrent continuous paging sessions.
- [improvement] DAT-365: Ability to skip unloading/loading solr_query column.


### 1.2.0

- [bug] DAT-302: CsvWriter trims leading/trailing spaces in values.
- [bug] DAT-311: CSV connector fails when the number of columns in a record is greater than 512.
- [improvement] DAT-252: Improve range split algorithm in multi-dc and/or vnodes environments.
- [improvement] DAT-317: Support simplified notation for JSON arrays and objects in collection fields.
- [bug] DAT-326: DSBulk fails when mapping contains a primary key column mapped to a function.


### 1.1.0

- [bug] DAT-289: Generated queries do not respect indexed mapping order.
- [bug] DAT-288: DSBulk cannot handle columns of type list<timestamp>.
- [improvement] DAT-292: Add support for counter tables.
- [bug] DAT-295: Generated query does not always contain all token ranges.
- [bug] DAT-297: Empty map values do not work when loading using DSBulk.
- [new feature] DAT-291: Add count workflow.
- [improvement] DAT-298: Rename CQL_DATE_TIME to CQL_TIMESTAMP.
- [improvement] DAT-286: Intercept Univocity exceptions and expose them in a user-friendly way.
- [improvement] DAT-287: Combine batch.mode and batch.enabled into a single setting.
- [improvement] DAT-299: Count the number of rows for the N biggest partitions.
- [bug] DAT-300: DSBulk fails to start with Java 10+.
- [improvement] DAT-290: DSBulk should avoid logging to stdout.
- [improvement] DAT-294: Use Antlr to validate user-supplied queries and mappings.


### 1.0.2

- [improvement] DAT-249: Don't use ANSI color codes on non-ANSI terminals.
- [bug] DAT-255: Debug files are missing records.
- [improvement] DAT-251: Update DriverSettings with new TokenAwarePolicy.
- [bug] DAT-259: LogManager files have interleaved entries.
- [bug] DAT-260: LogManager is closing files too soon.
- [bug] DAT-266: DSE Geometry types cause CodecNotFoundException.
- [improvement] DAT-270: Replace Java collections with JCTools equivalents whenever possible.
- [improvement] DAT-257: Check case on failure to identify keyspace or table.
- [improvement] DAT-258: Default driver.auth.provider to DsePlainTextAuthProvider if -p is specified.
- [improvement] DAT-261: Improve message when no files match pattern in a directory.
- [bug] DAT-268: Map-type settings are unrecognized from the command-line.
- [bug] DAT-273: Json connector doesn't respect WRITE_NULL_MAP_VALUES in serialization features setting.
- [improvement] DAT-275: Simplify schema.queryTimestamp format.
- [improvement] DAT-276: Remove connector.json.mapperFeatures setting.
- [improvement] DAT-277: Support all CQL literal formats.
- [improvement] DAT-253: More understandable error reporting when CSV file without headers is loaded without mapping specified.


### 1.0.1

- [improvement] DAT-240: Detect console width on Windows.
- [improvement] DAT-229: Allow user-supplied options to contain control characters.
- [improvement] DAT-237: Numeric overflows should display the original input that caused the overflow.
- [bug] DAT-245: Addresses should be properly translated when cluster has custom native port.
- [improvement] DAT-235: Improve error message when mapping is wrong.
- [improvement] DAT-243: Improve message when DSBulk completes with errors.
- [improvement] DAT-242: Reduce and filter stack traces.
- [improvement] DAT-244: Validate that mapped fields are present in the record.
- [improvement] DAT-238: Monitor throughput in bytes/sec.
- [improvement] DAT-246: Do not log ongoing metrics to main log file.
- [improvement] DAT-247: Improve handling of uncaught exceptions.
- [bug] DAT-241: Null words should be supported by all connectors.


### 1.0.0

- [improvement] DAT-220: Make ResultSubscription fully non-blocking.
- [bug] DAT-221: User home directory not correctly expanded when used with -f option.
- [improvement] DAT-225: Change driver.pooling.local.connections default to 8.
- [improvement] DAT-108: Upgrade DSE driver to 1.6.3.
- [improvement] DAT-227: Warn that continuous paging is not available when unloading with CL > ONE.
- [new feature] DAT-224: Add support for numeric overflow and rounding.
- [improvement] DAT-226: Use Netty's FastThreadLocal.
- [improvement] DAT-228: Simplify embedded CQL grammar.


### 1.0.0-rc1

- [bug] DAT-165: Messages should not be logged to standard output when it is being used for unloading.
- [new feature] DAT-172: Support numeric-to-temporal conversions.
- [new feature] DAT-175: Support temporal-to-numeric conversions.
- [enhancement] DAT-167: Add support for user-supplied execution ids.
- [new feature] DAT-22: Implement JSON connector.
- [improvement] DAT-163: Improve error message for invalid JSON paths.
- [new feature] DAT-181: Support temporal-to-timeuuid conversions.
- [new feature] DAT-184: Support boolean-to-number conversions.
- [improvement] DAT-183: Add code coverage to DSBulk builds.
- [bug] DAT-188: JSON connector does not terminate in SINGLE_DOCUMENT mode with an empty file.
- [improvement] DAT-179: Add ability for connectors to promote some settings to the common help section.
- [improvement] DAT-174: Support TTL and TIMESTAMP clauses with custom queries.
- [improvement] DAT-173: Ability to specify an error threshold as a percentage.
- [improvement] DAT-187: Change default for driver.auth.principal from user@DATASTAX.COM to unspecified.
- [improvement] DAT-192: Add descriptions for load balancing policy settings.
- [bug] DAT-193: SettingsManager is printing metaSettings to the console.
- [bug] DAT-191: Prevent file collisions on unload.
- [improvement] DAT-194: Stdin and stdout urls should use the special "-" token, similar to unix tools.
- [improvement] DAT-199: Connectors should be able to report write failures.
- [new feature] DAT-201: Use an ANTLR4-based parser to parse user-supplied statements.
- [improvement] DAT-205: Use a pooling library for concurrent writes.
- [improvement] DAT-210: Do not print effective settings to the console.
- [improvement] DAT-206: Default connector.*.url to "-".
- [improvement] DAT-200: DSBulk should fail if execution directory already exists and is not empty.
- [improvement] DAT-189: More gracefully error out when Json document mode is wrong.
- [improvement] DAT-190: Consider connector.json.mode for unloading.
- [bug] DAT-209: Records are being counted twice in Unload workflow.
- [improvement] DAT-158: Reorganize in-tree documentation.
- [improvement] DAT-180: Improve DSBulk exit statuses.
- [improvement] DAT-213: Create examples for connector.json.parserFeatures and connector.json.generatorFeatures.
- [improvement] DAT-216: application.conf and application.template.conf should include dsbulk element wrapping.
- [improvement] DAT-214: driver.auth.principal should be optional when using Kerberos.
- [improvement] DAT-207: Rename driver.auth.saslProtocol to driver.auth.saslService.
- [improvement] DAT-215: When validating path-based settings, verify file existence.


### 1.0.0-beta2

- [improvement] DAT-81: Surface setup errors better.
- [improvement] DAT-145: Use HdrHistogram as reservoir for latency measurement.
- [improvement] DAT-104: Add dry-run feature to try out load without writing to DSE.
- [bug] DAT-137: Workflow Engine does not react to interruption signals properly.
- [improvement] DAT-140: Report memory usage.
- [bug] DAT-150: In-flight requests are negative when continuous paging is active.
- [bug] DAT-152: Support query + keyspace settings combination.
- [improvement] DAT-109: Refactor setting initialization and validation.
- [improvement] DAT-146: Optimize load workflow for multiple files.
- [bug]: DAT-151: Unload workflow hangs when a destination file already exists.
- [improvement] DAT-155: Make dsbulk script more friendly for use in a DSE installation.
- [improvement] DAT-147: Support TIMESTAMP, TTL and now().
- [improvement] DAT-142: Add DSE version validation.
- [improvement] DAT-157: Optimize non-thread-per-core LoadWorkflow by trying to emit records to mapper threads together.
- [improvement] DAT-168: Field mapping should support string timestamps.
- [bug] DAT-170: Internal scheduler is not closed when LogManager is closed.


### 1.0.0-beta1

- [improvement] DAT-47: Refactor ReadResultEmitter.
- [improvement] DAT-110: Improve performance of read result mappers.
- [improvement] DAT-112: Sort 'Bulk Loader effective settings' output.
- [improvement] DAT-99: schema.keyspace should scope underlying session to the provided keyspace.
- [improvement] DAT-115: Make continuous paging optional.
- [improvement] DAT-49: Allow mappings to be inferred even when some are provided.
- [improvement] DAT-91: Add SSL/Auth Tests. Fix issue with ssl config and file paths.
- [improvement] DAT-107: Improve formatting of help section on the command line.
- [bug] DAT-100: Fully support CQL complex types.
- [improvement] DAT-117: Support blob and duration types.
- [improvement] DAT-78: Generate template file for users.
- [bug] DAT-122: Operation.log file is not created inside operation directory.
- [improvement] DAT-124: Java process should exit with non-zero status in case of error.
- [improvement] DAT-116: Log errors to stderr, not stdout.
- [bug] DAT-114: 'Reads' timer metrics should report correct latencies.
- [improvement] DAT-46: Provide a way to configure driver policies.
- [new feature] DAT-125: Report last successfully ingested lines in case of load failure.
- [new feature] DAT-129: Handle connector recoverable read errors gracefully.
- [improvement] DAT-132: When logging bound parameters of statements, be more clear about unset values.
- [bug] DAT-130: nullStrings setting doesn't handle "null" string properly.
- [improvement] DAT-133: When encountering a field parsing error, report the field index/name.
- [improvement] DAT-127: Remove unbounded queues from CSV connector.
- [bug] DAT-136: Large records cause the workflow to OOM.
- [bug] DAT-138: When maxErrors is reached the workflow does not always stop.
- [bug] DAT-128: Last recorded locations should be 100% accurate.
- [improvement] DAT-135: Fail fast when mapping doesn't align with table.
- [bug] DAT-144: When columns are larger than 4096 characters we error out.
- [improvement] DAT-92: schema.mapping should support specifying an array of target columns.


### 1.0.0-alpha2

- [improvement] DAT-42: Add LOADER_JAVA_OPTS env var support to allow users to set JVM options.
- [improvement] DAT-74: executor.maxPerSecond and executor.maxInflight should account for batch size.
- [improvement] DAT-76: Parallelize execution of workflow engine components.
- [improvement] DAT-77: Separate batch.bufferSize into batch.bufferSize and batch.maxBatchSize.
- [improvement] DAT-62: Add end-to-end tests for CSV read workflow.
- [improvement] DAT-72: Improve command line options.
- [improvement] DAT-88: Rename modules and packages to dsbulk.
- [improvement] DAT-80: Rename ssl.keystore.url and ssl.truststore.url settings.
- [improvement] DAT-83: Add --version option to emit version.
- [improvement] DAT-85: Make driver.hosts option a comma-delimited string for ease of use.
- [improvement] DAT-58: Generate settings.md documentation page from reference.conf.
- [improvement] DAT-79: Rename and remove various settings.
- [improvement] DAT-95: Change schema.mapping and schema.recordMetadata to string values that should be parsed in SchemaSettings.
- [improvement] DAT-97: Add -f option to choose config file.
- [improvement] DAT-98: Update "Loader/Unloader" refs to "Loader".
- [improvement] DAT-84: Add help subcommand to get help for groups of settings.
- [improvement] DAT-93: nullStrings setting should be more flexible.
- [bug] DAT-73: Make mappings work with quoted CQL identifiers.
- [improvement] DAT-87: Validate configuration more deeply.


### 1.0.0-alpha1

- [new feature] DAT-14: Implement configuration service.
- [new feature] DAT-15: Implement connection service.
- [new feature] DAT-16: Implement mapping service.
- [new feature] DAT-20: Implement fault tolerance.
- [new feature] DAT-17: Implement conversion service.
- [improvement] DAT-29: Simplify way to select connector.
- [improvement] DAT-30: Revisit bad files management.
- [improvement] DAT-37: ConnectorSettings.locateConnector "not found" error should leverage String.format completely.
- [new feature] DAT-33: Add support for String <-> InetAddress conversion.
- [new feature] DAT-21: Implement CSV connector.
- [improvement] DAT-34: Add grouping by replica set to token-aware batching.
- [new feature] DAT-19: Implement monitoring service.
- [new feature] DAT-27: Support NULL and UNSET values.
- [improvement] DAT-28: Support short class names for settings that expect a FQCN.
- [improvement] DAT-44: Do not block a thread while reading results.
- [new feature] DAT-24: Add support for authentication and encryption.
- [bug] DAT-48: Mappings are not being inferred.
- [improvement] DAT-31: Support String to Collection, UDT and Tuple conversion.
- [new feature] DAT-18: Create an executable bundle.
- [new feature] DAT-55: Implement Read Workflow Engine.
- [improvement] DAT-51: Simplify way to specify connector settings.
- [improvement] DAT-60: driver.contactPoints should support hostnames/ip's without port.
- [bug] DAT-69: CSV Connector fails to parse files with non-native line-ending.
- [new feature] DAT-64: Implement connector writes.
- [new feature] DAT-63: Support writing to standard output.
