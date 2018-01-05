## Changelog

### 1.0.0 (in progress)

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
- [improvement] DAT-180: Improve DSBulk exit statuses.


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
