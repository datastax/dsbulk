## Changelog

### 1.0.0 (in progress)

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
- [improvement] DAT-42: Add LOADER_JAVA_OPTS env var support to allow users to set JVM options.
- [improvement] DAT-74: executor.maxPerSecond and executor.maxInflight should account for batch size.
- [improvement] DAT-76: Parallelize execution of workflow engine components.
- [improvement] DAT-77: Separate batch.bufferSize into batch.bufferSize and batch.maxBatchSize
- [improvement] DAT-62: Add end-to-end tests for CSV read workflow
- [improvement] DAT-72: Improve command line options
- [improvement] DAT-80: Rename ssl.keystore.url and ssl.truststore.url settings
- [improvement] DAT-83: Add --version option to emit version
- [improvement] DAT-85: Make driver.hosts option a comma-delimited string for ease of use
- [improvement] DAT-58: Generate settings.md documentation page from reference.conf
- [improvement] DAT-79: Rename and remove various settings
- [improvement] DAT-95: Change schema.mapping and schema.recordMetadata to string values that should be parsed in SchemaSettings
- [improvement] DAT-97: Add -f option to choose config file
- [improvement] DAT-98: Update "Loader/Unloader" refs to "Loader"
- [improvement] DAT-87: Validate configuration more deeply.

