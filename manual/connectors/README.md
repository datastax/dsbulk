# DataStax Bulk Loader Connectors

[Connectors] are responsible for connecting to an external source of data and extracting 
that data in the form of [Records], for downstream consumption by the loader workflow engine.

Connectors are created by implementing the `Connector` interface. They are then discovered through
the Service Loader API.

Available configuration settings for all connectors are documented [here](../settings.md).

## Available Connectors

The DataStax Bulk Loader ships with the following connectors:

* [CSV Connector]: a highly-configurable connector that reads field-delimited files. 
  For more information about the CSV file format, see [RFC 4180] and the [Wikipedia article on CSV format].
* [Json Connector]: a connector that read Json data.
* [CQL Connector]: a connector that reads CQL files.

[Connectors]: ../../connectors/api/src/main/java/com/datastax/dsbulk/connectors/api/Connector.java
[Records]: ../../connectors/api/src/main/java/com/datastax/dsbulk/connectors/api/Record.java
[CSV Connector]: ./csv
[Json Connector]: ./json
[CQL Connector]: ./cql
[RFC 4180]: https://tools.ietf.org/html/rfc4180
[Wikipedia article on CSV format]: https://en.wikipedia.org/wiki/Comma-separated_values
