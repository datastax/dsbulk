# DataStax Loader Connectors

[Connectors] are responsible for connecting to an external source of data and extracting 
that data in the form of [Records], for downstream consumption by the loader workflow engine.

Connectors are created by implementing the `Connector` interface. They are then discovered through
the Service Loader API.

## Available Connectors

The DataStax Loader ships with the following connectors:

* [CSV Connector]: a connector that reads field-delimited files.
* [Json Connector]: a connector that read Json data.
* [CQL Connector]: a connector that reads CQL files.

[Connectors]: ../../connectors/api/src/main/java/com/datastax/loader/connectors/api/Connector.java
[Records]: ../../connectors/api/src/main/java/com/datastax/loader/connectors/api/Record.java
[CSV Connector]: ./csv
[Json Connector]: ./json
[CQL Connector]: ./cql
