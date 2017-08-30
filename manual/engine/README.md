# DataStax Bulk Loader/Unloader Workflow Engine

The DataStax Workflow Engine is the component responsible for the orchestration of a loading operation.

## Main Features

* Configuration: The engine collects user-supplied settings, merges them with default values and 
configures the loading operation to run.
* Connection: The engine handles the driver connection to DSE and manages driver-specific settings 
(contact points, keyspace, table, consistency level, etc.). 
It also supports authentication and SSL encryption.
* Conversion: The engine handles data type conversions, e.g. boolean, number, date conversions 
from anything (typically, strings or raw bytes as emitted by a connector) to appropriate internal 
representations (typically, Java Temporal or Number objects). It also handles `NULL` and `UNSET` values.
* Mapping: The engine analyzes metadata gathered from the driver and infers the appropriate `INSERT` prepared statement,
then crosses this information with user-supplied information about the data source to infer what are the bound 
variables to use.
* Monitoring: the engine reports metrics about all its internal components, mainly the connector and the bulk executor.
* Error Handling: the engine handles errors from both connectors and the bulk executor, and reports read, parse and 
write failures. These are redirected to a configurable "bad file" that contains sources that could not be loaded.

## Workflow Overview

![Workflow](./workflow.png)

## Engine Configuration

See the [engine configuration] section for information about how to configure the engine.

[engine configuration]: ./configuration/

## Engine Monitoring

See the [engine monitoring] section for information about how to monitor the engine.

[engine monitoring]: ./monitoring/

