# DataStax Loader Overview

The Datastax Loader tool provides the ability to load large amounts of data 
into the database efficiently and reliably. In this release, only csv file
loading is supported.  

The tool consists of three main components:
* [Connectors](./connectors)
* [Executor](./executor)
* [Engine](./engine).

Launch the tool with the appropriate script in the bin directory of
your distribution of this tool. All settings governing execution of
the loader are defined in `conf/reference.conf` with detailed descriptions.
You may change default values in that file to meet your needs or override default values with
command line arguments.

The help output from the Unix script should help you get up and running quickly:
 
```
Usage: datastax-loader <options>
Options:
 -c, --connector.name <name>       Name of connector; may be the fqcn or simple name
                                   of an implementation of the Connector interface,
                                   or a prefix of the simple name. Only the built-in
                                   CSVConnector is supported at this time, so "csv"
                                   is a good value for this.
 -s, --connector.csv.url <source url or path>
                                   Url or file path of data to load.
 -k, --schema.keyspace <keyspace>  Keyspace into which to load data.
 -t, --schema.table <table>        Table into which to load data.
 -m, --schema.mapping <mapping>    Mapping of fields in data to columns in the database.

All arguments except connector.csv.url are optional in that values fall back to defaults
in conf/reference.conf or are inferred from the input data. In addition, any setting
specified in conf/reference.conf may be overridden on the command-line with a long
option.

NOTE: The user is free to update defaults in reference.conf to meet particular requirements,
      thus reducing the command-line arguments specified in invocations.

Examples:
* Load CSV data from /opt/data/export.csv to the ks1.table1 table in a cluster with
  a localhost contact point. Field names in the data match column names in the
  table. Field names are obtained from a "header row" in the data:

  datastax-loader -c csv -s /opt/data/export.csv -k ks1 -t table1 --connector.csv.header=true

* Connector-specific options like connector.csv.header are also available with a
  shortcut that eliminates the connector-specific name from the setting name:

  datastax-loader -c csv -s /opt/data/export.csv -k ks1 -t table1 --connector.header=true

* Same as last example, but load data from a url:
  datastax-loader -c csv -s https://svr/data/export.csv -k ks1 -t table1 --connector.header=true

* Same as last example, but there is no header row and we specify an explicit field mapping based
  on field indices in the input:
  datastax-loader -c csv -s https://svr/data/export.csv -k ks1 -t table1 -m "{0=col1,1=col3}"

* Same as last example, but specify a few contact points; note how the value is quoted:
  datastax-loader -c csv -s https://svr/data/export.csv -k ks1 -t table1 -m "{0=col1,1=col3}" --driver.contactPoints '["10.200.1.3","10.200.1.4"]'

* Same as last example, but with connector-name, keyspace, table, and mapping set in
  conf/reference.conf:
  datastax-loader -s https://svr/data/export.csv --driver.contactPoints '["10.200.1.3","10.200.1.4"]'
```