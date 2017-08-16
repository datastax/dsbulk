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
  local BIN_NAME=`basename "$0"`
  cat << END
Usage: datastax-loader <options>
Options:
 -c, --connector.name <name>       Name of connector; may be the fqcn or simple name
                                   of an implementation of the Connector interface,
                                   or a prefix of the simple name. Only the built-in
                                   CSVConnector is supported at this time, so "csv"
                                   is a good value for this.
 -s, --connector.common.source <source url or path>
                                   Url or file path of data to load; the actual meaning
                                   of this option depends on the selected connector.
 -k, --schema.keyspace <keyspace>  Keyspace into which to load data.
 -t, --schema.table <table>        Table into which to load data.
 -m, --schema.mapping <mapping>    Mapping of fields in data to columns in the database.

All arguments except connector.name and connector.common.source are optional in that 
values fall back to defaults or are inferred from the input data.


CONFIG FILES, SETTINGS SEARCH ORDER, AND OVERRIDES:

Available settings along with defaults are recorded in conf/reference.conf. This file
also contains detailed descriptions of settings and is a great source of information.
When the loader starts up, settings are first loaded from conf/reference.conf.

The conf directory also contains an applicaton.conf where a user may specify permanent
overrides of settings. These may be in nested-structure form like this:

datastax-loader {
  connector {
    name="csv"
  }
}

or dotted form: datastax-loader.connector.name="csv"

Finally, a user may specify impromptu overrides via long options on the command line.
See examples for details.


EXAMPLES:
* Load CSV data from stdin to the ks1.table1 table in a cluster with
  a localhost contact point. Field names in the data match column names in the
  table. Field names are obtained from a "header row" in the data:
    generate_data | datastax-loader -c csv -s stdin:/ -k ks1 -t table1 --connector.csv.header=true

* Same as last example, but load from a local file:
    datastax-loader -c csv -s ~/export.csv -k ks1 -t table1 --connector.csv.header=true

* Same as last example, but load data from a url:
    datastax-loader -c csv -s https://svr/data/export.csv -k ks1 -t table1 --connector.csv.header=true

* Same as last example, but there is no header row and we specify an explicit field mapping based
  on field indices in the input:
    datastax-loader -c csv -s https://svr/data/export.csv -k ks1 -t table1 -m "{0=col1,1=col3}"

* Same as last example, but specify a few contact points; note how the value is quoted because
  this setting is an array of strings:
    datastax-loader -c csv -s https://svr/data/export.csv -k ks1 -t table1 -m "{0=col1,1=col3}" --driver.contactPoints '["10.200.1.3","10.200.1.4"]'

* Same as last example, but with connector-name, keyspace, table, and mapping set in
  conf/application.conf:
    datastax-loader -s https://svr/data/export.csv --driver.contactPoints '["10.200.1.3","10.200.1.4"]'
```