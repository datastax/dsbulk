# DataStax Bulk Loader/Unloader Overview

The Datastax Bulk Loader/Unloader tool provides the ability to load large amounts of data 
into the database efficiently and reliably as well as conversely unload data from the
database. In this release, only csv file loading is supported.  

The tool consists of three main components:
* [Connectors](./connectors)
* [Executor](./executor)
* [Engine](./engine).

Launch the tool with the appropriate script in the bin directory of
your distribution. The help text of the tool provides summaries of all 
supported settings.

## Basic Usage
The `dsbulk` command takes a subcommand argument followed by options:
```
# Load data
dsbulk load <options>

# Unload data
dsbulk unload <options>
``` 

All arguments except `connector.name` are optional in that values fall back to defaults or
are inferred from the input data (when performing a load). However, some connectors have 
required settings of their own and those must be set as well. For example, the csv connector
requires the `connector.csv.url` setting to specify the source path/url of the csv data to 
load (or path/url where to send unloaded data).

### Shortcuts
For convenience, many options (prefaced with `--`), have shortcut variants (prefaced with `-`).
For example, `--connector.name` has an equivalent short option `-c`. 

Connector-specific options also have shortcut variants, but they are only available when
the appropriate connector is chosen. This allows multiple connectors to overlap shortcut
options. For example, both `--connector.csv.url` and `--connector.json.url` have a
`-url` shortcut option, but in a given invocation of dsbulk, only the appropriate one
will be active.  

## Config Files, Settings, Search Order, and Overrides

Available settings along with defaults are recorded in `conf/reference.conf`. This file
also contains detailed descriptions of settings and is a great source of information.
When the loader starts up, settings are first loaded from conf/reference.conf.

The conf directory also contains an application.conf file where a user may specify permanent
overrides of settings. These may be in nested-structure form like this:

dsbulk {
  connector {
    name="csv"
  }
}

or dotted form: dsbulk.connector.name="csv"

Finally, a user may specify impromptu overrides via options on the command line.
See examples for details.

## Load Examples
* Load CSV data from `stdin` to the `ks1.table1` table in a cluster with
  a `localhost` contact point. Field names in the data match column names in the
  table. Field names are obtained from a *header row* in the data:

  `generate_data | dsbulk load -c csv -url stdin:/ -k ks1 -t table1 -header=true`

* Same as last example, but load from a local file:

  `dsbulk load -c csv -url ~/export.csv -k ks1 -t table1 -header=true`

* Same as last example, but load data from a url:

  `dsbulk load -c csv -url https://svr/data/export.csv -k ks1 -t table1 -header=true`

* Same as last example, but there is no header row and we specify an explicit field mapping based
  on field indices in the input:

  `dsbulk load -c csv -url https://svr/data/export.csv -k ks1 -t table1 -m '{0=col1,1=col3}'`

* Same as last example, but specify a few contact points at the default port:
  
  `dsbulk load -c csv -url https://svr/data/export.csv -k ks1 -t table1 -m '{0=col1,1=col3}' -h '[10.200.1.3, 10.200.1.4]'`

* Same as last example, but specify port 9876 for the contact points:

  `dsbulk load -c csv -url https://svr/data/export.csv -k ks1 -t table1 -m '{0=col1,1=col3}' -h '[10.200.1.3, 10.200.1.4]' -port 9876`

* Same as last example, but with default port for contact points, and connector-name, keyspace, table, and mapping set in
  conf/application.conf:

  `dsbulk load -url https://svr/data/export.csv -h '[10.200.1.3, 10.200.1.4]'`

## Unload Examples
Unloading is simply the inverse of loading and due to the symmetry, many settings are
used in both load and unload.

* Unload data to `stdout` from the `ks1.table1` table in a cluster with
  a `localhost` contact point. Column names in the table map to field names 
  in the data. Field names must be emitted in a *header row* in the output:

  `dsbulk unload -c csv -url stdout:/ -k ks1 -t table1 -header=true`

* Same as last example, but unload data to a local directory (which may
  not yet exist):
                                          
  `dsbulk unload -c csv -url ~/-data-export -k ks1 -t table1 -header=true`