# DataStax Bulk Loader Overview

The Datastax Bulk Loader tool provides the ability to load large amounts of data 
into the database efficiently and reliably as well as conversely unload data from the
database. In this release, csv and json file loading/unloading is supported.  

Launch the tool with the appropriate script in the bin directory of
your distribution. The help text of the tool provides summaries of all 
supported settings.

The most up-to-date documentation is available [online][onlineDocs]. 

## Basic Usage
The `dsbulk` command takes a subcommand argument followed by options:
```
# Load data
dsbulk load <options>

# Unload data
dsbulk unload <options>
``` 

All arguments are optional in that values fall back to defaults or
are inferred from the input data (when performing a load). However, some connectors have 
required settings of their own and those must be set as well. For example, the csv connector
requires the `connector.csv.url` setting to specify the source path/url of the csv data to 
load (or path/url where to send unloaded data).

See the [Settings page](settings.md) or the [template application config]
for details.

### Shortcuts
For convenience, many options (prefaced with `--`), have shortcut variants (prefaced with `-`).
For example, `--connector.name` has an equivalent short option `-c`. 

Connector-specific options also have shortcut variants, but they are only available when
the appropriate connector is chosen. This allows multiple connectors to overlap shortcut
options. For example, a future JSON connector will likely have a `--connector.json.url`
setting with a `-url` shortcut. This overlaps with the `-url` shortcut option for 
`--connector.csv.url`, but in a given invocation of `dsbulk`, only the appropriate shortcut will 
be active.  

Run the tool with `--help` and specify the connector to see its short options:

```
dsbulk -c csv --help
```

## Config Files, Settings, Search Order, and Overrides

Available settings along with defaults are documented [here](settings.md) and in the
[template application config].

The conf directory also contains an `application.conf` file where a user may specify permanent
overrides of settings. These are expressed in dotted form:
```hocon
dsbulk.connector.name="csv"
dsbulk.schema.keyspace="myks"
```

Finally, a user may specify impromptu overrides via options on the command line.
See examples for details.

## Load Examples
* Load CSV data from `stdin` (represented by a hyphen url value) to the `ks1.table1` table in a 
  cluster with a `localhost` contact point. Field names in the data match column names in the
  table. Field names are obtained from a *header row* in the data; by default the 
  tool presumes a header exists in each file being loaded:

  `generate_data | dsbulk load -url - -k ks1 -t table1`

* Specify a few hosts (initial contact points) that belong to the desired cluster and
  load from a local file, without headers:
  
  `dsbulk load -url  ~/export.csv -k ks1 -t table1 -h '10.200.1.3, 10.200.1.4' -header false`

* Specify port 9876 for the cluster hosts and load from an external source url:

  `dsbulk load -url https://svr/data/export.csv -k ks1 -t table1 -h '10.200.1.3, 10.200.1.4' -port 9876`

* Load all csv files from a directory. The files do not have header rows. Map field indices
  of the input to table columns:

  `dsbulk load -url ~/export-dir -k ks1 -t table1 -m '0=col1,1=col3' -header false`

* Load a file containing three fields per row. The file has no header row. Map all fields to
  table columns in field order. Note that field indices need not be provided.

  `dsbulk load -url ~/export-dir -k ks1 -t table1 -m 'col1, col2, col3' -header false`

* With default port for cluster hosts, keyspace, table, and mapping set in
  `conf/application.conf`:

  `dsbulk load -url https://svr/data/export.csv -h '10.200.1.3,10.200.1.4'`

## Unload Examples
Unloading is simply the inverse of loading and due to the symmetry, many settings are
used in both load and unload.

* Unload data to `stdout` (represented by a hyphen url value) from the `ks1.table1` table in a 
  cluster with a `localhost` contact point. Column names in the table map to field names 
  in the data. Field names must be emitted in a *header row* in the output:

  `dsbulk unload -url - -k ks1 -t table1`

* Unload data to a local directory (which may
  not yet exist):
                                          
  `dsbulk unload -url ~/-data-export -k ks1 -t table1`
  
* Unload data from a remote cluster to a remote destination url:

  `dsbulk unload -url https://svr/data/table1 -k ks1 -t table1 -h 10.200.1.3`

## Command-line Help
Available settings along with defaults are documented [here](settings.md) and in the
[template application config].
This information is also available on the command-line via the `help` subcommand.

* Get help for common options and a list of sections from which more help is available:

  `dsbulk help`
  
* Get help for all `connector.csv` options:

  `dsbulk help connector.csv`
  
[template application config]:application.template.conf
[onlineDocs]:https://docs.datastax.com/en/dse/1.0.0/dsbulk/