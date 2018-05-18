# DataStax Bulk Loader Overview

The Datastax Bulk Loader tool (DSBulk) provides the ability to load large amounts of data 
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

See the [Settings page](settings.md) or the [template configuration file]
for details.

### Shortcuts

For convenience, many options (prefaced with `--`), have shortcut variants (prefaced with `-`).
For example, `--connector.name` has an equivalent short option `-c`. 

Connector-specific options also have shortcut variants, but they are only available when
the appropriate connector is chosen. This allows multiple connectors to overlap shortcut
options. For example, the JSON connector has a `--connector.json.url`
setting with a `-url` shortcut. This overlaps with the `-url` shortcut option for the CSV 
connector, that actually maps to `--connector.csv.url`. But in a given invocation of `dsbulk`, 
only the appropriate shortcut will be active.  

Run the tool with `--help` and specify the connector to see its short options:

```
dsbulk -c csv --help
```

## Configuration Files vs Command Line Options

All DSBulk options can be passed as command line arguments, or in a configuration file.

Using a configuration file is sometimes easier than passing all configuration 
options via the command line. 

By default, the configuration file is located under DSBulk's `conf` directory and is named 
`application.conf`. This location can be modified via the `-f` switch. See examples below.

DSBulk ships with a default, empty `application.conf` file that users can customize to their 
needs; it also has a [template configuration file] that can serve as a starting point for 
further customization.

Configuration files are also required to be compliant with the [HOCON] syntax. This syntax
is very flexible and allows sections to be grouped together in blocks, e.g.:

```hocon
dsbulk {
  connector {
    name = "csv"
    csv {
      url = "C:\\Users\\My Folder"
      delimiter = "\t"
    }
  }
}
```

The above is equivalent to the following snippet using dotted notation instead of blocks:

```hocon
dsbulk.connector.name = "csv"
dsbulk.connector.csv.url = "C:\\Users\\My Folder"
dsbulk.connector.csv.delimiter = "\t"
```

**Important caveats:**

1. Options specified in configuration files are always prefixed with `dsbulk.`. 
   _This prefix must not be included when specifying options via the command line._ 
   For example, to select the connector to use in a configuration file,
   use `dsbulk.connector.name = csv`, as in the example above; on the command line, 
   however, you should use `--connector.name csv` to achieve the same effect.
2. Options specified through the command line _override options specified in configuration files_.
   See examples for details.

## Escaping and Quoting Command Line Arguments

When supplied via the command line, all option values are expected to be in valid [HOCON] syntax.
For example, control characters, the backslash character, and the double-quote character all need to
be properly escaped.

For example, `\t` is the escape sequence that corresponds to the tab character, whereas `\\` is
the escape sequence for the backslash character:

```bash
dsbulk load -delim '\t' -url 'C:\\Users\\My Folder'
```

In general, string values containing special characters also need to be properly quoted with
double-quotes, as required by the HOCON syntax:

```bash
dsbulk load -url '"C:\\Users\\My Folder"'
```

However, when the expected type of an option is a string, it is possible to omit the
surrounding double-quotes, for convenience. Thus, note the absence of the double-quotes in the first
example. Similarly, when an argument is a list, it is possible to omit the surrounding square
brackets; making the following two lines equivalent:

```bash
dsbulk load --codec.nullStrings 'NIL, NULL'
dsbulk load --codec.nullStrings '[NIL, NULL]'
```

The same applies for arguments of type map: it is possible to omit the surrounding
curly braces, making the following two lines equivalent:

```bash
dsbulk load --connector.json.deserializationFeatures '{ USE_BIG_DECIMAL_FOR_FLOATS : true }'
dsbulk load --connector.json.deserializationFeatures 'USE_BIG_DECIMAL_FOR_FLOATS : true'
```

This syntactic sugar is only available for command line arguments of type string, list or map; 
all other option types, as well as all options specified in a configuration file _must_ be fully 
compliant with HOCON syntax, and it is the user's responsibility to ensure that such options are 
properly escaped _and_ quoted.

## Load Examples

* Load table `table1` in keyspace `ks1` from CSV data read from `stdin`.
  Use a cluster with a `localhost` contact point. Field names in the data match column names in the
  table. Field names are obtained from a *header row* in the data; by default the tool presumes 
  a header exists in each file being loaded:

  `dsbulk load -k ks1 -t table1`

* Load table `table1` in keyspace `ks1` from a gzipped CSV file by unzipping it to `stdout` and piping to
  `stdin` of the tool:

  `gzcat table1.csv.gz | dsbulk load -k ks1 -t table1`

* Load the file `export.csv` to table `table1` in keyspace `ks1` using the short form option for `url`
  and the tab character as a field delimiter:

  `dsbulk load -k ks1 -t table1 -url export.csv -delim '\t'`

* Specify a few hosts (initial contact points) that belong to the desired cluster and
  load from a local file, without headers. Map field indices of the input to table columns:
  
  `dsbulk load -url ~/export.csv -k ks1 -t table1 -h '10.200.1.3, 10.200.1.4' -header false -m '0=col1,1=col3'`

* Specify port 9876 for the cluster hosts and load from an external source url:

  `dsbulk load -url https://192.168.1.100/data/export.csv -k ks1 -t table1 -h '10.200.1.3, 10.200.1.4' -port 9876`

* Load all csv files from a directory. The files do not have header rows. Map field indices
  of the input to table columns:

  `dsbulk load -url ~/export-dir -k ks1 -t table1 -m '0=col1,1=col3' -header false`

* Load a file containing three fields per row. The file has no header row. Map all fields to
  table columns in field order. Note that field indices need not be provided.

  `dsbulk load -url ~/export-dir -k ks1 -t table1 -m 'col1, col2, col3' -header false`

* With default port for cluster hosts, keyspace, table, and mapping set in
  `conf/application.conf`:

  `dsbulk load -url https://192.168.1.100/data/export.csv -h '10.200.1.3,10.200.1.4'`

* With default port for cluster hosts, keyspace, table, and mapping set in `dsbulk_load.conf`:

  `dsbulk load -f dsbulk_load.conf -url https://192.168.1.100/data/export.csv -h '10.200.1.3,10.200.1.4'`

* Load table `table1` in keyspace `ks1` from a CSV file, where double-quote characters in fields are
  escaped with a double-quote; for example,  `"f1","value with ""quotes"" and more"` is a line in
  the CSV file:

  `dsbulk load -url ~/export.csv -k ks1 -t table1 -escape '\"'`

## Unload Examples

Unloading is simply the inverse of loading and due to the symmetry, many settings are
used in both load and unload.

* Unload data to `stdout` from the `ks1.table1` table in a cluster with a `localhost` contact 
  point. Column names in the table map to field names in the data. Field names must be emitted 
  in a *header row* in the output:

  `dsbulk unload -k ks1 -t table1`

* Unload data to `stdout` from the `ks1.table1` and gzip the result:

  `dsbulk unload -k ks1 -t table1 | gzip > table1.gz`

* Unload data to a local directory (which may not yet exist):
                                          
  `dsbulk unload -url ~/data-export -k ks1 -t table1`
  
## Command-line Help

Available settings along with defaults are documented [here](settings.md) and in the
[template configuration file]. This information is also available on the command-line 
via the `help` subcommand.

* Get help for common options and a list of sections from which more help is available:

  `dsbulk help`
  
* Get help for all `connector.csv` options:

  `dsbulk help connector.csv`
  
[template configuration file]:application.template.conf
[onlineDocs]:https://docs.datastax.com/en/dse/1.0.0/dsbulk/
[HOCON]:https://github.com/lightbend/config/blob/master/HOCON.md