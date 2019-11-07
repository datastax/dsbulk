# DataStax Bulk Loader Overview

The Datastax Bulk Loader tool (DSBulk) is a unified tool for loading into and unloading from DSE 
storage engines (i.e., Cassandra/Apollo and DataStax Enterprise).

DSBulk provides the ability to load large amounts of data 
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

# Count rows
dsbulk count <options>
``` 

### Long options

Any DSBulk or Java Driver setting can be entered on the command line as a long-option argument of
the following general form:

    --full.path.of.setting "some-value"
    
DSBulk settings always start with `dsbulk`; for convenience, this prefix can be omitted in a long
option argument, so the following two options are equivalent and both map to DSBulk's 
`dsbulk.batch.mode` setting:

    --dsbulk.batch.mode PARTITION_KEY
    --batch.mode PARTITION_KEY
    
Java Driver settings always start with `datastax-java-driver`; for convenience, this prefix can be
shortened to `driver` in a long option argument, so the following two options are equivalent and 
both map to the driver's `datastax-java-driver.basic.cloud.secure-connect-bundle` setting:

    --datastax-java-driver.basic.cloud.secure-connect-bundle /path/to/bundle
    --driver.basic.cloud.secure-connect-bundle /path/to/bundle
    
Most settings have default values, or values that can be inferred from the input data. However, 
sometimes the default value is not suitable for you, in which case you will have to specify the
desired value either in the application configuration file (see below), or on the command line. 

For example, the default value for `connector.csv.url` is to read from standard input or write to 
standard output; if that does not work for you, you need to override this value and specify the 
source path/url of the csv data to load (or path/url where to send unloaded data).

See the [Settings page](manual/settings.md) or DSBulk's 
[template configuration file](manual/application.template.conf) for details.

### Short options (Shortcuts)

For convenience, many options (prefaced with `--`), have shortcut variants (prefaced with `-`).
For example, `--dsbulk.schema.keyspace` has an equivalent short option `-k`. 

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

Using one or more configuration files is sometimes easier than passing all configuration 
options via the command line. 

By default, the configuration files are located under DSBulk's `conf` directory; the main
configuration file is named `application.conf`. This location can be modified via the `-f` 
switch. See examples below.

DSBulk ships with a default, empty `application.conf` file that users can customize to their 
needs; it also has a [template configuration file](manual/application.template.conf) that can 
serve as a starting point for further customization.

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

You can split your configuration in more than one file using file inclusions; see the HOCON
documentation for details. The default configuration file includes another file called 
`driver.conf`, also located in the `conf` directory. This file should be used to configure 
the Java Driver for DSBulk. This file is empty as well; users can customize it to their needs. 
A [driver template configuration file](manual/driver.template.conf) can serve as a starting 
point for further customization.

**Important caveats:**

1. In configuration files, it is not possible to omit the prefix `dsbulk`. 
   For example, to select the connector to use in a configuration file,
   use `dsbulk.connector.name = csv`, as in the example above; on the command line, 
   however, you can use `--dsbulk.connector.name csv` or `--connector.name csv` to achieve 
   the same effect, as stated above.
2. In configuration files, it is not possible to abbreviate the prefix `datastax-java-driver`
   to `driver`. For example, to select the consistency level to use in a configuration file,
   use `datastax-java-driver.basic.request.consistency = QUORUM` in a configuration file; on 
   the command line, however, you can use both
   `--datastax-java-driver.basic.request.consistency = QUORUM` or 
   `--driver.basic.request.consistency = QUORUM` to achieve the same effect.
3. Options specified through the command line _override options specified in configuration 
   files_. See examples for details.

## Escaping and Quoting Command Line Arguments

Regardless of whether they are supplied via the command line or in a configuration file, all option 
values are expected to be in valid [HOCON] syntax: control characters, the backslash character, 
and the double-quote character all need to be properly escaped.

For example, `\t` is the escape sequence that corresponds to the tab character:

```bash
dsbulk load -delim '\t'
```

In general, string values containing special characters (such as a colon or a whitespace) also 
need to be properly quoted with double-quotes, as required by the HOCON syntax:

```bash
dsbulk load -h '"host.com:9042"'
```

File paths on Windows systems usually contain backslashes; `\\` is the escape sequence for the 
backslash character, and since Windows paths also contain special characters, the whole path 
needs to be double-quoted: 

```bash
dsbulk load -url '"C:\\Users\\My Folder"'
```

However, when the expected type of an option is a string, it is possible to omit the
surrounding double-quotes, for convenience:

```bash
dsbulk load -url 'C:\\Users\\My Folder'
```

Similarly, when an argument is a list, it is possible to omit the surrounding square
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

Also, note that this syntactic sugar is not capable of quoting single elements inside a list or a 
map; all elements in a list or a map must be individually quoted if they contain special characters.
For example, to specify a list with 2 contact points containing port numbers, it is necessary to 
quote each contact point individually, however the surrounding brackets, as explained above, can 
be omitted for brevity:

```bash
dsbulk load -h '"host1.com:9042","host2.com:9042"'
```

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
  
  `dsbulk load -url ~/export.csv -k ks1 -t table1 -h '10.200.1.3,10.200.1.4' -header false -m '0=col1,1=col3'`

* Specify port 9876 for the cluster hosts and load from an external source url:

  `dsbulk load -url https://192.168.1.100/data/export.csv -k ks1 -t table1 -h '10.200.1.3,10.200.1.4' -port 9876`

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


## Count Examples

When counting rows in a table, no connector is required, and `schema.mapping` should not be present.

* Count the total rows in the `ks1.table1` table in a cluster with a `localhost` contact point. 

  `dsbulk count -k ks1 -t table1`

* Count the total number of rows per token range in the `ks1.table1` table in a cluster with a `localhost` contact point. 

  `dsbulk count -k ks1 -t table1 -stats ranges`

* Count the total number of rows per host in the `ks1.table1` table in a cluster with a `localhost` contact point. 

  `dsbulk count -k ks1 -t table1 -stats hosts`

* Count the total number of rows for the biggest 100 partitions in the `ks1.table1` table in a cluster with a `localhost` contact point (by default, DSBulk returns the number of rows for the 10 biggest partitions in the table). 

  `dsbulk count -k ks1 -t table1 -stats partitions -partitions 100`

* Count the total number of rows, the total number of rows per token range, the total number of rows per hosts in the `ks1.table1`, and the total number of rows for the biggest 10 partitions table in a cluster with a `localhost` contact point. 

  `dsbulk count -k ks1 -t table1 -stats global,ranges,hosts,partitions`


## Command-line Help

Available settings along with defaults are documented [here](manual/settings.md), they are also
documented in DSBulk's [template configuration file](manual/application.template.conf) and in
the driver [template configuration file](manual/driver.template.conf). This information is also
available on the command-line via the `help` subcommand.

* Get help for common options and a list of sections from which more help is available:

  `dsbulk help`
  
* Get help for all `connector.csv` options:

  `dsbulk help connector.csv`
  
[onlineDocs]:https://docs.datastax.com/en/dsbulk/doc/
[HOCON]:https://github.com/lightbend/config/blob/master/HOCON.md