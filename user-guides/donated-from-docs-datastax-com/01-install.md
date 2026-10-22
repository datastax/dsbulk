# Install DataStax Bulk Loader (DSBulk)

DSBulk is open-source software that you can use to load, unload, and count database records and return related information.
You can load and unload CSV and JSON files with options for parsing and progress reporting.
Load/unload targets can be compressed or uncompressed, and they can be files, directories, stdin/stdout, or URLs.
Performance is optimized through multi-threaded operation, and you can tune various parameters to improve load and unload times.

You can use DSBulk as a standalone tool that connects to a local or remote cluster.
Remote connections don't require DSBulk to run locally on the cluster node, but this configuration is supported.

DSBulk requires a Java executable, as explained in [Post-installation requirements and recommendations](#dsbulkPostInstallRequirementsAndRecommendations).

## Compatibility

DataStax Bulk Loader (DSBulk) is supported on Linux, macOS, and Windows platforms, and it is compatible with the following databases:

- DataStax Astra DB
- DataStax Hyper-Converged Database (HCD)
- DataStax Enterprise (DSE) 5.1, 6.8, and 6.9
- Open-source Apache Cassandra® 2.1 and later

## Install on Linux or macOS

1. Download the DSBulk tarball or zip file from the [DSBulk GitHub repo](https://github.com/datastax/dsbulk/releases) or [Maven Central](https://central.sonatype.com/artifact/com.datastax.oss/dsbulk-distribution).
2. Unpack the archive, replacing **`VERSION`** with the version number of your downloaded file:

   ```bash
   tar -xzvf dsbulk-VERSION.tar.gz
   ```

3. Continue to [Post-installation requirements and recommendations](#dsbulkPostInstallRequirementsAndRecommendations).

## Install on Windows

1. Ensure you have Java installed on your Windows system.
2. Download the DSBulk zip file for Windows from the [DSBulk GitHub repo](https://github.com/datastax/dsbulk/releases).
   Select `.zip`, _not_ `.zip.asc`.
3. Extract the contents to a directory.
4. Optional: DSBulk attempts to find the Java executable automatically.
   Alternatively, you can manually configure Java for DSBulk to specify the Java executable to use.
   To do this, define the `JAVA_HOME` environment variable to specify which Java VM to use for DSBulk.
5. Adjust the `port-number` to your specific configuration.
   The default is `9042`.
6. Open a command prompt, navigate to `dsbulk-VERSION\bin` directory, replacing **`VERSION`** with the version number of your downloaded archive.
7. Run `dsbulk` commands from your DSBulk `\bin` directory.

   For example, to load data from a CSV file into your database:

   ```shell
   dsbulk load -url 'C:\\PATH_TO_CSV' -k KEYSPACE_NAME -t TABLE_NAME
   ```

   Replace the following:
   - **`PATH_TO_CSV`**: The path to the CSV file that you want to load.
   - **`KEYSPACE_NAME`** and **`TABLE_NAME`**: The keyspace and table in your database where you want to load the data.

   You must escape all backslashes (`\`) in Windows paths.
   For more information, see [Escaping command line arguments](02-usage.md#escape-and-quote-command-line-arguments).

8. Continue to [Post-installation requirements and recommendations](#dsbulkPostInstallRequirementsAndRecommendations).

## <a id="dsbulkPostInstallRequirementsAndRecommendations"></a>Post-installation requirements and recommendations

Review the following information to ensure your DSBulk installation functions as expected.

### Java executable

A Java executable is required.

On macOS, Linux, and *nix systems, the rules used to find a Java executable are as follows:

1. Use `$JAVA` if defined.
2. Use `${JAVA_HOME}/bin/java` if defined.
3. Use `$(/usr/libexec/java_home)/bin/java` if defined.
4. Use the first Java executable found on `$PATH`.

On Windows systems, the rules used to find a Java executable are as follows:

1. Use `%JAVA_HOME%\bin\java` if defined.
2. Use the first Java executable found on `$PATH`.

### Export DSBULK_JAVA_OPTS

You can pass system properties to the DSBulk process by exporting the environment variable `DSBULK_JAVA_OPTS`.
This step can be useful, for example, to configure JMX monitoring, or to configure advanced authentication schemes such as Kerberos.
For example, on a Linux system:

```bash
# Remote JMX configuration
export DSBULK_JAVA_OPTS="$DSBULK_JAVA_OPTS -Dcom.sun.management.jmxremote.port=port-number"

# Kerberos configuration
export DSBULK_JAVA_OPTS="$DSBULK_JAVA_OPTS -Djava.security.krb5.conf=configuration-path-and-filename"

# Invoke DSBulk
bin/dsbulk load -h host1.com -k ks1 -t table1 -url data.csv
```

### Prior package installs

If you previously used a package install of DSE on the node where you just installed `dsbulk`, and a prior version of `dsbulk` was included, such as 1.9.1.

1. After unpacking the latest version of `dsbulk` from the standalone tarball or zip file, update your `PATH` so that it points to the new version.

   For example, on a macOS node, edit your $HOME/.bashrc file, adding a command such as the following:

   ```bash
   export PATH=path/to/unpacked/location/dsbulk-VERSION/bin:$PATH
   ```

2. From the command line, execute your updated `.bashrc`, for example:

   ```bash
   source ~/.bashrc
   ```

3. Verify the `dsbulk` version:

   ```bash
   dsbulk --version
   ```

## Get started with DSBulk

1. [Create CQL tables](https://cassandra.apache.org/doc/5.0/cassandra/developing/cql/ddl.html#create-table-statement).

   The `dsbulk` commands can load, unload, and count data, but they cannot create keyspaces or tables.

2. Run some commands.
   Try [loading data](dsbulk-load.md), [unloading data](dsbulk-unload.md), and [counting data](dsbulk-count.md).
3. Explore available commands and options with `dsbulk help` and the DSBulk documentation.
