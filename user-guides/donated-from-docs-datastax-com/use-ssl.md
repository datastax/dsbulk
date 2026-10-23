# Use SSL with DSBulk

How you use SSL encrypted connections with DSBulk depends on your database platform.

## Enable SSL encrypted connections for DSE

1. Enable SSL on your cluster.
2. Pass [`--driver.advanced.ssl-engine-factory.*`](options/driver-options.md#driverOptionsSsl) options to the Java driver used by DSBulk.

   You can do this on the command line, but a [configuration file](02-usage.md#use-a-configuration-file) is recommended.

   For example, in your configuration file, you can enclose the Java driver configuration options in a `datastax-java-driver` block:

   ```json
   datastax-java-driver {
     advanced {
       ssl-engine-factory {
         keystore-password = "cassandra"
         keystore-path = "/Users/myaccount/tmp/ssl/keystore.node0"
         class = DefaultSslEngineFactory
         truststore-password = "dse#r0cks!"
         truststore-path = "/Users/myaccount/tmp/ssl/truststore.node0"
       }
     }
   }
   ```

   Alternatively, you can create a separate configuration file, such as `driver.conf`, that contains all the `datastax-java-driver` properties.
   Then, reference that file in your primary DSBulk configuration file using HOCON-formatted file inclusions, such as `include classpath("driver.conf")`.
   For more information, see [Use a configuration file](02-usage.md#use-a-configuration-file).

3. Recommended: Add [connection options](options/driver-options.md#basic-connection-options) and [authentication options](options/driver-options.md#authentication-options) to your DSBulk configuration files.
4. Store your configuration files in the `conf` directory of your DSBulk installation, or use the `-f` option to select the primary configuration file from another location.

   The following example selects a configuration file relative to the current working directory.
   If there are multiple DSBulk configuration files, the selected file must have `include` statements referencing the other files.

   ```bash
   dsbulk load -f "../my-application.conf" -url file1.csv -k ks1 -t table1
   ```

## Enable SSL encryption for Astra DB and Mission Control CQL gateway connections

For Astra DB and Mission Control CQL gateway connections, the [`--driver.basic.cloud.secure-connect-bundle`](options/driver-options.md#driverBasicCloudSecureConnectBundle) (`-b`) option enables SSL encryption automatically.

The driver extracts the required certificates and keys from the Secure Connect Bundle (SCB), and it ignores all [`--driver.advanced.ssl-engine-factory.*`](options/driver-options.md#driverOptionsSsl) options.

It is recommended that you store [connection options](options/driver-options.md#basic-connection-options) and [authentication options](options/driver-options.md#authentication-options) in a DSBulk [configuration file](02-usage.md#use-a-configuration-file).
