# Connecting DSBulk to your cluster

This guide covers the options that control how DSBulk connects to a Cassandra
cluster. They apply to every operation: [loading](loading.md),
[unloading](unloading.md) and [counting](counting.md). Read this guide first
if your cluster is not on your local machine, or if it requires a username,
a password, or TLS.

If you are connecting to Astra DB, see [cloud-and-astra.md](cloud-and-astra.md)
instead. It uses a secure connect bundle rather than a contact point.

The examples use a table named `iris_with_id` in a keyspace named
`dsbulkblog`, loaded from `/tmp/dsbulkblog/iris.csv`. Replace these with your
own values.

## Shortcuts and full option names

Most options have a short form, such as `-h`, and a long form, such as
`--datastax-java-driver.basic.contact-points`. They are interchangeable. For
driver options, you can also shorten `datastax-java-driver` to `driver`, so
`--driver.basic.contact-points` is the same option.

Settings that you use often can be saved in a configuration file instead of
being typed each time. See [Configuration file and
connector](#configuration-file-and-connector).

The full list of options is in [manual/settings.md](../manual/settings.md).

## Contact points and port

By default, DSBulk connects to a node on the local machine at `127.0.0.1`.
Use `-h` to name a different host, by IP address or by host name:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -h 127.0.0.1
```

You only need one contact point, because DSBulk discovers the other nodes
from it. It is a good idea to give more than one, so that DSBulk can start
if the first node is down. Pass a list:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -h '["192.168.0.1", "192.168.0.2"]'
```

If a name resolves to several addresses in DNS, DSBulk uses all of them.
Avoid `localhost` as a host name, because on some platforms it resolves to
both an IPv4 and an IPv6 address.

The default port is 9042. If your nodes listen on a different port, set it
with `-port`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -h 127.0.0.1 -port 9042
```

A contact point can also carry its own port, in the form `host:port`. Put
the value in quotes (`-h "host1:9042"`), because the command-line parser
rejects an unquoted colon. The `-port` value applies to any contact point
that does not. Cassandra 3 and
earlier require all nodes in a cluster to use the same port.

## Local data center and load balancing

DSBulk sends requests only to nodes in the local data center. If you do not
say which one that is, DSBulk infers it from your contact points. To set it
explicitly, use `-dc`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -h 10.0.0.1 -dc dc1
```

To control which nodes DSBulk may talk to, use `-allow` to list the hosts
that are permitted, or `-deny` to list the hosts that are not. Both take a
list in the same format as `-h`.

## Authentication

If your cluster requires a username and password, pass them with `-u` and
`-p`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -u cassandra -p cassandra
```

Be careful when you include a password on the command line. It can appear in
shell history, process listings, and system logs. There are two safer
alternatives:

- Give `-u` without `-p`. DSBulk prompts for the password. This works only in
  an interactive terminal. The prompt is controlled by the
  `runner.promptForPasswords` setting, which is `true` by default. Set it to
  `false` for unattended scripts, where there is no terminal to prompt on.
- Put the username and password in a configuration file with restricted file
  permissions. See [Configuration file and
  connector](#configuration-file-and-connector).

> **Note:** Kerberos (GSSAPI) and proxy authentication are available only
> when you connect to DataStax Enterprise. They are not covered in this guide.
> See `dsbulk help driver` and [manual/settings.md](../manual/settings.md) for
> the options.

## SSL/TLS

To encrypt the connection, tell DSBulk to use the default SSL engine factory
and give it a truststore:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -h cassandra.example.com \
  --driver.advanced.ssl-engine-factory.class DefaultSslEngineFactory \
  --driver.advanced.ssl-engine-factory.truststore-path /path/to/truststore.jks \
  --driver.advanced.ssl-engine-factory.truststore-password changeit
```

The related options are all under `driver.advanced.ssl-engine-factory`:

| Option | Purpose |
| --- | --- |
| `class` | Set to `DefaultSslEngineFactory` to turn on TLS. |
| `truststore-path` | Truststore that holds the server's certificate chain. |
| `truststore-password` | Password for the truststore. |
| `keystore-path` | Keystore that holds the client certificate, when the server requires client authentication. |
| `keystore-password` | Password for the keystore. |
| `hostname-validation` | Whether the server's certificate name must match its host name. Default is `true`. |
| `cipher-suites` | Cipher suites to enable, as a list. When unset, the JVM default is used. |

If you set neither `truststore-path` nor `keystore-path`, DSBulk uses the
JVM's default SSL context, which is configured through Java system
properties. Leave `hostname-validation` set to `true` unless you have a
specific reason to disable it. The truststore and keystore passwords are as
sensitive as the cluster password. Keep them in a configuration file, not on
the command line.

## Consistency level and retries

The default consistency level is `LOCAL_ONE`. Set another with `-cl`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -cl LOCAL_QUORUM
```

Valid values are `ANY`, `LOCAL_ONE`, `ONE`, `TWO`, `THREE`, `LOCAL_QUORUM`,
`QUORUM`, `EACH_QUORUM` and `ALL`. Stronger levels usually reduce throughput.

When a request fails, DSBulk retries it up to 10 times by default. Change that
with `-maxRetries`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -maxRetries 5
```

## Configuration file and connector

Every long option can be saved in a configuration file, so you do not need to
retype it. DSBulk reads `conf/application.conf` in its installation folder.
To use a different file, pass its path with `-f`:

```text
dsbulk load -f /path/to/my-cluster.conf -k dsbulkblog -t iris_with_id \
  -url /tmp/dsbulkblog/iris.csv
```

The file uses the HOCON format. In it, the options appear without their
leading dashes, grouped under `datastax-java-driver` for driver options and
`dsbulk` for everything else:

```text
datastax-java-driver {
  basic.contact-points = ["192.168.0.1", "192.168.0.2"]
  basic.load-balancing-policy.local-datacenter = dc1
  advanced.auth-provider {
    username = cassandra
    password = cassandra
  }
}
dsbulk {
  schema.keyspace = dsbulkblog
}
```

The repository includes templates that list every option with its default:
[manual/application.template.conf](../manual/application.template.conf) and
[manual/driver.template.conf](../manual/driver.template.conf). The templates
keep the driver settings in a separate `driver.conf` file that
`application.conf` includes. You can do the same, or declare the driver
settings directly in one file, as shown above.

Use `-c` to choose the connector, which controls the format of the data that
is read or written. The default is `csv`. The other choice is `json`:

```text
dsbulk load -c json -url /tmp/dsbulkblog/iris.json -k dsbulkblog \
  -t iris_with_id
```

## Getting help and the version

`dsbulk help` prints the global help. To see the options in one section, name
it:

```text
dsbulk help driver
```

For connector-specific help, give the connector before the section name:

```text
dsbulk help -c json codec
```

To print the DSBulk version, run:

```text
dsbulk --version
```

## Shortcut reference

| Group | Shortcuts |
| --- | --- |
| Connection | `-h`, `-port`, `-dc`, `-cl`, `-maxRetries`, `-u`, `-p`, `-allow`, `-deny` |
| Schema | `-k`, `-t`, `-m`, `-query`, `-timestamp`, `-ttl` |
| Data | `-c`, `-f`, `-url`, `-locale`, `-timeZone`, `-nullStrings` |
| Run control | `-dryRun`, `-maxConcurrentQueries`, `-maxErrors`, `-logDir`, `-verbosity` |
| Monitoring | `-jmx`, `-prometheus`, `-reportRate` |
| Counting | `-stats`, `-partitions` |

Some shortcuts apply only to certain connectors or operations. The
connector-specific ones, such as `-delim` and `-header` for CSV, are described
in the [loading guide](loading.md). There are further shortcuts for DataStax
Enterprise Graph. Run `dsbulk help` or see
[manual/settings.md](../manual/settings.md) for the complete list.

## Where to go next

- [Loading data](loading.md)
- [Unloading data](unloading.md)
- [Counting rows](counting.md)
- [Astra DB, cloud storage and migration](cloud-and-astra.md)
