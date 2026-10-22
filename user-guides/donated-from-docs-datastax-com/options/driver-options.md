# Driver options

The Apache Cassandra® Java driver is a dependency of DSBulk and it is included with your DSBulk installation.
You can find the Java driver version (`driver.version`) in the DSBulk [`pom.xml`](https://github.com/datastax/dsbulk/blob/1.x/pom.xml#L61) file.

This topic describes common and notable Java driver options that you can specify on the DSBulk command line or in configuration files.
Many more options exist.
For more information, see the [Apache Cassandra Java driver documentation](https://apache.github.io/cassandra-java-driver/).

If you are using an earlier version of DSBulk, refer to the documentation for the driver version included with that release, as specified in the DSBulk `pom.xml`.

## Synopsis

The `driver` options in DSBulk are derived from the Java driver itself.

When passing driver options to DSBulk on the command line, the standard form is `--driver.` followed by one or more segments, such as `--driver.basic.contact-points` or `--driver.basic.load-balancing-policy.evaluator.allow`.

For the values of DSBulk options, HOCON syntax rules apply unless otherwise noted.
For more information, see [Escape and quote DSBulk command line arguments](../02-usage.md#escape-and-quote-command-line-arguments).

### Short and long forms

On the command line, you can specify options in short form (if available), standard form, or long form.

For all `driver` options, the long form is prefixed by `datastax-java-driver` instead of `driver`, such as `--datastax-java-driver.basic.contact-points`.

The following examples show the same command with different forms of the `driver.basic.contact-points` option:

```bash
# Short form
dsbulk load -h '[ "192.168.0.1", "192.168.0.2" ]' -url filename.csv -k ks1 -t table1

# Standard form
dsbulk load --driver.basic.contact-points '[ "192.168.0.1", "192.168.0.2" ]' -url filename.csv -k ks1 -t table1

# Long form
dsbulk load --datastax-java-driver.basic.contact-points '[ "192.168.0.1", "192.168.0.2" ]' -url filename.csv -k ks1 -t table1
```

In [configuration files](../02-usage.md#use-a-configuration-file), you must use the long form with the `datastax-java-driver` prefix.
For example:

```json
datastax-java-driver.basic.contact-points = '[ "192.168.0.1", "192.168.0.2" ]'
```

### Executor options

<!-- dsbulk.executor options control how DSBulk uses and configures the driver at a high level. The only option that is actively used is the global continuous paging option. -->

The `executor` options are used to configure the Java driver.
However, these aren't `datastax-java-driver` options.

The standard form for these options is `--executor.*`, and the long form is `--dsbulk.executor.*`.

The only active `executor` option is [`--executor.continuousPaging.enabled`](#dsbulkExecutorOptionsContinuousPagingEnabled).
Other `executor` options are [deprecated](#deprecated-executor-options).

## <a id="basic-connection-options"></a>Basic connection options

These are the core driver connection options.
These options specify the type of cluster (self-managed or Astra DB), contact points, and ports.

See also [Authentication options](#authentication-options) and [SSL options](#driverOptionsSsl).

### <a id="driverBasicCloudSecureConnectBundle"></a>--driver.basic.cloud.secure-connect-bundle (-b)

This option is only for Astra DB and Mission Control CQL gateway connections.
For self-managed clusters that don't use a Mission Control CQL gateway, use [`--driver.basic.contact-points`](#driverBasicContactPoints) and [`--driver.basic.default-port`](#driverBasicDefaultPort).

When connecting to an Astra DB database or a Mission Control CQL gateway, you must provide the Secure Connect Bundle (SCB).
The value of `-b` is the path to your [Astra DB database's SCB](https://docs.datastax.com/en/astra-db-serverless/databases/secure-connect-bundle.html) or [Mission Control CQL gateway's SCB](https://docs.datastax.com/en/mission-control/databases/cql-gateway-connectivity.html#download-the-scb).
The SCB contains the necessary contact points, certificates, and keys to establish an SSL-encrypted connection to your database.
The driver extracts this information automatically.

The specified location must be a path on the local filesystem or a valid URL.
For example:

```bash
-b "/path/to/SCB.zip"        # Absolute path
-b "./path/to/SCB.zip"       # Relative path from the current working directory
-b "~/path/to/SCB.zip"       # Shorthand path from the user's $HOME directory
-b "C:\\path\\to\\SCB.zip"   # Microsoft Windows path with escaped backslashes
-b "file:/path/to/SCB.zip"   # URL with file protocol
-b "http://host.com/SCB.zip" # URL with HTTP protocol
```

> [!TIP]
> When `-b` is specified, the following options are ignored or unsupported because they are inferred from the SCB.
> Additionally, some options aren't applicable to Astra DB.
>
> - `--driver.basic.contact-points`
> - `--driver.basic.load-balancing-policy.evaluator.allow`
> - `--driver.basic.load-balancing-policy.evaluator.deny`
> - `--driver.basic.request.consistency` other than `LOCAL_QUORUM` with `dsbulk load`
> - All `--driver.advanced.ssl-engine-factory.*` options

Default: `null`

### <a id="driverBasicContactPoints"></a>--driver.basic.contact-points (-h)

When connecting to a self-managed cluster (DSE, HCD, or open-source Apache Cassandra®), specify the contact points to use for the initial connection to the cluster.

Contact points are addresses of cluster nodes that the driver uses to discover the cluster topology.
Only one contact point is required because the driver can retrieve the address of the other nodes automatically.
However, it is recommended that you provide multiple contact points in case a node is unavailable.
If there is only one contact point, and it is unavailable, then the driver cannot initialize correctly.

The default value depends on the type of cluster:

- For self-managed clusters, the default is `["127.0.0.1:9042"]`.
- For Astra DB, DSBulk automatically sets `--driver.basic.contact-points` to an empty list (`[]`) because the contact points are inferred from [`--driver.basic.cloud.secure-connect-bundle`](#driverBasicCloudSecureConnectBundle).

#### Contact points syntax

Provide contact points as a list of strings in the format of `host` or `host:port`:

```bash
# IPv4 addresses with or without ports
-h '["192.168.0.1:9042","192.168.0.2:9042"]'
-h '["192.168.0.1","192.168.0.2"]'

# IPv6 addresses with or without ports
-h '["fe80:0:0:0:f861:3eff:fe1d:9d7b:9042","fe80:0:0:f861:3eff:fe1d:9d7b:9044:9042"]'
-h '["fe80:0:0:0:f861:3eff:fe1d:9d7b","fe80:0:0:f861:3eff:fe1d:9d7b:9044"]'

# Host names with or without ports
-h '["host1.com:9042","host2.com:9042"]'
-h '["host1.com","host2.com"]'
```

If a host is specified without a port, then the driver uses the default port ([`--driver.basic.default-port`](#driverBasicDefaultPort)).

Address strings that contain special characters must be wrapped in quotes, such as `-h '["fe80::f861:3eff:fe1d:879e%en0"]'`.

#### Avoid ambiguous address resolution

If the host is a DNS name that resolves to multiple A-records, then all the corresponding addresses are used.
Don't use `localhost` as a host name because it resolves to both IPv4 and IPv6 addresses on some platforms.

The heuristic to determine whether a contact point is in the form `host` or `host:port` isn't guaranteed to be accurate for some IPv6 addresses.
Avoid ambiguous IPv6 address strings such as `fe80::f861:3eff:fe1d:1234` that could be interpreted as a combination of IP `fe80::f861:3eff:fe1d` with port `1234`, or as IP `fe80::f861:3eff:fe1d:1234` without a port.
In such cases, DSBulk doesn't change the contact point.

To avoid this issue, provide IPv6 addresses in full form.
For example, instead of `fe80::f861:3eff:fe1d:1234`, use `fe80:0:0:0:0:f861:3eff:fe1d:1234` so the string is parsed as IP `fe80:0:0:0:0:f861:3eff:fe1d` with port `1234`.

### <a id="driverBasicDefaultPort"></a>--driver.basic.default-port (-port)

The port to use for any host without a specified port in [`--driver.basic.contact-points`](#driverBasicContactPoints).

Cassandra 3.0 and earlier and DSE 6.7 and earlier require all nodes in a cluster to share the same port.

Default: `9042`

## <a id="advanced-connection-options"></a>Advanced connection options

Use the following options to configure connection pool settings, including sizes, limits, and timeouts.

### <a id="datastaxJavaDriverAdvancedAddressTranslatorClass"></a>--driver.advanced.address-translator.class

Use this option to set the address translator class.
If not qualified, the driver assumes that the class resides in the `com.datastax.oss.driver.internal.core.addresstranslation` package.

- **`PassThroughAddressTranslator` (default)**: Use the driver's built-in translator implementation that returns all addresses unchanged.
- **Custom class**: Specify a custom class that implements `AddressTranslator` and has a public constructor with a `DriverContext` argument.

### --driver.advanced.connection.connect-timeout

Set the timeout to establish a channel connection to the server.

This timeout controls how long the driver waits for the underlying channel to actually connect to the server.
This timeout doesn't apply to protocol negotiations, which can continue after the channel is established.

Default: `"30 seconds"`

### --driver.advanced.connection.init-query-timeout

Set the timeout for internal queries that run as part of the driver initialization process, immediately after opening a connection.

The timeout applies to each node connection.
The connection initialization process fails if this timeout is reached.

If the driver's first connection fails, the entire driver initialization fails.
For subsequent connections that fail, the driver retries the connection later.

Default: `"30 seconds"`

### <a id="datastaxJavaDriverAdvancedConnectionMaxRequestsPerConnection"></a>--driver.advanced.connection.max-requests-per-connection

Set the maximum number of requests that can be executed concurrently on a connection (local or remote).

Must be a positive integer between 1 and 32768.

Default: `32768`

### <a id="datastaxJavaDriverAdvancedConnectionPoolLocalSize"></a>--driver.advanced.connection.pool.local.size

Set the number of connections in the pool for nodes that are considered local.

Default: `8`

### <a id="datastaxJavaDriverAdvancedConnectionPoolRemoteSize"></a>--driver.advanced.connection.pool.remote.size

Set the number of connections in the pool for nodes that are considered remote.

This option has no effect when using the default DSBulk [load balancing policy](#load-balancing-policy-options) because this policy doesn't consider remote nodes.

Default: `8`

### <a id="datastaxJavaDriverAdvancedHeartbeatInterval"></a>--driver.advanced.heartbeat.interval

Set the heartbeat interval to keep connections alive.

If a connection is idle (no reads) for the duration of the heartbeat interval, then the driver sends a simulated request (heartbeat) over the connection to keep it alive.

If there is no activity, no heartbeat, or the [heartbeat expires](#datastaxJavaDriverAdvancedHeartbeatTimeout), then the connection is closed and replaced.

Default: `"1 minute"`

### <a id="datastaxJavaDriverAdvancedHeartbeatTimeout"></a>--driver.advanced.heartbeat.timeout

Set how long the driver waits for a response to a heartbeat before considering the connection expired.

If the driver doesn't receive a response within the timeout limit, then the connection is closed and replaced.

Default: `"1 minute"`

### <a id="datastaxJavaDriverAdvancedProtocolCompression"></a>--driver.advanced.protocol.compression

The name of the algorithm used to compress Cassandra Native Protocol frames:

- **`none` (default)**
- **`lz4`**
- **`snappy`**

### <a id="datastaxJavaDriverAdvancedProtocolVersion"></a>--driver.advanced.protocol.version

Set the Cassandra Native Protocol version to use:

- **Unset or `null` (default)**: If this option isn't set, the driver gets the node versions at startup (by default, it checks `system.peers.release_version`), and then it uses the highest common protocol version.

  For example, if you have a mixed cluster with Cassandra 2.1 nodes (protocol v3) and Cassandra 3.0 nodes (protocol v3 and v4), the driver chooses protocol v3.

  If the nodes don't have a common protocol version, initialization fails.

- **Protocol version**: If this option is set to a specific protocol version, the driver uses the given version for all connections without any negotiation or downgrading.

  If any of the [contact points](#basic-connection-options) don't support the specified protocol version, that contact point is skipped.

  Once the protocol version is set, it cannot change for the duration of the driver's session.
  If an incompatible node joins the cluster after initialization, the connection fails, and the driver doesn't try to reconnect to the node.

### <a id="datastaxJavaDriverAdvancedResolveContactPoints"></a>--driver.advanced.resolve-contact-points

How to resolve the addresses passed to [`--driver.basic.contact-points`](#driverBasicContactPoints):

- **`true` (default)**: Addresses are created with `InetSocketAddress(String, int)`.
  The host name is resolved at the initial connection, and then the driver uses the resolved IP address for all subsequent connection attempts.
- **`false`**: Addresses are created with `InetSocketAddress.createUnresolved()`.
  The host name is resolved each time the driver opens a new connection.

  This is useful for containerized environments where DNS records are more likely to change over time.

  Because the JVM and OS have their own DNS caching mechanisms, you might need additional configuration beyond the driver.

> [!IMPORTANT]
> This option has no effect on dynamically discovered peers.
>
> The driver relies on Cassandra system tables that expose raw IP addresses.
> To convert them to unresolved addresses, use a custom address translator in [`--driver.advanced.address-translator.class`](#datastaxJavaDriverAdvancedAddressTranslatorClass).
> For containerized environments, address translation is typically required by default, regardless of the presence of DSBulk.

## <a id="authentication-options"></a>Authentication options

Use these options to provide cluster authentication credentials for the Java driver used by DSBulk.

### <a id="datastaxJavaDriverAdvancedAuthProviderClass"></a>--driver.advanced.auth-provider.class

Use this option to set the authentication provider class:

- **`null` (default)**: Disable authentication.
  Only use this value if the cluster doesn't require authentication.
- **`PlainTextAuthProvider`**: This authentication provider implementation is included with the Java driver.
  If not qualified, the Java driver assumes that the class resides in either the `com.datastax.oss.driver.internal.core.auth` or `com.datastax.dse.driver.internal.core.auth` package.

  Use this class to authenticate with credentials set in [`--driver.advanced.auth-provider.username`](#datastaxJavaDriverAdvancedAuthProviderUsername) and [`--driver.advanced.auth-provider.password`](#datastaxJavaDriverAdvancedAuthProviderPassword).

  For DSE clusters only, proxy authentication is supported with [`--driver.advanced.auth-provider.authorization-id`](#datastaxJavaDriverAdvancedAuthProviderClassAuthorizationId).

- **`DseGssApiAuthProvider`**: This authentication provider implementation is included with the Java driver.
  If not qualified, the Java driver assumes that the class resides in either the `com.datastax.oss.driver.internal.core.auth` or `com.datastax.dse.driver.internal.core.auth` package.

  Use this class for GSSAPI authentication to DSE clusters secured with `DseAuthenticator`.
  For more information, see the javadocs for this authenticator.

- **Custom class**: Specify a custom class that implements `AuthProvider` and has a public constructor that takes a `DriverContext` argument.
  If not qualified, the Java driver assumes that the class resides in either the `com.datastax.oss.driver.internal.core.auth` or `com.datastax.dse.driver.internal.core.auth` package.

  To simplify customization, the Java driver provides two abstract classes that can be extended: `PlainTextAuthProviderBase` and `DseGssApiAuthProviderBase`.

### <a id="datastaxJavaDriverAdvancedAuthProviderUsername"></a>--driver.advanced.auth-provider.username (-u)

This option is required if [`--driver.advanced.auth-provider.class`](#datastaxJavaDriverAdvancedAuthProviderClass) is set to `PlainTextAuthProvider`.

For self-managed clusters with authentication enabled, provide a username for cluster authentication.
For Astra DB, use the literal string `"token"` as the username.

Strings that contain special characters must be quoted, and strings that contain double-quotes must be escaped.
For more information, see [Escape and quote command line arguments](../02-usage.md#escape-and-quote-command-line-arguments).

It is recommended that you specify credentials in a [configuration file](../02-usage.md#use-a-configuration-file) instead of on the command line.

Default: `null`

### <a id="datastaxJavaDriverAdvancedAuthProviderPassword"></a>--driver.advanced.auth-provider.password (-p)

This option is required if [`--driver.advanced.auth-provider.class`](#datastaxJavaDriverAdvancedAuthProviderClass) is set to `PlainTextAuthProvider`.

For self-managed clusters with authentication enabled, provide a password for cluster authentication.
For Astra DB, use an [application token](https://docs.datastax.com/en/astra-db-serverless/administration/manage-application-tokens.html) as the password.

Strings that contain special characters must be quoted, and strings that contain double-quotes must be escaped.
For more information, see [Escape and quote command line arguments](../02-usage.md#escape-and-quote-command-line-arguments).

It is recommended that you specify credentials in a [configuration file](../02-usage.md#use-a-configuration-file) instead of on the command line.

Default: `null`

### <a id="datastaxJavaDriverAdvancedAuthProviderClassAuthorizationId"></a>--driver.advanced.auth-provider.authorization-id

For DSE clusters, use this option for proxy authentication.

Set the authorization ID of the user to impersonate.

Default: `null`

## <a id="driverOptionsSsl"></a>SSL options

Use these options to enable [SSL](http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html) encrypted connections between self-managed clusters and the driver used by DSBulk.
For more information, see [Use SSL with DSBulk](../use-ssl.md) and the documentation for your driver.

> [!IMPORTANT]
> For Astra DB and Mission Control CQL gateway connections, the [`--driver.basic.cloud.secure-connect-bundle`](#driverBasicCloudSecureConnectBundle) option enables SSL encryption automatically.
> The driver extracts the required certificates and keys from the SCB, and it ignores all `--driver.advanced.ssl-engine-factory.*` options.

### <a id="datastaxJavaDriverAdvancedSslEngineFactoryClass"></a>--driver.advanced.ssl-engine-factory.class

> [!IMPORTANT]
> This option is for self-managed clusters with manually configured SSL only.
> Don't use this option when connecting to Astra DB or through a Mission Control CQL gateway.

Use this option to set the SSL engine factory class:

- **`null` (default)**: Disable SSL encryption.
- **`DefaultSslEngineFactory`**: Enable SSL encryption with the driver-provided implementation that uses the JDK's built-in SSL implementation.
  If not qualified, the driver assumes that the class resides in the `com.datastax.oss.driver.internal.core.ssl` package.
- **Custom class**: Enable SSL encryption with a custom class that implements `SslEngineFactory` and has a public constructor with a `DriverContext` argument.
  If not qualified, the driver assumes that the class resides in the `com.datastax.oss.driver.internal.core.ssl` package.

### --driver.advanced.ssl-engine-factory.cipher-suites

> [!IMPORTANT]
> This option is for self-managed clusters with manually configured SSL only.
> Don't use this option when connecting to Astra DB or through a Mission Control CQL gateway.

For the `DefaultSslEngineFactory` class only, provide a list of strings containing the cipher suites to enable when creating an `SSLEngine` for a connection.
For example, `--driver.advanced.ssl-engine-factory.cipher-suites '["TLS_RSA_WITH_AES_128_CBC_SHA","TLS_RSA_WITH_AES_256_CBC_SHA"]'`

If omitted, the driver won't explicitly enable cipher suites on the engine.
For more information, see the javadocs for [`SSLEngine.setEnabledCipherSuites()`](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/javax/net/ssl/SSLEngine.html#setEnabledCipherSuites(java.lang.String%5B%5D)).

Default: `null`

### <a id="datastaxJavaDriverAdvancedSslEngineFactoryHostnameValidation"></a>--driver.advanced.ssl-engine-factory.hostname-validation

> [!IMPORTANT]
> This option is for self-managed clusters with manually configured SSL only.
> Don't use this option when connecting to Astra DB or through a Mission Control CQL gateway.

For the `DefaultSslEngineFactory` class only, specify whether to validate that the server certificate's hostname matches the server being connected to:

- **`true` (default)**: Enable hostname validation.
- **`false`**: Disable hostname validation.

### <a id="datastaxJavaDriverAdvancedSslEngineFactoryKeystorePath"></a>--driver.advanced.ssl-engine-factory.keystore-path

> [!IMPORTANT]
> This option is for self-managed clusters with manually configured SSL only.
> Don't use this option when connecting to Astra DB or through a Mission Control CQL gateway.

For the `DefaultSslEngineFactory` class only, set the locations used to access keystore contents.

If you set either `--driver.advanced.ssl-engine-factory.keystore-path` or [`--driver.advanced.ssl-engine-factory.truststore-path`](#datastaxJavaDriverAdvancedSslEngineFactoryTruststorePath), then the driver builds an `SSLContext` from these files.

If neither of these options are specified, then the default `SSLContext` is used, which is based on system property configuration.

Default: `null`

### <a id="datastaxJavaDriverAdvancedSslEngineFactoryKeystorePassword"></a>--driver.advanced.ssl-engine-factory.keystore-password

> [!IMPORTANT]
> This option is for self-managed clusters with manually configured SSL only.
> Don't use this option when connecting to Astra DB or through a Mission Control CQL gateway.

If you set [`--driver.advanced.ssl-engine-factory.keystore-path`](#datastaxJavaDriverAdvancedSslEngineFactoryKeystorePath), then set the password to access the keystore.

Strings that contain special characters must be quoted, and strings that contain double-quotes must be escaped.
For more information, see [Escape and quote command line arguments](../02-usage.md#escape-and-quote-command-line-arguments).

It is recommended that you specify credentials in a [configuration file](../02-usage.md#use-a-configuration-file) instead of on the command line.

Default: `null`

### <a id="datastaxJavaDriverAdvancedSslEngineFactoryTruststorePath"></a>--driver.advanced.ssl-engine-factory.truststore-path

> [!IMPORTANT]
> This option is for self-managed clusters with manually configured SSL only.
> Don't use this option when connecting to Astra DB or through a Mission Control CQL gateway.

For the `DefaultSslEngineFactory` class only, set the locations used to access truststore contents.

If you set either [`--driver.advanced.ssl-engine-factory.keystore-path`](#datastaxJavaDriverAdvancedSslEngineFactoryKeystorePath) or `--driver.advanced.ssl-engine-factory.truststore-path`, then the driver builds an `SSLContext` from these files.

If neither of these options are specified, then the default `SSLContext` is used, which is based on system property configuration.

Default: `null`

### <a id="datastaxJavaDriverAdvancedSslEngineFactoryTruststorePassword"></a>--driver.advanced.ssl-engine-factory.truststore-password

> [!IMPORTANT]
> This option is for self-managed clusters with manually configured SSL only.
> Don't use this option when connecting to Astra DB or through a Mission Control CQL gateway.

If you set [`--driver.advanced.ssl-engine-factory.truststore-path`](#datastaxJavaDriverAdvancedSslEngineFactoryTruststorePath), then set the password to access the truststore.

Strings that contain special characters must be quoted, and strings that contain double-quotes must be escaped.
For more information, see [Escape and quote command line arguments](../02-usage.md#escape-and-quote-command-line-arguments).

It is recommended that you specify credentials in a [configuration file](../02-usage.md#use-a-configuration-file) instead of on the command line.

Default: `null`

## <a id="request-options"></a>Request options

Although all driver options inherently relate to query execution, these options are for specific aspects of query handling, such as consistency levels, idempotence, and retry policies.

### <a id="datastaxJavaDriverBasicRequestConsistency"></a>--driver.basic.request.consistency (-cl)

Set the consistency level to use for all queries executed by DSBulk.

Stricter consistency levels reduce throughput because they take longer to process and verify.
Typically, the strictest levels are used for write operations that must be written to multiple replicas before being acknowledged.

All levels other than `LOCAL_ONE` and `ONE` disable [continuous paging](#continuous-paging-options), which can reduce throughput for read requests (`dsbulk unload` and `dsbulk count`).
To use continuous paging, the consistency level must be `ONE` or `LOCAL_ONE`.

The accepted values, from less strict (faster) to more strict (slower), are as follows:

- `ANY`
- `LOCAL_ONE`
- `ONE`
- `TWO`
- `THREE`
- `LOCAL_QUORUM`
- `QUORUM`
- `EACH_QUORUM`
- `ALL`

The default value depends on the type of cluster:

- For self-managed clusters, the default value is `LOCAL_ONE`.
- For Astra DB, the default value is `LOCAL_QUORUM` for `dsbulk load`, and `LOCAL_ONE` for `dsbulk unload` and `dsbulk count`.

### <a id="datastaxJavaDriverBasicRequestDefaultIdempotence"></a>--driver.basic.request.default-idempotence

Set the default idempotence flag for all queries executed by DSBulk `load` operations:

- **`true` (default)**: All queries are considered idempotent by default.
  Write failures are retried for all queries according to the configured retry policy.
- **`false`**: All queries are considered non-idempotent by default.
  Write failures aren't retried.

### <a id="datastaxJavaDriverBasicRequestPageSize"></a>--driver.basic.request.page-size

Set the page size to control how many rows the driver retrieves simultaneously in a single network roundtrip.

This helps manage the amount of results stored in memory concurrently.
If there are more results, the driver sends additional requests to retrieve them.
This happens automatically when iterating with the sync API, or it happens explicitly with the async API's `fetchNextPage` method.

If you encounter out-of-memory errors with continuous paging, consider lowering this value.

To disable results paging, set this option to a negative integer or `0`.

Default: `5000`

To configure page size for continuous paging, see [Continuous paging options](#continuous-paging-options).

### <a id="datastaxJavaDriverBasicRequestSerialConsistency"></a>--driver.basic.request.serial-consistency

Set the serial consistency level:

- `LOCAL_SERIAL` (default)
- `SERIAL`

### <a id="datastaxJavaDriverBasicRequestTimeout"></a>--driver.basic.request.timeout

Set how long the driver waits for a request to complete.
This is a global limit on the duration of a `session.execute()` call from the driver, including any internal retries the driver handles.

Default: `"5 minutes"`

> [!TIP]
> The default value of 5 minutes is intentional.
>
> Unlike a web application where requests must be processed as fast as possible, DSBulk is a data loading and unloading tool that prioritizes throughput over latency.
> If you lower this value, you might encounter timeouts due to the large batch writes and long-running read queries that DSBulk performs.

### --driver.advanced.retry-policy.class

Set the retry policy class:

- **`MultipleRetryPolicy` (default)**: The default retry policy used by DSBulk is `"com.datastax.oss.dsbulk.workflow.commons.policies.retry.MultipleRetryPolicy"`.
  This is a special retry policy implementation that uses opinionated rules to retry most errors up to the limit set in [`--driver.advanced.retry-policy.max-retries`](#datastaxJavaDriverAdvancedRetryPolicyMaxRetries).
- **Custom class**: Specify a custom class that implements `RetryPolicy` and has a public constructor that takes two arguments: The `DriverContext` and a `String` representing the profile name.
  If not qualified, the driver assumes that the class resides in the `com.datastax.oss.driver.internal.core.retry` package.

### <a id="datastaxJavaDriverAdvancedRetryPolicyMaxRetries"></a>--driver.advanced.retry-policy.max-retries (-maxRetries)

For the `MultipleRetryPolicy` class only, set the number of times to retry a failed query.

Default: `10`

### <a id="datastaxJavaDriverAdvancedTimestampGeneratorClass"></a>--driver.advanced.timestamp-generator.class

Set the [microsecond timestamp generator](https://apache.github.io/cassandra-java-driver/4.19.0/core/query_timestamps/) class.
If it is not qualified, the driver assumes that the class resides in the `com.datastax.oss.driver.internal.core.time` package.

- **`AtomicTimestampGenerator` (default)**: This timestamp generator implementation is included with the Java driver.
  This class generates timestamps that are unique across _all_ client threads.
- **`ThreadLocalTimestampGenerator`**: This timestamp generator implementation is included with the Java driver.
  This class generates timestamps that are unique within each thread only.
  The same timestamp can be generated by different threads.
- **`ServerSideTimestampGenerator`**: This timestamp generator implementation is included with the Java driver.
  This class lets the server assign timestamps rather than the client threads.
- **Custom class**: Specify a custom class that implements `TimestampGenerator` and has a public constructor that takes two arguments: The `DriverContext` and a `String` representing the profile name.

## <a id="load-balancing-policy-options"></a>Load balancing policy options

The Java driver's [load balancing policy](https://apache.github.io/cassandra-java-driver/4.19.0/core/load_balancing/) controls the distribution of requests across a cluster.
For a given query execution, the load balancing policy determines the node that coordinates the query execution and the nodes that can be used as failover hosts, if any.

Typically, the default load balancing policy configuration is sufficient.

> [!IMPORTANT]
> When connecting to Astra DB or a Mission Control CQL gateway, always use the default values for the load balancing policy options.
> For Astra DB, the driver infers contact points, including the local datacenter, from the SCB ([`--driver.basic.cloud.secure-connect-bundle`](#driverBasicCloudSecureConnectBundle)).

### <a id="datastaxJavaDriverBasicLoadBalancingPolicyClass"></a>--driver.basic.load-balancing-policy.class

Set the load balancing policy class.
If not qualified, the driver assumes that the class resides in the `com.datastax.oss.driver.internal.core.loadbalancing` package.

- **`DcInferringLoadBalancingPolicy` (default)**: The default load balancing policy used by DSBulk is `"com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy"`.
  This is a special policy that infers the local datacenter from the [contact points](#basic-connection-options).
- **Custom class**: Specify a custom class that implements `LoadBalancingPolicy` and has a public constructor that takes two arguments: The `DriverContext` and a `String` representing the profile name.

### <a id="datastaxJavaDriverBasicLoadBalancingPolicyEvaluatorClass"></a>--driver.basic.load-balancing-policy.evaluator.class

Use this option if you want to provide an optional custom filter to include or exclude nodes from the load balancing policy.
DSBulk has a default node evaluator implementation that you can use without specifying a custom class.

If you choose to use this option, you must provide the fully-qualified name of a class that implements `java.util.function.Predicate<Node>` and has a public constructor that takes two arguments: The `DriverContext` instance and a `String` representing the current execution profile name.

The predicate's `test(Node)` method is invoked each time the policy processes a topology or state change.
If the method returns `false`, the node is set at distance `IGNORED`, which means the Java driver never connects to it, and the node is never included in query plans.

With the default DSBulk node evaluator implementation, you can further limit the available nodes with the [`--driver.basic.load-balancing-policy.evaluator.allow`](#datastaxJavaDriverBasicLoadBalancingPolicyEvaluatorAllow) and [`--driver.basic.load-balancing-policy.evaluator.deny`](#datastaxJavaDriverBasicLoadBalancingPolicyEvaluatorDeny) options.

Default: `"com.datastax.oss.dsbulk.workflow.commons.policies.lbp.SimpleNodeDistanceEvaluator"`

### <a id="datastaxJavaDriverBasicLoadBalancingPolicyEvaluatorAllow"></a>--driver.basic.load-balancing-policy.evaluator.allow (-allow)

> [!IMPORTANT]
> This option is for self-managed clusters only.
> Don't use this option when connecting to Astra DB.

To use this option, you must use the default DSBulk node evaluator implementation (See [`--driver.basic.load-balancing-policy.evaluator.class`](#datastaxJavaDriverBasicLoadBalancingPolicyEvaluatorClass)).

An optional list of node host names or addresses that are allowed to connect.

Host names and addresses can use any of the formats allowed in [`--driver.basic.contact-points`](#driverBasicContactPoints).

Default: `[]`

### <a id="datastaxJavaDriverBasicLoadBalancingPolicyEvaluatorDeny"></a>--driver.basic.load-balancing-policy.evaluator.deny (-deny)

> [!IMPORTANT]
> This option is for self-managed clusters only.
> Don't use this option when connecting to Astra DB.

To use this option, you must use the default DSBulk node evaluator implementation (See [`--driver.basic.load-balancing-policy.evaluator.class`](#datastaxJavaDriverBasicLoadBalancingPolicyEvaluatorClass)).

An optional list of node host names or addresses that aren't allowed to connect.

Host names and addresses can use any of the formats allowed in [`--driver.basic.contact-points`](#driverBasicContactPoints).

Default: `[]`

### <a id="datastaxJavaDriverBasicLoadBalancingPolicyLocalDatacenter"></a>--driver.basic.load-balancing-policy.local-datacenter (-dc)

Use this option to explicitly set the datacenter that is considered local for load balancing purposes.

The default load balancing policy for DSBulk (`DcInferringLoadBalancingPolicy`) infers the local datacenter from the [contact points](#basic-connection-options), and its query plans only include nodes from that datacenter.

When using the default policy, don't set this option unless inference fails or you want to override the inferred value.

Default: `null`

## <a id="continuous-paging-options"></a>Continuous paging options

Use these options to configure the Java driver's continuous paging settings.

Continuous paging can improve processing of results returned by read requests (`dsbulk unload` and `dsbulk count`), particularly for large datasets.
This feature isn't relevant to write requests (`dsbulk load`).

### <a id="dsbulkExecutorOptionsContinuousPagingEnabled"></a>--executor.continuousPaging.enabled

> [!IMPORTANT]
> `--executor.continuousPaging.enabled` is an [`executor`](#executor-options) option, not a `datastax-java-driver` option.
> The long form of this option is `--dsbulk.executor.continuousPaging.enabled`.

Globally enable or disable continuous paging for read request results (`dsbulk unload` and `dsbulk count`):

- **`true` (default)**: Enable continuous paging for eligible read requests.

  To use continuous paging, the target cluster must support continuous paging, and the request consistency level ([`--driver.basic.request.consistency`](#datastaxJavaDriverBasicRequestConsistency)) must be either `ONE` or `LOCAL_ONE`.
  If either condition isn't met, then the driver falls back to traditional paging.

  You can use the `--driver.advanced.continuous-paging.*` options to tune the continuous paging behavior.

  Make sure [`--engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries) doesn't exceed the cluster's resources.
  If `--engine.maxConcurrentQueries` is too high, requests can be rejected due to overloaded server resources.

- **`false`**: Disable continuous paging.
  The driver ignores all `--driver.advanced.continuous-paging.*` options, and it uses traditional paging for all read requests.

### <a id="datastaxJavaDriverAdvancedContinuousPagingMaxEnqueuedPages"></a>--driver.advanced.continuous-paging.max-enqueued-pages

To use this option, you must enable continuous paging with [`--dsbulk.executor.continuousPaging.enabled true`](#dsbulkExecutorOptionsContinuousPagingEnabled).

Set the maximum number of pages that can be stored in the local queue.

This option must be set to a positive integer.

Default: `4`

### <a id="datastaxJavaDriverAdvancedContinuousPagingMaxPages"></a>--driver.advanced.continuous-paging.max-pages

To use this option, you must enable continuous paging with [`--dsbulk.executor.continuousPaging.enabled true`](#dsbulkExecutorOptionsContinuousPagingEnabled).

Set the maximum number of pages to return in total:

- **`0` (default)**: Retrieve all pages.
- **Positive integer**: Retrieve no more than this number of pages.
  All pages beyond this limit are ignored.

### <a id="datastaxJavaDriverAdvancedContinuousPagingMaxPagesPerSecond"></a>--driver.advanced.continuous-paging.max-pages-per-second

To use this option, you must enable continuous paging with [`--dsbulk.executor.continuousPaging.enabled true`](#dsbulkExecutorOptionsContinuousPagingEnabled).

Set the maximum number of pages to retrieve per second:

- **`0` (default)**: Retrieve an unlimited number of pages per second.
- **Positive integer**: Retrieve no more than this number of pages per second.

### <a id="datastaxJavaDriverAdvancedContinuousPagingPageSize"></a>--driver.advanced.continuous-paging.page-size

To use this option, you must enable continuous paging with [`--dsbulk.executor.continuousPaging.enabled true`](#dsbulkExecutorOptionsContinuousPagingEnabled).

Set the page size for continuous paging as a number of rows or bytes.
The unit (rows or bytes) is set by [`--driver.advanced.continuous-paging.page-size-in-bytes`](#datastaxJavaDriverAdvancedContinuousPagingPageSizeInBytes).
For example, if you use the default values for both options, then the driver retrieves 5,000 rows at a time when using continuous paging.

Specifically, this is the number of rows or bytes that the driver retrieves simultaneously in a single network roundtrip.
These options help manage the amount of results stored in memory concurrently during continuous paging.
If there are more results, the driver sends additional requests to retrieve them.
This happens automatically when iterating with the sync API, or it happens explicitly with the async API's `fetchNextPage` method.

If you encounter out-of-memory errors with continuous paging, consider lowering this value.

Default: `5000`

### <a id="datastaxJavaDriverAdvancedContinuousPagingPageSizeInBytes"></a>--driver.advanced.continuous-paging.page-size-in-bytes

To use this option, you must enable continuous paging with [`--dsbulk.executor.continuousPaging.enabled true`](#dsbulkExecutorOptionsContinuousPagingEnabled).

Set the unit, rows or bytes, for the [`--driver.advanced.continuous-paging.page-size`](#datastaxJavaDriverAdvancedContinuousPagingPageSize) option.
For example, if you use the default values for both options, then the driver retrieves 5,000 rows at a time.
If you want to driver to retrieve 5,000 bytes at a time instead, set this option to `true`.

- **`false` (default)**: Page size is interpreted as a number of rows.
- **`true`**: Page size is interpreted as a number of bytes.

> [!TIP]
> The ideal page size and unit depends on your use case, including the available memory and the dataset's characteristics.
> Because DSBulk is designed for high throughput, the following examples assume that an `unload` or `count` operation is meant to read many rows or an entire table:
>
> - **Low row limit**: If your dataset has large rows, and the row size (in memory) is generally consistent, you might choose to return fewer rows per page.
> - **High row limit**: If your dataset is small or has lightweight rows (in terms of memory), you might choose to return many rows per page.
> - **Byte limit**: If the row size is inconsistent (a mix of large rows and lightweight rows), you might choose to use a byte-based page limit (`--driver.advanced.continuous-paging.page-size-in-bytes true`).
>   This can be more performant than a row-based page size where a group of large rows could overwhelm the available memory unexpectedly.

### <a id="datastaxJavaDriverAdvancedContinuousPagingTimeoutFirstPage"></a>--driver.advanced.continuous-paging.timeout.first-page

To use this option, you must enable continuous paging with [`--dsbulk.executor.continuousPaging.enabled true`](#dsbulkExecutorOptionsContinuousPagingEnabled).

Specify how long the driver should wait for the coordinator node to send the first page.

Default: `"5 minutes"`

### <a id="datastaxJavaDriverAdvancedContinuousPagingTimeoutOtherPages"></a>--driver.advanced.continuous-paging.timeout.other-pages

To use this option, you must enable continuous paging with [`--dsbulk.executor.continuousPaging.enabled true`](#dsbulkExecutorOptionsContinuousPagingEnabled).

Specify how long the driver should wait for the coordinator node to send subsequent pages (after the first page).

Default: `"5 minutes"`

## Metrics options

Use these options to enable driver metrics collection by DSBulk.
For more information, see [Monitoring options](monitor-options.md).

### <a id="datastaxJavaDriverAdvancedMetricsNodeEnabled"></a>--driver.advanced.metrics.node.enabled

If you [enable JMX reporting](monitor-options.md#monitoring-jmx), provide a list of node-level metrics to monitor:

- `bytes-received`
- `bytes-sent`
- `cql-messages`
- `errors.connection.auth`
- `errors.connection.init`
- `errors.request.aborted`
- `errors.request.others`
- `errors.request.read-timeouts`
- `errors.request.unavailables`
- `errors.request.unsent`
- `errors.request.write-timeouts`
- `ignores.aborted`
- `ignores.other`
- `ignores.read-timeout`
- `ignores.total`
- `ignores.unavailable`
- `ignores.write-timeout`
- `pool.in-flight`
- `pool.open-connections`
- `retries.aborted`
- `retries.other`
- `retries.read-timeout`
- `retries.total`
- `retries.unavailable`
- `retries.write-timeout`

Default: `[]` (no node-level metrics monitored)

### <a id="datastaxJavaDriverAdvancedMetricsSessionEnabled"></a>--driver.advanced.metrics.session.enabled

If you [enable JMX reporting](monitor-options.md#monitoring-jmx), provide a list of session-level metrics to monitor:

- `bytes-received`
- `bytes-sent`
- `connected-nodes`
- `cql-client-timeouts`
- `cql-requests`

Default: `[]` (no session-level metrics monitored)

## Deprecated driver options

- **--driver.addressTranslator**: Deprecated. Use [`--driver.advanced.address-translator.class`](#datastaxJavaDriverAdvancedAddressTranslatorClass).

- **--driver.auth.authorizationId**: Deprecated. Use [`--driver.advanced.auth-provider.authorization-id`](#datastaxJavaDriverAdvancedAuthProviderClassAuthorizationId).

- **--driver.auth.keyTab**: Deprecated. Use [`--driver.advanced.auth-provider.class`](#datastaxJavaDriverAdvancedAuthProviderClass) and the related `--driver.advanced.auth-provider.*` settings.

- **--driver.auth.password (-p)**: Deprecated. Use [`--driver.advanced.auth-provider.password`](#datastaxJavaDriverAdvancedAuthProviderPassword).

- **--driver.auth.principal**: Deprecated. Use [`--driver.advanced.auth-provider.class`](#datastaxJavaDriverAdvancedAuthProviderClass) and the related `--driver.advanced.auth-provider.*` settings.

- **--driver.auth.provider**: Deprecated. Use [`--driver.advanced.auth-provider.class`](#datastaxJavaDriverAdvancedAuthProviderClass).

- **--driver.auth.saslService**: Deprecated. Use [`--driver.advanced.auth-provider.class`](#datastaxJavaDriverAdvancedAuthProviderClass) and the related `--driver.advanced.auth-provider.*` settings.

- **--driver.auth.username (-u)**: Deprecated. Use [`--driver.advanced.auth-provider.username`](#datastaxJavaDriverAdvancedAuthProviderUsername).

- **--driver.policy.lbp.dcAwareRoundRobin.allowRemoteDCsForLocalConsistencyLevel**: Deprecated. See [`--driver.basic.load-balancing-policy.class`](#datastaxJavaDriverBasicLoadBalancingPolicyClass).

- **--driver.policy.lbp.dcAwareRoundRobin.localDc**: Deprecated. Use [`--driver.basic.load-balancing-policy.local-datacenter`](#datastaxJavaDriverBasicLoadBalancingPolicyLocalDatacenter).

- **--driver.policy.lbp.dcAwareRoundRobin.usedHostsPerRemoteDc**: Deprecated. See [`--driver.basic.load-balancing-policy.class`](#datastaxJavaDriverBasicLoadBalancingPolicyClass).

- **--driver.policy.lbp.dse.childPolicy**: Deprecated. See [`--driver.basic.load-balancing-policy.class`](#datastaxJavaDriverBasicLoadBalancingPolicyClass).

- **--driver.policy.lbp.name (-lbp)**: Deprecated. Use [`--driver.basic.load-balancing-policy.class`](#datastaxJavaDriverBasicLoadBalancingPolicyClass).

- **--driver.policy.lbp.tokenAware.childPolicy**: Deprecated. See [`--driver.basic.load-balancing-policy.class`](#datastaxJavaDriverBasicLoadBalancingPolicyClass).

- **--driver.policy.lbp.whiteList.childPolicy**: Deprecated. See [`--driver.basic.load-balancing-policy.class`](#datastaxJavaDriverBasicLoadBalancingPolicyClass).

- **--driver.policy.lbp.whiteList.hosts**: Deprecated. Use [`--driver.basic.load-balancing-policy.evaluator.class`](#datastaxJavaDriverBasicLoadBalancingPolicyEvaluatorClass) and [`--driver.basic.load-balancing-policy.evaluator.allow`](#datastaxJavaDriverBasicLoadBalancingPolicyEvaluatorAllow).

- **--driver.policy.maxRetries (-maxRetries)**: Deprecated. Use [`--driver.advanced.retry-policy.max-retries`](#datastaxJavaDriverAdvancedRetryPolicyMaxRetries).

- **--driver.pooling.heartbeat**: Deprecated. Use [`--driver.advanced.heartbeat.interval`](#datastaxJavaDriverAdvancedHeartbeatInterval).

- **--driver.pooling.local.connections**: Deprecated. Use [`--driver.advanced.connection.pool.local.size`](#datastaxJavaDriverAdvancedConnectionPoolLocalSize).

- **--driver.pooling.local.requests**: Deprecated. Use [`--driver.advanced.connection.max-requests-per-connection`](#datastaxJavaDriverAdvancedConnectionMaxRequestsPerConnection).

- **--driver.pooling.remote.connections**: Deprecated. Use [`--driver.advanced.connection.pool.remote.size`](#datastaxJavaDriverAdvancedConnectionPoolRemoteSize).

- **--driver.pooling.remote.requests**: Deprecated. Use [`--driver.advanced.connection.max-requests-per-connection`](#datastaxJavaDriverAdvancedConnectionMaxRequestsPerConnection).

- **--driver.protocol.compression**: Deprecated. Use [`--driver.advanced.protocol.compression`](#datastaxJavaDriverAdvancedProtocolCompression).

- **--driver.query.consistency (-cl)**: Deprecated. Use [`--driver.basic.request.consistency`](#datastaxJavaDriverBasicRequestConsistency).

- **--driver.query.fetchSize**: Deprecated. Use [`--driver.basic.request.page-size`](#datastaxJavaDriverBasicRequestPageSize).

- **--driver.query.idempotence**: Deprecated. Use [`--driver.basic.request.default-idempotence`](#datastaxJavaDriverBasicRequestDefaultIdempotence).

- **--driver.query.serialConsistency**: Deprecated. Use [`--driver.basic.request.serial-consistency`](#datastaxJavaDriverBasicRequestSerialConsistency).

- **--driver.socket.readTimeout**: Deprecated. Use [`--driver.basic.request.timeout`](#datastaxJavaDriverBasicRequestTimeout).

- **--driver.ssl.cipherSuites**: Deprecated. Use [`--driver.advanced.ssl-engine-factory.class`](#datastaxJavaDriverAdvancedSslEngineFactoryClass) and the related `datastax-java-driver.advanced.ssl-engine-factory.*` options.

- **--driver.ssl.keystore.algorithm**: Deprecated. Use [`--driver.advanced.ssl-engine-factory.class`](#datastaxJavaDriverAdvancedSslEngineFactoryClass) and the related `datastax-java-driver.advanced.ssl-engine-factory.*` options.

- **--driver.ssl.keystore.password**: Deprecated. Use [`--driver.advanced.ssl-engine-factory.keystore-password`](#datastaxJavaDriverAdvancedSslEngineFactoryKeystorePassword).

- **--driver.ssl.keystore.path**: Deprecated. Use [`--driver.advanced.ssl-engine-factory.keystore-path`](#datastaxJavaDriverAdvancedSslEngineFactoryKeystorePath).

- **--driver.ssl.openssl.keyCertChain**: Deprecated. Use [`--driver.advanced.ssl-engine-factory.class`](#datastaxJavaDriverAdvancedSslEngineFactoryClass) and the related `datastax-java-driver.advanced.ssl-engine-factory.*` options.

- **--driver.ssl.openssl.privateKey**: Deprecated. Use [`--driver.advanced.ssl-engine-factory.class`](#datastaxJavaDriverAdvancedSslEngineFactoryClass) and the related `datastax-java-driver.advanced.ssl-engine-factory.*` options.

- **--driver.ssl.provider**: Deprecated. Use [`--driver.advanced.ssl-engine-factory.class`](#datastaxJavaDriverAdvancedSslEngineFactoryClass) and the related `datastax-java-driver.advanced.ssl-engine-factory.*` options.

- **--driver.ssl.truststore.algorithm**: Deprecated. Use [`--driver.advanced.ssl-engine-factory.class`](#datastaxJavaDriverAdvancedSslEngineFactoryClass) and the related `datastax-java-driver.advanced.ssl-engine-factory.*` options.

- **--driver.ssl.truststore.password**: Deprecated. Use [`--driver.advanced.ssl-engine-factory.truststore-password`](#datastaxJavaDriverAdvancedSslEngineFactoryTruststorePassword).

- **--driver.ssl.truststore.path**: Deprecated. Use [`--driver.advanced.ssl-engine-factory.truststore-path`](#datastaxJavaDriverAdvancedSslEngineFactoryTruststorePath).

- **--driver.timestampGenerator**: Deprecated. Use [`--driver.advanced.timestamp-generator.class`](#datastaxJavaDriverAdvancedTimestampGeneratorClass).

## Deprecated executor options

The following `dsbulk.executor` options were used to configure driver concurrency and throughput in earlier versions of DSBulk.

These options are deprecated in favor of built-in `datastax-java-driver` options or `dsbulk.engine` options that provide equal or better functionality.

If your DSBulk configuration uses these options, replace them with their suggested alternatives.

Some deprecated options are still supported but disabled by default.
Others no longer have any effect.
If you use a non-functional deprecated option, DSBulk ignores it and logs a warning.

- <a id="executor-continuousPaging-maxConcurrentQueries"></a>**--executor.continuousPaging.maxConcurrentQueries**: Deprecated. Use [`--dsbulk.engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries).

- **--executor.continuousPaging.maxPages**: Deprecated. Use [`--driver.advanced.continuous-paging.max-pages`](#datastaxJavaDriverAdvancedContinuousPagingMaxPages).

- **--executor.continuousPaging.maxPagesPerSecond**: Deprecated. Use [`--driver.advanced.continuous-paging.max-pages-per-second`](#datastaxJavaDriverAdvancedContinuousPagingMaxPagesPerSecond).

- **--executor.continuousPaging.pageSize**: Deprecated. Use [`--driver.advanced.continuous-paging.page-size`](#datastaxJavaDriverAdvancedContinuousPagingPageSize).

- **--executor.continuousPaging.pageUnit**: Deprecated. Use [`--driver.advanced.continuous-paging.page-size-in-bytes`](#datastaxJavaDriverAdvancedContinuousPagingPageSizeInBytes).

- <a id="dsbulkExecutorOptionsMaxInFlight"></a>**--executor.maxInFlight**:

  > [!IMPORTANT]
  > `--executor.maxInFlight` creates a semaphore that blocks the driver under high contention.
  > Therefore, it is recommended that you use [`--dsbulk.engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries), which achieves the same effect without blocking the driver.

  This option sets a soft global throughput limit on the maximum number of _concurrent operations in flight_, which are concurrent requests waiting for a response from the server:

  - **Positive number**: Set a soft limit on the maximum number of operations in flight.
    This acts as a safeguard to prevent more requests than the cluster can handle.
    Make sure this value is within the bounds of the cluster's throughput capacity.
    If this value is too high, the cluster can get overloaded, leading to out-of-memory errors, increased latency, and timeouts.

    This is considered a soft limit due to the way pending requests are counted.
    For `load` operations, a [batch](batch-options.md) is considered one request, which means that the actual number of pending requests can exceed this value if you are using batching.
    For `unload` and `count` operations, each request for the next page of results is considered one request.

    To set a fixed throughput limit, use [`--dsbulk.engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries) (recommended) or [`--dsbulk.executor.maxPerSecond`](#dsbulkExecutorOptionsMaxPerSecond).

  - **Zero or negative number (default)**: Disables this option.
    Throughput can still be limited through other options, such as [`--dsbulk.engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries).

  Astra DB rate limits are always enforced on the Astra DB server side, regardless of the DSBulk configuration.
  If your `dsbulk` commands connect to an Astra DB database, make sure this limit doesn't exceed the Astra DB rate limit.

- **--executor.maxBytesPerSecond**:

  > [!IMPORTANT]
  > `--executor.maxBytesPerSecond` creates a semaphore that blocks the driver under high contention.
  > Therefore, it is recommended that you use [`--dsbulk.engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries), which achieves a similar effect without blocking the driver.

  This option sets a fixed global throughput limit on the maximum number of bytes processed per second:

  - **Enabled**: Set a fixed maximum number of bytes per second as a valid long integer or in HOCON size-in-bytes format.
    For example, `1234`, `1K`, or `5 kibibytes`.

    This acts as a safeguard to prevent more requests than the cluster can handle.
    Make sure this value is within the bounds of the cluster's throughput capacity.
    If this value is too high, the cluster can get overloaded, leading to out-of-memory errors, increased latency, and timeouts.

    This setting applies to all operations.
    For `load` operations, this includes bytes written to the database, and for `unload` and `count` operations, this includes bytes read from the database.

    To set a soft or variable throughput limit, use [`--dsbulk.engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries) (recommended) or [`--dsbulk.executor.maxInFlight`](#dsbulkExecutorOptionsMaxInFlight).

  - **Disabled (default)**: Set to `0` or a negative number to disable this option.
    Throughput can still be limited through other options, such as [`--dsbulk.engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries).

  Astra DB rate limits are always enforced on the Astra DB server side, regardless of the DSBulk configuration.
  If your `dsbulk` commands connect to an Astra DB database, make sure this limit doesn't exceed the Astra DB rate limit.

- <a id="dsbulkExecutorOptionsMaxPerSecond"></a>**--executor.maxPerSecond**

  > [!IMPORTANT]
  > `--executor.maxPerSecond` creates a semaphore that blocks the driver under high contention.
  > Therefore, it is recommended that you use [`--dsbulk.engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries), which achieves the same effect without blocking the driver.

  This option sets a fixed global throughput limit on the maximum number of concurrent operations per second:

  - **Positive number**: Set a fixed maximum number of operations per second.
    This acts as a safeguard to prevent more requests than the cluster can handle.
    Make sure this value is within the bounds of the cluster's throughput capacity.
    If this value is too high, the cluster can get overloaded, leading to increased latency and timeouts.

    This setting applies to all operations.
    For `load` operations, it limits the maximum number of writes per second, which includes nested statements inside a [batch](batch-options.md).
    For `unload` and `count` operations, it limits the number of rows retrieved per second.

    To set a soft or variable throughput limit, use [`--dsbulk.engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries) (recommended) or [`--dsbulk.executor.maxInFlight`](#dsbulkExecutorOptionsMaxInFlight).

  - **Zero or negative number (default)**: Disables this option.
    Throughput can still be limited through other options, such as [`--dsbulk.engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries).

  Astra DB rate limits are always enforced on the Astra DB server side, regardless of the DSBulk configuration.
  If your `dsbulk` commands connect to an Astra DB database, make sure this limit doesn't exceed the Astra DB rate limit.
