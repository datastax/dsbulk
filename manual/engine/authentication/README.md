# Workflow Engine Authentication

DataStax Loader can connect to secured DSE clusters (and also to secured vanilla C* clusters).

Three different authentication providers are available:

- `PlainTextAuthProvider`:
    Uses [PlainTextAuthProvider] for authentication.
    Supports SASL authentication using the `PLAIN` mechanism (plain text authentication).
- `DsePlainTextAuthProvider`:
    Uses [DsePlainTextAuthProvider] for authentication.
    Supports SASL authentication to DSE clusters using the `PLAIN` mechanism 
    (plain text authentication), and also supports optional proxy authentication; 
    should be preferred to `PlainTextAuthProvider` when connecting to secured DSE clusters.
- `DseGSSAPIAuthProvider`:
    Uses [DseGSSAPIAuthProvider] for authentication.
    Supports SASL authentication to DSE clusters using the `GSSAPI` mechanism 
    (Kerberos authentication), and also supports optional proxy authentication.

All the available configuration options are listed in the [engine configuration] section.

[engine configuration]: ../configuration/
[PlainTextAuthProvider]: http://docs.datastax.com/en/drivers/java/latest/com/datastax/driver/core/PlainTextAuthProvider.html
[DsePlainTextAuthProvider]: http://docs.datastax.com/en/drivers/java-dse/latest/com/datastax/driver/dse/auth/DsePlainTextAuthProvider.html
[DseGSSAPIAuthProvider]: http://docs.datastax.com/en/drivers/java-dse/latest/com/datastax/driver/dse/auth/DseGSSAPIAuthProvider.html
