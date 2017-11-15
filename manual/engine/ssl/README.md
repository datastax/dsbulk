# Workflow Engine Encryption

DataStax Bulk Loader can connect to DSE clusters with SSL encryption enabled.

Two providers are available:
        
- `JDK`: uses JDK's standard SSLContext.
- `OpenSSL`: uses Netty's native support for OpenSSL.

Using `OpenSSL` provides better performance and generates less garbage. This is the recommended
provider when using SSL.

All the available configuration options are listed in the [Settings page].
        
For more information about how to configure SSL encryption, see:

* The [Java Secure Socket Extension (JSSE) Reference Guide](JSSE).
* The [DataStax Java driver documentation on SSL](http://docs.datastax.com/en/developer/java-driver//manual/ssl/).

[JSSE]: http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html
[driver ssl]: http://docs.datastax.com/en/developer/java-driver-dse/latest/manual/ssl/
[Settings page]: ../../settings.md

