# DataStax Bulk Loader/Unloader CQL connector

This library provides a [`CqlScriptReader`](api/src/main/java/com/datastax/oss/driver/bulk/api/cql/CqlScriptReader.java) 
class, i.e. a `Reader` for CQL script files.

The reader can operate in two modes:

1. Single-line: each line is assumed to contain one single, complete statement.
2. Multi-line: statements can span accross multiple lines.

The single-line mode is more reliable and considerably faster (about 10 times faster) 
then the multi-line mode.

The multi-line mode, however, is the only suitable mode if the script contains statements 
spanning multiple lines, because it actually performs a high-level parsing and is capable 
of detecting statement boundaries in the file. It is compatible with all versions of the CQL grammar.

This library also provides subclasses of `CqlScriptReader` that operate in reactive style: 

1. [`RxJavaCqlScriptReader`](rxjava/src/main/java/com/datastax/oss/driver/bulk/api/cql/RxJavaCqlScriptReader.java).
2. [`ReactorCqlScriptReader`](reactor/src/main/java/com/datastax/oss/driver/bulk/api/cql/ReactorCqlScriptReader.java)

### CQL Script Reader - Examples

Single line reader example:

```java
try (CqlScriptReader reader =
            new CqlScriptReader(
                    new FileReader("videodb-inserts.cql"))) {
    
   reader.readStream().forEach(session::execute);

   } catch (IOException e) {
   e.printStackTrace();
}
```

To create a multi-line reader, simply pass `true` to the second constructor parameter:

```java
try (CqlScriptReader reader =
            new CqlScriptReader(
                    new FileReader("videodb-schema.cql"), true)) {

   reader.readStream().forEach(session::execute);

} catch (IOException e) {
   e.printStackTrace();
}
```

Reactive subclasses can also emit statements in a reactive fashion via the method 
`readReactive()` thant returns a `Publisher<Statement>`:

```java
BulkExecutor executor = ...;
try (AbstractReactiveCqlScriptReader reader =
          new RxJavaCqlScriptReader(               
                new FileReader("videodb-inserts.cql"), true)) {
    
   executor.writeReactive(reader.readReactive());
   
} catch (IOException e) {
   e.printStackTrace();
}
```
