# DataStax Bulk Loader Mapping Parser

This module contains an ANTLR 4 grammar and parser for DSBulk's mapping syntax.

Most workflows require mapping a Record to a Statement, or a Row to a Record. In the former case,
fields in the record are mapped to bound variables in the statement; in the latter case, result
set variables are mapped to record fields. Such mapping can be expressed with the syntax defined
in this module.

## Mapping Syntax
    
Mappings should be specified as a map of the following form:

- Indexed data sources: `0 = col1, 1 = col2, 2 = col3`, where `0`, `1`, `2`, are the zero-based 
  indices of fields in the source data; and `col1`, `col2`, `col3` are bound variable names in the 
  insert statement.
    - A shortcut to map the first `n` fields is to simply specify the destination columns: 
    `col1, col2, col3`.
- Mapped data sources: `fieldA = col1, fieldB = col2, fieldC = col3`, where `fieldA`, `fieldB`, 
  `fieldC`, are field names in the source data; and `col1`, `col2`, `col3` are bound variable names 
  in the insert statement.
    - A shortcut to map fields named like columns is to simply specify the destination columns: 
    `col1, col2, col3`.

The exact type of mapping to use depends on the connector being used. Some connectors can only 
produce indexed records; others can only produce mapped ones, while others are capable of producing 
both indexed and mapped records at the same time. Refer to the connector's documentation to know 
which kinds of mapping it supports.

To specify that a field should be used as the timestamp (a.k.a. write-time) or ttl (a.k.a.
time-to-live) of the inserted row, use the following syntax: `fieldA = writetime(*), fieldB =
ttl(*)`.

If your record distinguishes different timestamps and ttls for each column in the table, you can map
such a record with the following syntax: `fieldA = writetime(col1), fieldB = ttl(col1), fieldC =
writetime(col2), fieldD = ttl(col2)`.

Note that timestamp fields are parsed as regular CQL timestamp columns by DSBulk. TTL fields are 
parsed as integers representing durations in seconds.

To specify that a column should be populated with the result of a function call, specify the 
function call as the input field (e.g. `now() = c4`). This is only possible when writing to the
database, not when reading. 

Similarly, to specify that a field should be populated with the result of a function call, specify 
the function call as the input column (e.g. `field1 = now()`). This is only possible when reading
from the database, not when writing. 

Function calls can also be qualified by a keyspace name: `field1 = ks1.max(c1,c2)`.

For mapped data sources, it is also possible to specify that the mapping be partly auto-generated
and partly explicitly specified. For example, if a source row has fields `c1`, `c2`, `c3`, and `c5`,
and the table has columns `c1`, `c2`, `c3`, `c4`, one can map all like-named columns and specify
that `c5` in the source maps to `c4` in the table as follows: `* = *, c5 = c4`.

One can specify that all like-named fields be mapped, except for `c2`: `* = -c2`. To skip `c2` and 
`c3`: `* = [-c2, -c3]`.

Any identifier, field or column, that is not strictly alphanumeric (i.e. not matching
`[a-zA-Z][a-zA-Z0-9_]*`) must be surrounded by double-quotes, just like you would do in CQL: `"Field
""A""" = "Column 2"` (to escape a double-quote, simply double it).

Note that, contrary to the CQL grammar, unquoted identifiers will be considered as is, and will not
be lower-cased: an identifier such as `MyColumn1` will match a column named `"MyColumn1"` and not
`mycolumn1`. This is also valid for function and keyspace identifiers: for example, to reference the
keyspace `"MyKeyspace"` and the function `"MyFunc"`, use `field1 = MyKeyspace.MyFunc(*)`. Beware
that some built-in functions have full lower-case internal names; for example, to use the built-in
function `dateOf`, use `field1 = dateof(now())` (note the lower-case name, since that's the internal
name of that function).

Finally, it is also possible to specify constant typed literals. A constant typed literal is a
mapping token of the form `(type) literal`, where `type` is a valid CQL native type, and `literal`
is a valid CQL literal for that type. Constant typed literals can appear on the left-hand side of a
mapping entry when loading, and on the right-hand side when unloading. Examples:
* Load 123 into `col1` of type `int` for all records: `(int)123 = col1`
* Unload `'2022-02-02T12:34:56Z'` as a `timestamp` value into field `field1` for all rows: `field1 =
  (timestamp)'2022-02-02T12:34:56Z'`. Note that unloading constant literals is only supported
  starting with C* 3.11.5.

Currently, only the following CQL types can be used in a constant typed literal:

* ascii
* bigint
* blob
* boolean
* counter
* decimal
* double
* float
* inet
* int
* text
* varchar
* timestamp
* date
* time
* uuid
* varint
* timeuuid
* tinyint
* smallint
* duration
