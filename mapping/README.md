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

### Inferred (auto-generated) mappings

For mapped data sources, it is also possible to specify that the mapping be partly auto-generated
and partly explicitly specified. For example, if a source row has fields `c1`, `c2`, `c3`, and `c5`,
and the table has columns `c1`, `c2`, `c3`, `c4`, one can map all like-named columns and specify
that `c5` in the source maps to `c4` in the table as follows: `* = *, c5 = c4`.

One can specify that all like-named fields be mapped, except for `c2`: `* = -c2`. To skip `c2` and
`c3`: `* = [-c2, -c3]`.

### Writetime and TTL

To specify that a field should be used as the timestamp (a.k.a. writetime) or ttl (a.k.a.
time-to-live) of the inserted row, use the following syntax: `fieldA = writetime(*), fieldB =
ttl(*)`. The special function-like tokens `writetime(*)` and `ttl(*)` are meant to designate
row-level timestamps and ttls: the values of those fields will be used as timestamps and ttls of all
columns in the row.

If your record distinguishes different timestamps and ttls for each column in the row, you can map
such a record with the following syntax: `fieldA = writetime(col1), fieldB = ttl(col1), fieldC =
writetime(col2), fieldD = ttl(col2)`. A mapping entry like `fieldA = writetime(col1)` means that the
value of `fieldA` will be used has the timestamp of `col1`, and `col1` only. Other columns will not
have their writetime or ttl set unless specified elsewhere in the mapping.

It is also possible to mix `writetime(*)` and `writetime(<col_name>)`; a mapping like `fieldA =
writetime(*), fieldB = writetime(c3)` means: map `fieldA` as the timestamp for all columns in the
row, except `c3`, whose timestamp is going to be provided by `fieldB`. The same is valid for
`ttl(*)` and `ttl(<col_name>)`.

Note that timestamp fields are parsed as regular CQL timestamp columns by DSBulk. TTL fields are 
parsed as integers representing durations in seconds.

### Functions

To specify that a column should be populated with the result of a function call, specify the 
function call as the input field (e.g. `now() = c4`). This is only possible when writing to the
database, not when reading. 

Similarly, to specify that a field should be populated with the result of a function call, specify 
the function call as the input column (e.g. `field1 = now()`). This is only possible when reading
from the database, not when writing. 

Function calls can also be qualified by a keyspace name: `field1 = ks1.max(c1,c2)`.

### Identifiers

Any identifier, field, column, function name or keyspace name that is not strictly alphanumeric
(i.e. not matching `[a-zA-Z][a-zA-Z0-9_]*`) must be surrounded by double-quotes, just like you would
do in CQL: `"Field ""A""" = "Column 2"` (to escape a double-quote, simply double it).

Note that, contrary to the CQL grammar, unquoted identifiers will be considered as is, and will not
be lower-cased: an identifier such as `MyColumn1` will match a column named `"MyColumn1"` and not
`mycolumn1`. This is also valid for function and keyspace identifiers: for example, to reference the
keyspace `"MyKeyspace"` and the function `"MyFunc"`, use `field1 = MyKeyspace.MyFunc(*)`.  Of
course, it is also possible to double-quote such identifiers: `field1 = "MyKeyspace"."MyFunc"(*)`.
The two variants are equivalent.

Beware that most built-in functions have full lower-case internal names; for example, to use the
built-in function `dateOf`, use `field1 = dateof(now())` (note the lower-case name, since that's the
internal name of that function).

### Typed literals

Finally, it is also possible to specify constant typed literals. A constant typed literal is a
mapping token of the form `(<type>) <literal>`, where `type` is a valid CQL type (see below), and
`literal` is a valid CQL literal for that type. 

Constant typed literals can appear on the left-hand side of a mapping entry when loading, and on the
right-hand side when unloading.

Examples:

* Load 123 into `col1` of type `int` for all records: `(int)123 = col1`
* Unload `'2022-02-02T12:34:56Z'` as a `timestamp` value into field `created_at` for all rows:
  `created_at = (timestamp)'2022-02-02T12:34:56Z'`.

Note that unloading constant literals is only supported starting with C* 3.11.5.

Currently, only the following CQL types can be used in a constant typed literal:

|  CQL type   |          Examples of literals          | Requires single quotes? |
|:-----------:|:--------------------------------------:|:-----------------------:|
|   `ascii`   |          `'abc'` `'a''b''c'`           |           yes           |
|  `bigint`   |                 `123`                  |           no            |
|   `blob`    |              `0xcafebabe`              |           no            |
|  `boolean`  |             `true` `false`             |           no            |
|  `counter`  |                 `123`                  |           no            |
|   `date`    |             `'2022-01-01'`             |           yes           |
|  `decimal`  |                `123.4`                 |           no            |
|  `double`   |                `123.4`                 |           no            |
| `duration`  |  `'P5Y10M5DT4H3M2S'` `'-10mo5d4h3s'`   |           yes           |
|   `float`   |                `123.4`                 |           no            |
|   `inet`    |            `'192.168.1.1'`             |           yes           |
|    `int`    |                 `123`                  |           no            |
| `smallint`  |                 `123`                  |           no            |
|   `text`    |          `'abc'` `'a''b''c'`           |           yes           |
|   `time`    |              `'12:34:56'`              |           yes           |
| `timestamp` |        `'2022-04-05T17:53:46Z'`        |           yes           |
| `timeuuid`  | `f0dea7f2-b3fd-11ec-b909-0242ac120002` |           no            |
|  `tinyint`  |                 `123`                  |           no            |
|   `uuid`    | `f0dea7f2-b3fd-11ec-b909-0242ac120002` |           no            |
|  `varchar`  |          `'abc'` `'a''b''c'`           |           yes           |
|  `varint`   |                 `123`                  |           no            |
