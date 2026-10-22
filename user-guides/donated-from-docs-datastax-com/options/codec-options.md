# Codec options

The `codec` options determine how DSBulk converts the values of individual fields or cells during `load` and `unload` operations.

For example, you can use these options to specify how the values in a CQL `timestamp` column are formatted when unloaded to a CSV or JSON file.
The same options also specify how values are loaded from a CSV or JSON file into a CQL `timestamp` column in your database.

> [!TIP]
> In addition to the `codec` options, some [`schema`](schema-options.md) and [`connector`](connector-options.md) options also determine how data is manipulated during `load` and `unload` operations.
>
> Particularly for time/date, null, empty, and numeric values, make sure you are aware of all relevant options.
> Most options have a default behavior that might affect your data.
>
> To test a `dsbulk load` operation without writing the data to your database, use the [`--dryRun`](engine-options.md#engine-dryRun) option.

The `codec` options don't apply to `count` operations.

## Synopsis

The standard form for `codec` options is `--codec.KEY VALUE`:

- **`KEY`**: The specific option to configure, such as the `locale` option.
- **`VALUE`**: The value for the option, such as a string, number, or Boolean.

  HOCON syntax rules apply unless otherwise noted.
  For more information, see [Escape and quote DSBulk command line arguments](../02-usage.md#escape-and-quote-command-line-arguments).

### Short and long forms

On the command line, you can specify options in short form (if available), standard form, or long form.

For all `codec` options, the long form is the standard form with a `dsbulk.` prefix, such as `--dsbulk.codec.locale`.

The following examples show the same command with different forms of the `locale` option:

```bash
# Short form
dsbulk load -locale en_US -url filename.csv -k ks1 -t table1

# Standard form
dsbulk load --codec.locale en_US -url filename.csv -k ks1 -t table1

# Long form
dsbulk load --dsbulk.codec.locale en_US -url filename.csv -k ks1 -t table1
```

In [configuration files](../02-usage.md#use-a-configuration-file), you must use the long form with the `dsbulk.` prefix.
For example:

```json
dsbulk.codec.locale = "en_US"
```

## --codec.allowNullConnections

Whether null collection values (lists, sets or maps) are converted to null values instead of empty collections:

- **`false` (default)**: Null collection values are converted to empty collections.
- **`true`**: Null collection values are converted to null values.

## <a id="dsbulkCodecOptionsBinary"></a>--codec.binary

For `dsbulk unload` only, set the strategy to use when converting binary data to strings:

- **`BASE64` (default)**: Encode the binary data into a Base-64 string.
- **`HEX`**: Encode the binary data as CQL blob literals.

  CQL blob literals are formatted as `0[xX][0-9a-fA-F]+`, which is `0x` followed by hexadecimal characters.
  For example, `0xcafebabe`.
  `HEX` format produces longer strings than `BASE64` but it is the only format compatible with the CQL shell (`cqlsh`).

This option is used only in the following scenarios:

- Unloading columns of CQL type `blob`
- Unloading columns of a geometry type with [`--codec.geo "WKB"`](#dsbulkCodecOptionsGeo).

## <a id="dsbulkCodecOptionsBooleanNumbers"></a>--codec.booleanNumbers

A list of numbers that map to Boolean `true` and `false` values, formatted as `[true_number,false_number]`, such as `[1,0]`.

The mapping is reciprocal, so that numbers will map to Booleans, and Booleans will map to numbers.

If a Boolean field or cell contains a number that isn't specified in this list, the value is rejected.

Default: `[1,0]` (`1` maps to `true`, and `0` maps to `false`)

## <a id="dsbulkCodecOptionsBooleanStrings"></a>--codec.booleanStrings

A list of strings that map to Boolean `true` and `false` values.

Unlike `--codec.booleanNumbers`, this option allows multiple representations for `true` and `false` values.

Each representation is a pair of values separated by a colon, where the first value maps to `true` and the second value maps to `false`.
All values are case-insensitive.
Separate each pair with a comma, wrap each pair in double-quotes, and wrap the entire list in single quotes.
For example:

```bash
--codec.booleanStrings '["true_value1:false_value1","true_value2:false_value2"]'
```

For `load` operations, all representations are honored.

For `unload` operations, only the first representation is used, and all others are ignored.

Default: `["1:0","Y:N","T:F","YES:NO","TRUE:FALSE"]`

## <a id="dsbulkCodecOptionsDate"></a>--codec.date

The temporal pattern to use for `String` to [CQL date](https://cassandra.apache.org/doc/5.0/cassandra/developing/cql/types.html#dates) conversion.

Accepts a string specifying one of the following:

- **Date-time pattern**: Must be compatible with [`DateTimeFormatter`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns), such as `yyyy-MM-dd`.
- **Pre-defined formatter**: Must be compatible with [`DateTimeFormatter`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns), such as `ISO_LOCAL_DATE`.
- **`UNITS_SINCE_EPOCH`**: A special parser that reads and writes local dates as numbers representing time units since a given epoch.
  Specify the unit and the epoch to use with [`--codec.unit`](#dsbulkCodecOptionsUnit) and [`--codec.epoch`](#dsbulkCodecOptionsEpoch).

  This pattern is required for datasets that contain numeric data intended to be interpreted as units since a given epoch.

Default: `ISO_LOCAL_DATE`

## <a id="dsbulkCodecOptionsEpoch"></a>--codec.epoch

If [`--codec.timestamp`](#dsbulkCodecOptionsTimestamp), [`--codec.date`](#dsbulkCodecOptionsDate), or [`--codec.time`](#dsbulkCodecOptionsTime) is set to `UNITS_SINCE_EPOCH`, then this epoch determines the relative point in time to use when converting numeric data to and from temporals.

This option is paired with [`--codec.unit`](#dsbulkCodecOptionsUnit), which determines the time unit for the conversion.
For example, if `--codec.epoch "2000-01-01T00:00:00Z"` and `--codec.unit "SECONDS"`, the value `123` is interpreted as 123 seconds since January 1, 2000.

`--codec.epoch` applies to the following scenarios only:

- When a `load` or `unload` operation targets a CQL `timestamp` column _and_ `--codec.timestamp` is set to `UNITS_SINCE_EPOCH`.
- When a `load` or `unload` operation targets a CQL `date` column _and_ `--codec.date` is set to `UNITS_SINCE_EPOCH`.
- When a `load` or `unload` operation targets a CQL `time` column _and_ `--codec.time` is set to `UNITS_SINCE_EPOCH`.
- When a `load` operation uses a `USING TIMESTAMP` clause _and_ `--codec.timestamp` is set to `UNITS_SINCE_EPOCH`.
- When a `load` operation uses a `USING DATE` clause _and_ `--codec.date` is set to `UNITS_SINCE_EPOCH`.
- When a `load` operation uses a `USING TIME` clause _and_ `--codec.time` is set to `UNITS_SINCE_EPOCH`.
- When an `unload` operation uses a `writetime()` function call _and_ `--codec.timestamp` is set to `UNITS_SINCE_EPOCH`.

If the target column for a `load` operation is a numeric CQL type but the input is alphanumeric data that represents a temporal literal, then `--codec.epoch` and `--codec.unit` are used to convert the parsed temporal into a numeric value.
For example, if the input is `2018-12-10T19:32:45Z` with `--codec.epoch "2000-01-01T00:00:00Z"` and `--codec.unit "SECONDS"`, then the parsed timestamp will be converted to a number of seconds since January 1, 2000.

When parsing temporal literals, if the input doesn't contain a date part, then the date part of `--codec.epoch` is used.
For example, if the input is `19:32:45` with `--codec.epoch "2000-01-01T00:00:00Z"`, then the input is interpreted as `2000-01-01T19:32:45Z`.

The value of `--codec.epoch` must be expressed in [`ISO_ZONED_DATE_TIME`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_ZONED_DATE_TIME) format.

Default: `1970-01-01T00:00:00Z`

## <a id="dsbulkCodecOptionsFormatNumbers"></a>--codec.formatNumbers

Whether to use the [`--codec.number`](#dsbulkCodecOptionsNumber) pattern to format all numeric output:

- **`false` (default)**: Numbers are stringified using the `toString` method without rounding or decimal scale alteration.
- **`true`**: Numbers are formatted using the specified `--codec.number` pattern.
  This ensures consistent formatting for all numeric output but it can result in rounding (see [`--codec.roundingStrategy`](#dsbulkCodecOptionsRoundingStrategy)) or alteration of decimal scales.

Only applicable for `unload` operations where the [connector](connector-options.md) requires stringification because the connectors don't handle raw numeric data.

## <a id="dsbulkCodecOptionsGeo"></a>--codec.geo

A string containing the name of the strategy to use when converting geometry types to strings:

- **`WKT` (default)**: Encode the data in Well-Known Text (WKT) format.
- **`WKB`**: Encode the data in Well-Known Binary (WKB) format.
  The actual encoding depends on the [`codec.binary`](#dsbulkCodecOptionsBinary) option (`HEX` or `BASE64`).
- **`JSON`**: Encode the data in GeoJSON format.

This option is applicable only if all of the following conditions are met:

- Unloading data from DSE 5.0 or later
- Unloading CQL `Point`, `LineString`, or `Polygon` columns.
- The [connector](connector-options.md) requires stringification.

## <a id="dsbulkCodecOptionsLocale"></a>--codec.locale (-locale)

Set the locale to use for locale-sensitive conversions.

Default: `en_US`

## <a id="dsbulkCodecOptionsNullStrings"></a>--codec.nullStrings (-nullStrings)

A comma-separated list of strings to map to `null`, such as `["NULL","N/A","-"]`.

### `-nullStrings` with `dsbulk load`

For `load` operations, when an input field's value exactly matches one of the specified strings, the value is replaced with `null` before writing to the database.

The default value is an empty list (`[]`), which means no specific strings are mapped to `null`.
When loading to `text`, `varchar`, or `ascii` columns with the default mapping, input field values are unchanged by this option specifically.
For example, empty strings are written as `""` (not `null`) in the absence of other modifying options, such as [`--connector.csv.emptyValue`](connector-options.md#connectorCsvEmptyValue).

> [!IMPORTANT]
> - Regardless of `-nullString`, DSBulk always converts empty strings to `null` when a `load` operation targets a CQL column that _isn't_ `text`, `varchar`, or `ascii`.
> - `-nullStrings` is applied before [`--schema.nullToUnset`](schema-options.md#schemaNullToUnset), so any `null` produced by `-nullStrings` can still be left unset, if required.

### `-nullStrings` with `dsbulk unload`

For `unload` operations, this option applies only if the [connector](connector-options.md) requires stringification.

DSBulk uses only the first string in the list.
If a row cell contains `null`, it is converted to the first `--codec.nullStrings` entry in the output.

The default value is an empty list (`[]`), which means no specific strings are mapped to `null`.
With the default mapping, any unloaded cell containing a `null` value is unchanged, and the resulting output is `null`.
This assumes the absence of other modifying options, such as [`--connector.csv.emptyValue`](connector-options.md#connectorCsvEmptyValue).

## <a id="dsbulkCodecOptionsNumber"></a>--codec.number

The [`DecimalFormat`](https://docs.oracle.com/javase/8/docs/api/java/text/DecimalFormat.html) pattern to use for conversion between `String` and CQL numeric types.

Most inputs are recognized, including an optional localized thousands separator, localized decimal separator, or optional exponent.
For example, using the default `--codec.number` pattern and [`-locale en_US`](#dsbulkCodecOptionsLocale), then all of the following are valid: `1234`, `1,234`, `1234.5678`, `1,234.5678`, and `1,234.5678E2`.

For `unload` operations, rounding can occur and cause precision loss, depending on the combined behaviors of `--codec.number`, [`--codec.formatNumbers`](#dsbulkCodecOptionsFormatNumbers), and [`--codec.roundingStrategy`](#dsbulkCodecOptionsRoundingStrategy).

Default: `#,###.##`

## <a id="dsbulkCodecOptionsOverflowStrategy"></a>--codec.overflowStrategy

This option applies when converting an input field value to a CQL numeric type results in overflow:

- The value is outside the range of the target CQL type.
  For example, trying to convert `128` to a CQL `tinyint` results in overflow because `tinyint` has a maximum value of `127`.
- The value is decimal but the target CQL type is integral.
  For example, trying to convert `123.45` to a CQL `int` results in overflow.
- The value's precision is too large for the target CQL type.
  For example, trying to insert `0.1234567890123456789` into a CQL `double` results in overflow because there are too many significant digits to fit in a 64-bit double.

This option applies only when parsing numeric inputs during `load` operations.
It doesn't apply to `unload` operations because output formatting never results in overflow.

Allowed values for `--codec.overflowStrategy` are as follows:

- **`REJECT` (default)**: Overflows are considered errors, and the data is rejected.
- **`TRUNCATE`**: The data is truncated to fit in the target CQL type.

  The truncation algorithm is similar to the narrowing primitive conversion defined in the [Java Language Specification section 5.1.3](https://docs.oracle.com/javase/specs/jls/se21/html/jls-5.html#jls-5.1.3) with the following exceptions:
  - If the value is too big, it is rounded down to the maximum value allowed, rather than truncated at bit level.
    For example, DSBulk rounds `128` down to `127` to fit in a byte, whereas Java truncates the exceeding bits and converts them to `-127` instead.
  - If the value is too small, it is rounded up to the minimum value allowed, rather than truncated at bit level.
  - If the value is decimal but the target CQL type is integral, it is first rounded to an integral using the defined rounding strategy, and then it is narrowed to fit into the target type.

    > [!IMPORTANT]
    > Especially for decimal values, `TRUNCATE` can result in precision loss.
    > Make sure your data can tolerate such loss before using this strategy.

## <a id="dsbulkCodecOptionsRoundingStrategy"></a>--codec.roundingStrategy

Set the rounding strategy to use when converting CQL numeric types to `String`.
Only applies to `dsbulk unload` operations where [`--codec.formatNumbers true`](#dsbulkCodecOptionsFormatNumbers) _and_ the [connector](connector-options.md) requires stringification because the connectors don't handle raw numeric data.

Accepts the name of any `java.math.RoundingMode` enum constant, including `CEILING`, `FLOOR`, `UP`, `DOWN`, `HALF_UP`, `HALF_EVEN`, `HALF_DOWN`, and `UNNECESSARY`.

The precision used when rounding is inferred from the numeric pattern set in [`--codec.number`](#dsbulkCodecOptionsNumber).
For example, the default `--codec.number` pattern `#,###.##` has a rounding precision of two digits.
If `--codec.roundingStrategy` is set to `UP`, then the number `123.456` would be rounded to `123.46`.

Default: `UNNECESSARY` (infinite precision, and `--codec.number` is ignored)

## <a id="dsbulkCodecOptionsTime"></a>--codec.time

The temporal pattern to use for `String` to [CQL time](https://cassandra.apache.org/doc/5.0/cassandra/developing/cql/types.html#times) conversion.

Accepts a string specifying one of the following:

- **Date-time pattern**: Must be compatible with [`DateTimeFormatter`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns), such as `HH:mm:ss`.
- **Pre-defined formatter**: Must be compatible with [`DateTimeFormatter`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns), such as `ISO_LOCAL_TIME`.
- **`UNITS_SINCE_EPOCH`**: A special parser that reads and writes local times as numbers representing time units since a given epoch.
  Specify the unit and the epoch to use with [`--codec.unit`](#dsbulkCodecOptionsUnit) and [`--codec.epoch`](#dsbulkCodecOptionsEpoch).

  This pattern is required for datasets that contain numeric data intended to be interpreted as units since a given epoch.

Default: `ISO_LOCAL_TIME`

## <a id="dsbulkCodecOptionsTimestamp"></a>--codec.timestamp

The temporal pattern to use for `String` to [CQL timestamp](https://cassandra.apache.org/doc/5.0/cassandra/developing/cql/types.html#timestamps) conversion.

Accepts a string specifying one of the following:

- **Date-time pattern**: Must be compatible with [`DateTimeFormatter`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns), such as `yyyy-MM-dd HH:mm:ss`.
- **Pre-defined formatter**: Any public static field in [`DateTimeFormatter`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns), such as `ISO_ZONED_DATE_TIME` or `ISO_INSTANT`.
- **`UNITS_SINCE_EPOCH`**: A special parser that reads and writes local times as numbers representing time units since a given epoch.
  Specify the unit and the epoch to use with [`--codec.unit`](#dsbulkCodecOptionsUnit) and [`--codec.epoch`](#dsbulkCodecOptionsEpoch).

  This pattern is required for datasets that contain numeric data intended to be interpreted as units since a given epoch.

- **`CQL_TIMESTAMP`**: A special parser that accepts all valid CQL literal formats for the `timestamp` type.

  For `load` operations, if the input is a local date or date/time, then the timestamp is resolved using the time zone specified in `--codec.timeZone`.
  For `unload` operations, the formatter uses the `ISO_OFFSET_DATE_TIME` pattern, which is compliant with CQL and ISO-8601.

Default: `CQL_TIMESTAMP`

## <a id="dsbulkCodecOptionsTimezone"></a>--codec.timeZone (-timeZone)

The timezone to use for temporal conversions.

For `load` operations, `-timeZone` is used to obtain a timestamp from inputs that don't convey any explicit timezone information.

For `unload` operations, `-timeZone` is used to format all timestamps.

This option accepts any [`ZoneId`](https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html) format.

Default: `UTC`

## <a id="dsbulkCodecOptionsUnit"></a>--codec.unit

If [`--codec.timestamp`](#dsbulkCodecOptionsTimestamp), [`--codec.date`](#dsbulkCodecOptionsDate), or [`--codec.time`](#dsbulkCodecOptionsTime) is set to `UNITS_SINCE_EPOCH`, then this unit is used with `--codec.epoch` to convert numeric data to and from temporals.

`--codec.unit` defines the time unit for the conversion.
It is paired with `--codec.epoch`, which determines the starting epoch for the conversion.
For example, if `--codec.epoch "2000-01-01T00:00:00Z"` and `--codec.unit "SECONDS"`, the value `123` is interpreted as 123 seconds since January 1, 2000.

`--codec.unit` and `--codec.epoch` are used in specific scenarios only.
For more information about these scenarios and edge case handling, see [`--codec.epoch`](#dsbulkCodecOptionsEpoch).

`--codec.unit` accepts any `TimeUnit` enum constant.

Default: `MILLISECONDS`

## --codec.uuidStrategy

Set the strategy to use when generating time-based version 1 UUIDs from timestamps.

> [!WARNING]
> - The `MIN` and `MAX` strategies don't guarantee uniquely generated UUIDs.
>   Make sure your data can tolerate potential duplicates before using these strategies.
> - For all strategies, the clock sequence and node ID parts of the generated UUIDs are determined on a best-effort basis, and they aren't fully compliant with RFC 4122.

- **`RANDOM` (default)**: Generates UUIDs using a random number in place of the local clock sequence and node ID.
  This strategy ensures that the generated UUIDs are unique, even if the original timestamps aren't guaranteed to be unique.
- **`FIXED` (recommended if original timestamps are unique)**: Generates UUIDs using a fixed local clock sequence and node ID.

  This strategy is recommended if your original timestamps are guaranteed to be unique because it's faster than other strategies.

- **`MIN`**: Generates the smallest possible type 1 UUID for a given timestamp.
- **`MAX`**: Generates the largest possible type 1 UUID for a given timestamp.

## See also

- [Schema options](schema-options.md)
- [CSV and JSON connector options](connector-options.md)
