# CSV and JSON connector options

The `connector` options are used with the `dsbulk load` and `dsbulk unload` commands.
These options define the type of data being loaded or unloaded as CSV or JSON data, and they provide settings for transforming the data when loading or unloading.

For cluster authentication and connection options, see [Driver options](driver-options.md).

## Synopsis

The standard form for most `connector` options is `--connector.TYPE.KEY VALUE`.
The only exception is the [`--connector.name`](#connectorName) option, which doesn't include the **`TYPE`** portion.

- **`TYPE`**: The connector that you want to use, either `csv` or `json`, based on the type of files you are loading or unloading.

  For example, to set the `recursive` option, use either `--connector.csv.recursive` or `--connector.json.recursive`.

  > [!TIP]
  > The default connector is the CSV connector.
  > To use the JSON connector, you can explicitly set [`--connector.name json`](#connectorName), pass at least one `connector.json.KEY` option, or both.
  >
  > To test a `dsbulk load` operation without writing the data to your database, use the [`--dryRun`](engine-options.md#engine-dryRun) option.

- **`KEY`**: The specific option to configure, such as the `compression` option or `fileNameFormat` option.
- **`VALUE`**: The value for the option, such as a string, number, or Boolean.

  HOCON syntax rules apply unless otherwise noted.
  For more information, see [Escape and quote DSBulk command line arguments](../02-usage.md#escape-and-quote-command-line-arguments).

### Short and long forms

On the command line, you can specify options in short form (if available), standard form, or long form.

For all `connector` options, the long form is the standard form with a `dsbulk.` prefix, such as `--dsbulk.connector.csv.recursive`.

The following examples show the same command with different forms of the `url` option:

```bash
# Short form
dsbulk load -url filename.csv -k ks1 -t table1

# Standard form
dsbulk load --connector.csv.url filename.csv -k ks1 -t table1

# Long form
dsbulk load --dsbulk.connector.csv.url filename.csv -k ks1 -t table1
```

In [configuration files](../02-usage.md#use-a-configuration-file), you must use the long form with the `dsbulk.` prefix.
For example:

```json
dsbulk.connector.csv.url = "filename.csv"
```

## <a id="connectorName"></a>--connector.name (-c)

The `--connector.name` (`-c`) option specifies the connector to use for a `dsbulk load` or `dsbulk unload` operation:

- **`csv` (default)**: Use the CSV connector to read/write CSV files.

  When loading or unloading CSV files, you can omit the `--connector.name` option because the default is `csv`.

- **`json`**: Use the JSON connector to read/write JSON files.

  If your command doesn't explicitly set any `--connector.json` options, consider explicitly setting `--connector.name json` to ensure that the JSON connector is used.

This option deviates from the [standard form for connector options](#synopsis) because it doesn't include the connector type in the option name.
The long form for this option is `--dsbulk.connector.name`.

## CSV connector options

You can use the following options when loading or unloading CSV files.

### <a id="connectorCSVComment"></a>--connector.csv.comment (-comment)

The character that indicates the start of a comment line in loaded or unloaded files.
Only one character can be specified.

Use [quotes and escaping](../02-usage.md#escape-and-quote-command-line-arguments) as needed.

Default: `"\u0000"`, a `null` character that means comment line detection is disabled.

### <a id="connectorCompression"></a>--connector.csv.compression

Use this option to load data from a compressed file, or unload data to a compressed file.

The default is no compression (not set).

#### `--connector.csv.compression` with `dsbulk load`

When loading data from a compressed file, specify one of the following compression types:

- `brotli`
- `bzip2`
- `deflate`
- `deflate64`
- `gzip`
- `lzma`
- `lz4`
- `snappy`
- `xz`
- `z`
- `zstd`

When searching for the file to load, DSBulk appends the appropriate extension to the `fileNamePattern`, such as `.gz` for the `gzip` type.

#### `--connector.csv.compression` with `dsbulk unload`

When unloading data to a compressed file, specify one of the following compression types:

- `bzip2`
- `deflate`
- `gzip`
- `lzma`
- `lz4`
- `snappy`
- `xz`
- `zstd`

When unloading data to compressed files, the resulting file names are based on the `fileNameFormat` option and the appropriate extension for the compression type.
For example, the following command unloads data with the default `fileNameFormat` and `gzip` compression:

```bash
dsbulk unload -k test -t table1 --connector.csv.compression gzip -url mydir
```

The compressed files output by this command are named `output-COUNTER.csv.gz`, such as `output-000001.csv.gz`, `output-000002.csv.gz`, and so on.

### <a id="connectorCSVDelimiter"></a>--connector.csv.delimiter (-delim)

One or more characters to use as field delimiters for `load` and `unload` operations.
Field delimiters containing multiple characters are allowed, such as `'||'`.

Use [quotes and escaping](../02-usage.md#escape-and-quote-command-line-arguments) as needed.

Default: `,` (fields are delimited by commas)

### <a id="connectorCsvEmptyValue"></a>--connector.csv.emptyValue

Sets the string representation for empty values in loaded or unloaded records.
For example, if you want empty values to translate to the literal string `EMPTY`, then set `--connector.csv.emptyValue EMPTY`.
For the string representation of null values, see [`connector.csv.nullValue`](#connectorCsvNullValue).

With `dsbulk load`, if the parser finds input wrapped in quotes that doesn't contain any characters (`""`), then the `emptyValue` string is written to the database.
The default value `AUTO` writes an empty string to the database when DSBulk encounters an empty value.
Quotes with white space characters inside (`"   "`) are not considered empty values, unless you set the various `connector.csv.ignore*whitespaces` options.

With `dsbulk unload`, if the writer needs to write an empty string to the output file, then the `emptyValue` string is written to the output.
The default value `AUTO` writes a quoted, empty field to the output when it encounters an empty value.

When reading from CSV files, the following examples show how the line `a,,""` is parsed with different configurations for `emptyValue` and `nullValue`:

- If `emptyValue` and `nullValue` are both set to `AUTO` (default), then `a,,""` becomes `["a", null, ""]`.
- If `emptyValue` is set to `EMPTY` and `nullValue` is set to `NULL`, then `a,,""` becomes `["a", "NULL", "EMPTY"]`.
- If `emptyValue` is set to `BAR` and `nullValue` is set to `FOO`, then `a,,""` becomes `["a", "FOO", "BAR"]`.

See also [`--codec.nullStrings`](codec-options.md#dsbulkCodecOptionsNullStrings).

### --connector.csv.encoding (-encoding)

The character encoding format for all loaded or unloaded records.

Applies to all records read or written by a given command.
It cannot be selectively applied.

Default: `UTF-8`

### <a id="connectorCSVEscape"></a>--connector.csv.escape (-escape)

The character used for escaping quotes inside an already quoted value.
Only one character can be specified.

Applies to all records loaded by a given `dsbulk load` command.
It cannot be selectively applied.

Default: `\`

### <a id="connectorfileNameFormat"></a>--connector.csv.fileNameFormat

With `dsbulk unload` only, you can specify the file name format for the output files.
The file name must comply with the `String.format()` formatting rules, and it must contain a `%NNd` format specifier that is used to increment the file name counter.
Replace `NN` with the number of digits to use for the counter, such as `%06d` for a six-digit counter with leading zeros.

> [!IMPORTANT]
> This option is ignored if `-url` isn't a file path.

Default: `output-%06d.csv`

### <a id="connectorfileNamePattern"></a>--connector.csv.fileNamePattern

With `dsbulk load` only, you can specify a glob pattern to use when searching for files to read.
This string must use glob syntax, as described in [`java.nio.file.FileSystem.getPathMatcher()`](https://docs.oracle.com/javase/7/docs/api/java/nio/file/FileSystem.html#getPathMatcher(java.lang.String)).

> [!IMPORTANT]
> This option applies _only_ if `-url` is a file path to a directory.

Default: `**/*.csv`

### <a id="connectorCSVHeader"></a>--connector.csv.header (-header)

Whether the loaded or unloaded files begin with a header line.

Applies to all files read or written by a given command.
It cannot be selectively applied.

#### `-header` with `dsbulk load`

When loading CSV files, the `header` option has the following behavior:

- **`true` (default)**: The first non-empty line in every input file is treated as the header line.
  The values from this line assign the field names for each column, in lieu of [`schema.mapping`](schema-options.md).
  For example, a line like `fieldA,fieldB,fieldC` would map to the columns as `fieldA` to column 1, `fieldB` to column 2, and `fieldC` to column 3.
- **`false`**: Disables header line handling.
  Loaded records contain field indexes instead of field names where index `0` maps to column 1, index `1` maps to column 2, index `2` maps to column 3, and so on.

#### `-header` with `dsbulk unload`

When unloading CSV files, the `header` option has the following behavior:

- **`true` (default)**: Each output file begins with a header line.
- **`false`**: Output files don't contain header lines.

### <a id="connectorCsvIgnoreLeadingWhitespaces"></a>--connector.csv.ignoreLeadingWhitespaces

Whether to trim leading whitespace in values when loading or unloading records:

- **`false` (default)**: Leading whitespace is preserved.
- **`true`**: Leading whitespace isn't preserved.

This option applies to all values, with or without quotes.
To trim leading whitespace from quoted values only, use [`--connector.csv.ignoreLeadingWhitespacesInQuotes`](#connectorCsvIgnoreLeadingWhitespacesInQuotes).

### <a id="connectorCsvIgnoreLeadingWhitespacesInQuotes"></a>--connector.csv.ignoreLeadingWhitespacesInQuotes

Whether to trim leading whitespace in quoted values when loading records:

- **`false` (default)**: Leading whitespace in quoted values is preserved.
- **`true`**: Leading whitespace in quoted values isn't preserved.

This option applies to quoted values only.
To trim leading whitespace from all values, with or without quotes, use [`--connector.csv.ignoreLeadingWhitespaces`](#connectorCsvIgnoreLeadingWhitespaces).

### <a id="connectorCsvIgnoreTrailingWhitespaces"></a>--connector.csv.ignoreTrailingWhitespaces

Whether to trim trailing whitespace in values when loading or unloading records:

- **`false` (default)**: Trailing whitespace is preserved.
- **`true`**: Trailing whitespace isn't preserved.

This option applies to all values, with or without quotes.
To trim trailing whitespace from quoted values only, use [`--connector.csv.ignoreTrailingWhitespacesInQuotes`](#connectorCsvIgnoreTrailingWhitespacesInQuotes).

### <a id="connectorCsvIgnoreTrailingWhitespacesInQuotes"></a>--connector.csv.ignoreTrailingWhitespacesInQuotes

Whether to trim trailing whitespace in quoted values when loading records:

- **`false` (default)**: Trailing whitespace in quoted values is preserved.
- **`true`**: Trailing whitespace in quoted values isn't preserved.

This option applies to quoted values only.
To trim trailing whitespace from all values, with or without quotes, use [`--connector.csv.ignoreTrailingWhitespaces`](#connectorCsvIgnoreTrailingWhitespaces).

### <a id="connectorCSVmaxCharsPerColumn"></a>--connector.csv.maxCharsPerColumn

Specify the maximum number of characters that a field can contain when loading or unloading records.

Use this option to size internal buffers and avoid out-of-memory (OOM) problems.

Accepts a positive integer or `-1`.

If set to `-1`, internal buffers are resized dynamically.
This is convenient, but it can cause memory problems and reduce throughput, particularly for large fields that require constant resizing.
If you observe performance issues after setting `--connector.csv.maxCharsPerColumn -1`, try setting this option to a fixed, positive integer that is large enough for all field values.

Default: `4096`

### <a id="connectorCSVmaxColumns"></a>--connector.csv.maxColumns

Specify the maximum number of columns that a loaded or unloaded record can contain.

Use this option to size internal buffers and avoid OOM problems.

Default: `512`

### <a id="connectorConcurrentFiles"></a>--connector.csv.maxConcurrentFiles (-maxConcurrentFiles)

The maximum number of files to load or unload simultaneously.

Allowed values include the following:

- **`AUTO` (default)**: The connector estimates an optimal number of files automatically.
- **`NC`**: A special syntax that you can use to set the number of threads as a multiple of the number of available cores for a given operation.
  For example, if you set `-maxConcurrentFiles 0.5C` and there are 8 cores, then there will be 4 parallel threads (`0.5 * 8 = 4`).
- **Positive integer**: Specifies the exact number of files to read or write in parallel.
  For example, `1` reads or writes one file at a time.

> [!TIP]
> With `dsbulk load`, it can be helpful to reduce this value if the disk is slow, especially SAN disks.
> Excessive disk IO can perform worse than reading files individually (`-maxConcurrentFiles 1`).
> If diagnostic tools like `iostat` show too much time spent on disk IO, consider adjusting `maxConcurrentFiles` to a lower value, `AUTO`, or `1`.
>
> Rows larger than 10KB can also benefit from a lower `maxConcurrentFiles` value.

### <a id="connectormaxRecords"></a>--connector.csv.maxRecords (-maxRecords)

Specify the maximum number of records to load from or unload to each file.
The default is `-1` (unlimited).

#### `-maxRecords` with `dsbulk load`

If `-maxRecords` is set to a positive integer, then all records past the maximum number are ignored.
For example, if `-maxRecords 1000`, only the first 1000 records from each input file are loaded.

`--connector.csv.maxRecords` respects [`--connector.csv.header true`](#connectorCSVHeader).
If a file begins with a header line, that line isn't counted as a record.

#### `-maxRecords` with `dsbulk unload`

If `-maxRecords` is set to a positive integer, then each output file will contain no more than the maximum number of records.

If the total records to unload exceeds `-maxRecords`, then DSBulk creates additional files.
File names are determined by the `fileNameFormat` option.

If `-maxRecords` is set to `-1`, then the `unload` operation writes all records to one file.

> [!IMPORTANT]
> This option is ignored if the output destination _isn't_ a directory.

### <a id="connectorCSVnewline"></a>--connector.csv.newline (-newline)

How to determine line breaks when loading or unloading records:

- **`AUTO` (default)**: Use Java's `System.lineSeparator()` to write line breaks for `dsbulk unload` operations, and to detect line breaks automatically for `dsbulk load` operations.
- **String**: Specify one or two characters that represent the end of a line.

  In this case, a _character_ is determined by the resolved value of the given string.
  For example, `\n` is considered one character because the group of symbols (`\` and `n`) resolves to the newline character.

  Use [quotes and escaping](../02-usage.md#escape-and-quote-command-line-arguments) as needed.
  For example, if line breaks are indicated by a carriage return followed by a newline, set `-newline "\r\n"`.

### <a id="connectorCSVNormalizeLineEndingsInQuotes"></a>--connector.csv.normalizeLineEndingsInQuotes

For `load` and `unload` operations, use this option to normalize line separator characters in quoted values.
DSBulk uses Java's `System.lineSeparator()` to detect line separators.

- **`false` (default)**: No line separator normalization is performed.
- **`true`**: All line separators in quoted values are replaced with `\n`.

> [!TIP]
> On Microsoft Windows, the detection mechanism for line endings might not function correctly if this option is `false` due to a defect in the CSV parsing library.
> If you observe parsing issues on Microsoft Windows, try setting `--connector.csv.normalizeLineEndingsInQuotes true`.

### <a id="connectorCsvNullValue"></a>--connector.csv.nullValue

Sets the string representation for null values in loaded or unloaded records.
For example, if you want null values to translate to the literal string `NULL`, then set `--connector.csv.nullValue NULL`.
For the string representation of empty values, see [`connector.csv.emptyValue`](#connectorCsvEmptyValue).

With `dsbulk load`, if the parser finds an input that doesn't contain any characters, then the `nullValue` string is written to the database.
The default value `AUTO` writes `null` to the database when DSBulk encounters an null input.

With `dsbulk unload`, if the writer needs to write a null value to the output file, then the `nullValue` string is written to the output.
The default value `AUTO` writes nothing to the output when it encounters a null value.

When reading from CSV files, the following examples show how the line `a,,""` is parsed with different configurations for `emptyValue` and `nullValue`:

- If `emptyValue` and `nullValue` are both set to `AUTO` (default), then `a,,""` becomes `["a", null, ""]`.
- If `emptyValue` is set to `EMPTY` and `nullValue` is set to `NULL`, then `a,,""` becomes `["a", "NULL", "EMPTY"]`.
- If `emptyValue` is set to `BAR` and `nullValue` is set to `FOO`, then `a,,""` becomes `["a", "FOO", "BAR"]`.

See also [`--codec.nullStrings`](codec-options.md#dsbulkCodecOptionsNullStrings).

### <a id="connectorCSVquote"></a>--connector.csv.quote (-quote)

Specify the character used for quoting fields when the field delimiter is part of the field value.

Only one character can be specified.
A _character_ is determined by the resolved value of the given string.
For example, `\"` is considered one character because the group of symbols (`\` and `"`) resolves to an escaped double-quote character.

Applies to all records read or written by a given `load` or `unload` command.
It cannot be selectively applied.

Default: `"\""` (the double-quote character with escaping)

### <a id="connectorRecursive"></a>--connector.csv.recursive

Whether to load files from subdirectories if the `-url` option points a directory.

Ignored if `-url` _isn't_ a file path to a directory.

Not applicable to the `dsbulk unload` command.

Default: `false` (no recursion)

### <a id="connectorSkipRecords"></a>--connector.csv.skipRecords (-skipRecords)

With `dsbulk load` only, you can specify the number of records to bypass (skip) before the parser begins processing the input file.
The default is `0` (no records skipped).

Applies to all files loaded by a given `dsbulk load` command.
It cannot be selectively applied.

`--connector.csv.skipRecords` respects [`--connector.csv.header true`](#connectorCSVHeader).
If a file begins with a header line, that line isn't counted towards the skipped records.

### <a id="dsbulk-csvUrl"></a>--connector.csv.url (`-url`)

Specify the source or destination for a `load` or `unload` operation.

Use [quotes and escaping](../02-usage.md#escape-and-quote-command-line-arguments) as needed for the `-url` string.

> [!IMPORTANT]
> `-url` cannot be used with the `urlfile` option.
> If both are specified,then `urlfile` takes precedence.

For more examples, see [Load data](../dsbulk-load.md) and [Unload data](../dsbulk-unload.md).

#### `-url` with `dsbulk load`

For a `dsbulk load` operation, specify the location where the input files are stored.
Allowed values include the following:

- **Standard input**: Specified by `-`.
  This is the default source if `-url` is omitted.
- **URL**: If `-url` begins with `http:` or `https:`, the source is read directly, and options like `fileNamePattern` and `recursive` are ignored.

  AWS S3 URLs must contain the necessary query parameters for DSBulk to build an `S3Client` and access the target bucket.
  For more information, see [Load from AWS S3](../dsbulk-load.md#load-from-aws-s3).

- **File path**: Specify a local or remote file or directory.

  If the target is a directory, `dsbulk load` processes all files in the directory that match the `fileNamePattern`.
  To read from a directory and its subdirectories, include the `recursive` option.

  Relative paths are resolved against the current working directory.
  Paths that begin with a tilde (`~`) resolve to the current user's home directory, and then follow the path from there.

  The `file:` prefix is accepted but optional.
  If `-url` doesn't begin with `file:`, `http:`, or `https:`, it is assumed to be a file path.

#### `-url` with `dsbulk unload`

For a `dsbulk unload` operation, specify the destination where the output will be written.
Allowed values include the following:

- **Standard output**: Specified by `-`.
  This is the default destination if `-url` is omitted.
- **URL**: If `-url` begins with `http:` or `https:`, the output is written directly to the given URL, and options like `fileNameFormat` are ignored.

  > [!IMPORTANT]
  > Some URLs aren't supported by `dsbulk unload`.
  > If the current user doesn't have write permissions for the target URL, the output isn't written to the given URL.
  >
  > DSBulk cannot unload directly to AWS S3.
  > Instead, you can pipe the `dsbulk unload` output to a command that uploads the files to S3 using an AWS CLI, SDK, or API.

- **File path**: Specify a local or remote directory.

  For `dsbulk unload`, a file path target is always treated as a directory.
  If the directory doesn't exist, DSBulk attempts to create it.
  The `fileNameFormat` option sets the naming convention for the output files.

  Relative paths are resolved against the current working directory.
  Paths that begin with a tilde (`~`) resolve to the current user's home directory, and then follow the path from there.

  The `file:` prefix is accepted but optional.
  If `-url` doesn't begin with `file:`, `http:`, or `https:`, it is assumed to be a file path.

For example:

- **Target a remote file**: `-url https://192.168.1.100/data/file.csv`
- **Target a directory**: `-url path/to/directory/`
- **Target a local file, navigating from the current user's home directory**: `-url ~/file.csv`
- **Target a compressed file**: Use the `-url` and `compression` options

### <a id="connectorUrlFile"></a>--connector.csv.urlfile

For `dsbulk load` only, you can use this option to load multiple files from various URLs and paths.
Create a local `.txt` file that contains a list of URLs or paths to files that you want to load, and then point `urlfile` to that local file.

By default, this option is not set and not used.

> [!IMPORTANT]
> - `urlfile` cannot be used with the `-url` option.
>   If both are specified,then `urlfile` takes precedence.
>   If neither are specified, then the default of `-url -` (standard input) is used.
> - Don't use `urlfile` with `dsbulk unload`.
>   This causes a fatal error.

The following requirements apply to the local file targeted by `urlfile`:

- Must be UTF-8 encoded.
- Each line must contain only one valid path or URL.
- Don't escape characters inside the file.
- Use `#` for comment lines.
- Leading and trailing white space is trimmed from each line.
- Related `connector` options, such as `fileNamePattern` and `recursive`, are respected when resolving file paths in `urlfile`.

When using the `urlfile` option with AWS S3 URLs, DSBulk creates an S3 client for each bucket specified in the S3 URLs.
DSBulk caches the S3 clients to prevent them from being re-created unnecessarily when processing many S3 URLs that target the same buckets.
If all of your S3 URLs target the same bucket, then the same S3 client is used for each URL, and the cache contains only one entry.
The size of the S3 client cache is controlled by the `--s3.clientCacheSize` (`--dsbulk.s3.clientCacheSize`) option, and the default is `20` entries.
The default value is arbitrary, and it only needs to be changed when loading from many different S3 buckets in a single command.

## JSON connector options

You can use the following options when loading or unloading JSON files.

### <a id="jsonCompression"></a>--connector.json.compression

Use this option to load data from a compressed file, or unload data to a compressed file.

The default is `none` (no compression).

#### `--connector.json.compression` with `dsbulk load`

When loading data from a compressed file, specify one of the following compression types:

- `brotli`
- `bzip2`
- `deflate`
- `deflate64`
- `gzip`
- `lzma`
- `lz4`
- `snappy`
- `xz`
- `z`
- `zstd`

When searching for the file to load, DSBulk appends the appropriate extension to the `fileNamePattern`, such as `.gz` for the `gzip` type.

#### `--connector.json.compression` with `dsbulk unload`

When unloading data to a compressed file, specify one of the following compression types:

- `bzip2`
- `deflate`
- `gzip`
- `lzma`
- `lz4`
- `snappy`
- `xz`
- `zstd`

When unloading data to compressed files, the resulting file names are based on the `fileNameFormat` option and the appropriate extension for the compression type.
For example, the following command unloads data with the default `fileNameFormat` and `gzip` compression:

```bash
dsbulk unload -k test -t table1 --connector.json.compression gzip -url mydir
```

The compressed files output by this command are named `output-COUNTER.json.gz`, such as `output-000001.json.gz`, `output-000002.json.gz`, and so on.

### <a id="connectorJSONdeserializationFeatures"></a>--connector.json.deserializationFeatures

For `dsbulk load` operations only, you can set JSON deserialization features in the form of `map<String,Boolean>`.

Map keys must be enum constants defined in [`Enum DeserializationFeature`](https://fasterxml.github.io/jackson-databind/javadoc/2.6/com/fasterxml/jackson/databind/DeserializationFeature.html) for Jackson features that are supported by DSBulk.

Jackson feature compatibility depends on the way a feature operates on the resulting JSON tree.
Generally, DSBulk doesn't support Jackson features that filter elements or alter the content of elements in the JSON tree because these features conflict with DSBulk's built-in filtering and formatting capabilities.
Instead of using Jackson features to modify the JSON tree, try using the DSBulk [`codec`](codec-options.md) and [`schema`](schema-options.md) options.

Default: `{ USE_BIG_DECIMAL_FOR_FLOATS : true }` (Parse floating point numbers using `BigDecimal` to avoid precision loss)

> [!IMPORTANT]
> The deserialization feature `USE_BIG_DECIMAL_FOR_FLOATS` is enabled by default to ensure that precision isn't truncated when parsing floating point numbers.
> However, this feature can increase parsing time slightly.
>
> If you don't need this feature, you can disable it by setting `--connector.json.deserializationFeatures { }`, or remove the `USE_BIG_DECIMAL_FOR_FLOATS` entry from your `deserializationFeatures` map.
>
> For more options related to loading and unloading numeric values, see [Codec options](codec-options.md).

### --connector.json.encoding (-encoding)

The character encoding format for all loaded or unloaded records.

Applies to all records read or written by a given command.
It cannot be selectively applied.

Default: `UTF-8`

### <a id="jsonfileNameFormat"></a>--connector.json.fileNameFormat

With `dsbulk unload` only, you can specify the file name format for the output files.
The file name must comply with the `String.format()` formatting rules, and it must contain a `%NNd` format specifier that is used to increment the file name counter.
Replace `NN` with the number of digits to use for the counter, such as `%06d` for a six-digit counter with leading zeros.

> [!IMPORTANT]
> This option is ignored if `-url` isn't a file path.

Default: `output-%06d.json`

### <a id="jsonfileNamePattern"></a>--connector.json.fileNamePattern

With `dsbulk load` only, you can specify a glob pattern to use when searching for files to read.
This string must use glob syntax, as described in [`java.nio.file.FileSystem.getPathMatcher()`](https://docs.oracle.com/javase/7/docs/api/java/nio/file/FileSystem.html#getPathMatcher(java.lang.String)).

> [!IMPORTANT]
> This option applies _only_ if `-url` is a file path to a directory.

Default: `**/*.json`
### <a id="connectorJSONgeneratorFeatures"></a>--connector.json.generatorFeatures

For `dsbulk unload` operations only, you can specify JSON generator features to enable in the form of `map<String,Boolean>`.

Accepts any enum constants defined in [`com.fasterxml.jackson.core.JsonGenerator.Feature`](https://www.javadoc.io/doc/com.fasterxml.jackson.core/jackson-core/latest/com/fasterxml/jackson/core/JsonGenerator.Feature.html) for Jackson features that are supported by DSBulk.
For example, the map `{ ESCAPE_NON_ASCII : true, QUOTE_FIELD_NAMES : true }` configures the generator to escape all characters outside 7-bit ASCII and quote field names when writing JSON output.

Jackson feature compatibility depends on the way a feature operates on the resulting JSON tree.
Generally, DSBulk doesn't support Jackson features that filter elements or alter the content of elements in the JSON tree because these features conflict with DSBulk's built-in filtering and formatting capabilities.
Instead of using Jackson features to modify the JSON tree, try using the DSBulk [`codec`](codec-options.md) and [`schema`](schema-options.md) options.

Default: `{ }` (no JSON generator features enabled)

### <a id="jsonConcurrentFiles"></a>--connector.json.maxConcurrentFiles (-maxConcurrentFiles)

The maximum number of files to load or unload simultaneously.

Allowed values include the following:

- **`AUTO` (default)**: The connector estimates an optimal number of files automatically.
- **`NC`**: A special syntax that you can use to set the number of threads as a multiple of the number of available cores for a given operation.
  For example, if you set `-maxConcurrentFiles 0.5C` and there are 8 cores, then there will be 4 parallel threads (`0.5 * 8 = 4`).
- **Positive integer**: Specifies the exact number of files to read or write in parallel.
  For example, `1` reads or writes one file at a time.

> [!TIP]
> With `dsbulk load`, it can be helpful to reduce this value if the disk is slow, especially SAN disks.
> Excessive disk IO can perform worse than reading files individually (`-maxConcurrentFiles 1`).
> If diagnostic tools like `iostat` show too much time spent on disk IO, consider adjusting `maxConcurrentFiles` to a lower value, `AUTO`, or `1`.
>
> Rows larger than 10KB can also benefit from a lower `maxConcurrentFiles` value.

### <a id="jsonmaxRecords"></a>--connector.json.maxRecords (-maxRecords)

Specify the maximum number of records to load from or unload to each file.
The default is `-1` (unlimited).

#### `-maxRecords` with `dsbulk load`

If `-maxRecords` is set to a positive integer, then all records past the maximum number are ignored.
For example, if `-maxRecords 1000`, only the first 1000 records from each input file are loaded.

#### `-maxRecords` with `dsbulk unload`

If `-maxRecords` is set to a positive integer, then each output file will contain no more than the maximum number of records.

If the total records to unload exceeds `-maxRecords`, then DSBulk creates additional files.
File names are determined by the `fileNameFormat` option.

If `-maxRecords` is set to `-1`, then the `unload` operation writes all records to one file.

> [!IMPORTANT]
> This option is ignored if the output destination _isn't_ a directory.

### <a id="connectorJsonMode"></a>--connector.json.mode

The mode for loading and unloading JSON documents.

#### `--connector.json.mode` with `dsbulk load`

When loading JSON documents, the `mode` option has the following behavior:

- **`MULTI_DOCUMENT` (default)**: The DSBulk parser expects that each input resource can contain an arbitrary number of successive JSON documents to be mapped to records.
  For example, the format of each JSON resource is a single document, such as `{doc1}`.

  You can specify the root directory for the JSON resources with [`-url`](#dsbulk-jsonUrl), and DSBulk can read the resources recursively if [`connector.json.recursive true`](#jsonRecursive).

- **`SINGLE_DOCUMENT`**: The DSBulk parser expects that each input resource contains a root array whose elements are JSON documents to be mapped to records.
  For example, the format of each JSON resource is an array with embedded JSON documents, such as `[ {doc1}, {doc2}, {doc3} ]`.

#### `--connector.json.mode` with `dsbulk unload`

- **`MULTI_DOCUMENT` (default)**: The DSBulk writer expects that each output resource can contain an arbitrary number of successive JSON documents to be mapped to records.
  For example, the format of each JSON output resource is a single document, such as `{doc1}`.
- **`SINGLE_DOCUMENT`**: The DSBulk writer expects that each output resource contains a root array whose elements are JSON documents to be mapped to records.
  For example, the format of each JSON output resource is an array with embedded JSON documents, such as `[ {doc1}, {doc2}, {doc3} ]`.

### <a id="connectorJSONparserFeatures"></a>--connector.json.parserFeatures

For `dsbulk load` operations only, you can specify JSON parser features to enable in the form of `map<String,Boolean>`.

Accepts any enum constants defined in [`com.fasterxml.jackson.core.JsonParser.Feature`](https://www.javadoc.io/doc/com.fasterxml.jackson.core/jackson-core/latest/com/fasterxml/jackson/core/JsonParser.Feature.html) for Jackson features that are supported by DSBulk.
For example, the map `{ ALLOW_COMMENTS : true, ALLOW_SINGLE_QUOTES : true }` configures the parser to allow comments and single-quoted strings in JSON data.

Jackson feature compatibility depends on the way a feature operates on the resulting JSON tree.
Generally, DSBulk doesn't support Jackson features that filter elements or alter the content of elements in the JSON tree because these features conflict with DSBulk's built-in filtering and formatting capabilities.
Instead of using Jackson features to modify the JSON tree, try using the DSBulk [`codec`](codec-options.md) and [`schema`](schema-options.md) options.

Default: `{ }` (no JSON parser features enabled)

### <a id="connectorJSONprettyPrint"></a>--connector.json.prettyPrint

Whether to use pretty printing for JSON output from the `dsbulk unload` command.
This option doesn't apply to `dsbulk load`.

- **`false` (default)**: Disable pretty printing to write JSON records in a compact format without extra spaces or line breaks.
- **`true`**: Sets the pretty printer. In 1.11.2 the records stay on one line unless `--connector.json.serializationFeatures '{INDENT_OUTPUT:true}'` is also set.

  > [!IMPORTANT]
  > Enabling `prettyPrint` produces much larger JSON records.

### <a id="jsonRecursive"></a>--connector.json.recursive

Whether to load files from subdirectories if the `-url` option points a directory.

Ignored if `-url` _isn't_ a file path to a directory.

Not applicable to the `dsbulk unload` command.

Default: `false` (no recursion)

### <a id="connectorJSONserializationFeatures"></a>--connector.json.serializationFeatures

For `dsbulk unload` operations only, you can set JSON serialization features in the form of `map<String,Boolean>`.

Map keys must be enum constants defined in [`Enum SerializationFeature`](https://fasterxml.github.io/jackson-databind/javadoc/2.6/com/fasterxml/jackson/databind/SerializationFeature.html) for Jackson features that are supported by DSBulk.

Jackson feature compatibility depends on the way a feature operates on the resulting JSON tree.
Generally, DSBulk doesn't support Jackson features that filter elements or alter the content of elements in the JSON tree because these features conflict with DSBulk's built-in filtering and formatting capabilities.
Instead of using Jackson features to modify the JSON tree, try using the DSBulk [`codec`](codec-options.md) and [`schema`](schema-options.md) options.

Default: `{ }` (no JSON serialization features set)

### <a id="connectorJSONserializationStrategy"></a>--connector.json.serializationStrategy

For `dsbulk unload` operations only, you can set a strategy for filtering unwanted entries when formatting output.

Accepts any enum constant defined in [`com.fasterxml.jackson.annotation.JsonInclude.Include`](https://javadoc.io/doc/com.fasterxml.jackson.core/jackson-annotations/latest/com.fasterxml.jackson.annotation/com/fasterxml/jackson/annotation/JsonInclude.Include.html) except `CUSTOM`.

Default: `ALWAYS` (include all entries; no filtering)

### <a id="jsonSkipRecords"></a>--connector.json.skipRecords (-skipRecords)

With `dsbulk load` only, you can specify the number of records to bypass (skip) before the parser begins processing the input file.
The default is `0` (no records skipped).

Applies to all files loaded by a given `dsbulk load` command.
It cannot be selectively applied.

### <a id="dsbulk-jsonUrl"></a>--connector.json.url (`-url`)

Specify the source or destination for a `load` or `unload` operation.

Use [quotes and escaping](../02-usage.md#escape-and-quote-command-line-arguments) as needed for the `-url` string.

> [!IMPORTANT]
> `-url` cannot be used with the `urlfile` option.
> If both are specified,then `urlfile` takes precedence.

For more examples, see [Load data](../dsbulk-load.md) and [Unload data](../dsbulk-unload.md).

#### `-url` with `dsbulk load`

For a `dsbulk load` operation, specify the location where the input files are stored.
Allowed values include the following:

- **Standard input**: Specified by `-`.
  This is the default source if `-url` is omitted.
- **URL**: If `-url` begins with `http:` or `https:`, the source is read directly, and options like `fileNamePattern` and `recursive` are ignored.

  AWS S3 URLs must contain the necessary query parameters for DSBulk to build an `S3Client` and access the target bucket.
  For more information, see [Load from AWS S3](../dsbulk-load.md#load-from-aws-s3).

- **File path**: Specify a local or remote file or directory.

  If the target is a directory, `dsbulk load` processes all files in the directory that match the `fileNamePattern`.
  To read from a directory and its subdirectories, include the `recursive` option.

  Relative paths are resolved against the current working directory.
  Paths that begin with a tilde (`~`) resolve to the current user's home directory, and then follow the path from there.

  The `file:` prefix is accepted but optional.
  If `-url` doesn't begin with `file:`, `http:`, or `https:`, it is assumed to be a file path.

#### `-url` with `dsbulk unload`

For a `dsbulk unload` operation, specify the destination where the output will be written.
Allowed values include the following:

- **Standard output**: Specified by `-`.
  This is the default destination if `-url` is omitted.
- **URL**: If `-url` begins with `http:` or `https:`, the output is written directly to the given URL, and options like `fileNameFormat` are ignored.

  > [!IMPORTANT]
  > Some URLs aren't supported by `dsbulk unload`.
  > If the current user doesn't have write permissions for the target URL, the output isn't written to the given URL.
  >
  > DSBulk cannot unload directly to AWS S3.
  > Instead, you can pipe the `dsbulk unload` output to a command that uploads the files to S3 using an AWS CLI, SDK, or API.

- **File path**: Specify a local or remote directory.

  For `dsbulk unload`, a file path target is always treated as a directory.
  If the directory doesn't exist, DSBulk attempts to create it.
  The `fileNameFormat` option sets the naming convention for the output files.

  Relative paths are resolved against the current working directory.
  Paths that begin with a tilde (`~`) resolve to the current user's home directory, and then follow the path from there.

  The `file:` prefix is accepted but optional.
  If `-url` doesn't begin with `file:`, `http:`, or `https:`, it is assumed to be a file path.

For example:

- **Target a remote file**: `-url https://192.168.1.100/data/file.json`
- **Target a directory**: `-url path/to/directory/`
- **Target a local file, navigating from the current user's home directory**: `-url ~/file.json`
- **Target a compressed file**: Use the `-url` and `compression` options

### <a id="jsonUrlFile"></a>--connector.json.urlfile

For `dsbulk load` only, you can use this option to load multiple files from various URLs and paths.
Create a local `.txt` file that contains a list of URLs or paths to files that you want to load, and then point `urlfile` to that local file.

By default, this option is not set and not used.

> [!IMPORTANT]
> - `urlfile` cannot be used with the `-url` option.
>   If both are specified,then `urlfile` takes precedence.
>   If neither are specified, then the default of `-url -` (standard input) is used.
> - Don't use `urlfile` with `dsbulk unload`.
>   This causes a fatal error.

The following requirements apply to the local file targeted by `urlfile`:

- Must be UTF-8 encoded.
- Each line must contain only one valid path or URL.
- Don't escape characters inside the file.
- Use `#` for comment lines.
- Leading and trailing white space is trimmed from each line.
- Related `connector` options, such as `fileNamePattern` and `recursive`, are respected when resolving file paths in `urlfile`.

When using the `urlfile` option with AWS S3 URLs, DSBulk creates an S3 client for each bucket specified in the S3 URLs.
DSBulk caches the S3 clients to prevent them from being re-created unnecessarily when processing many S3 URLs that target the same buckets.
If all of your S3 URLs target the same bucket, then the same S3 client is used for each URL, and the cache contains only one entry.
The size of the S3 client cache is controlled by the `--s3.clientCacheSize` (`--dsbulk.s3.clientCacheSize`) option, and the default is `20` entries.
The default value is arbitrary, and it only needs to be changed when loading from many different S3 buckets in a single command.

## See also

- [Schema options](schema-options.md)
- [Codec options](codec-options.md)
