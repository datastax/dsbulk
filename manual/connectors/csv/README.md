# DataStax Bulk Loader/Unloader CSV connector

The [CSV Connector] is a highly-configurable connector that reads files in field-delimited format.

For more information about the CSV file format, see [RFC 4180]
Adn the [Wikipedia article on CSV format].

[CSV Connector]: ../../connectors/csv/src/main/java/com/datastax/dsbulk/connectors/csv/CSVConnector.java
[RFC 4180]: https://tools.ietf.org/html/rfc4180
[Wikipedia article on CSV format]: https://en.wikipedia.org/wiki/Comma-separated_values

## Available Settings

* url

  The URL of the resource(s) to read.
  This setting has no default value and must be supplied by the user.
  Which protocols are available depend on which URL stream handlers
  have been installed, but at least one protocol is guaranteed to be supported:
  - file:  the file protocol can be used with all supported file systems, local or not;
           it also supports reading from a single file, or all files from a directory;
           in case of a directory, the "pattern" setting can be used to filter files to read,
           and the "recursive" setting can be used to control whether or
           not the connector should look for files in subdirectories as well.
           Example: 
           
           url = "file:///path/to/dir/or/file"

* pattern

  The glob pattern to use when searching for files to read.
  The syntax to use is the glob syntax, as described in
  java.nio.file.FileSystem.getPathMatcher().
  Only applicable when the "url" setting points to a directory
  on a known filesystem, ignored otherwise.
  Defaults to `**/*.csv`.

* recursive

  Whether to scan for files in subdirectories of the root directory.
  Only applicable when the "url" setting points to a directory
  on a known filesystem, ignored otherwise.
  Defaults to `false`.

* maxThreads

  The maximum number of reading threads.
  In other words, this setting controls how many files
  can be read simultaneously.
  Only applicable when the "url" setting points to a directory
  on a known filesystem, ignored otherwise.
  Defaults to 4.

* encoding

  The file encoding to use.
  Note that this setting applies to all files to be read.
  Defaults to `UTF-8`.

* header

  Whether the files to read begin with a header line or not.
  Defaults to `false` (no header line).
  When set to `true`, the first line in every file is discarded,
  even if the "skipLines" setting is set to zero (see below).
  However, that line will be used to assign field names to
  each record, thus allowing mappings by field name such
  as `{ myFieldName1 = myColumnName1, myFieldName2 = myColumnName2 }`.
  When set to false, records will not contain field names,
  only (zero-based) field indexes; in this case,
  the statement mapping should be index-based, such
  as in `{ 0 = myColumnName1, 1 = myColumnName2}`.
  Note that this setting applies to all files to be read.

* delimiter

  The character to use as field delimiter.
  Defaults to `,` (comma).
  Only one character can be specified.
  Note that this setting applies to all files to be read.

* quote

  The character used for quoting fields when the field delimiter is part of the field value.
  Defaults to `"` (double quote).
  Only one character can be specified.
  Note that this setting applies to all files to be read.

* escape

  The character used for escaping quotes inside an already quoted value.
  Defaults to <code>&#92;</code> (backslash).
  Only one character can be specified.
  Note that this setting applies to all files to be read.

* comment

  The character that represents a line comment when found in the beginning of a line of text.
  Defaults to `\0`, which disables this feature.
  Only one character can be specified.
  Note that this setting applies to all files to be read.

* skipLines

  Defines a number of lines to skip from each input file before the parser can begin to execute.
  Defaults to zero (i.e., do not skip any lines)
  Note that this setting applies to each input file individually.

* maxLines

  Defines the maximum number of lines to read from each input file.
  All lines past this number will be discarded.
  Defaults to -1, which disables this feature.
  Note that this setting applies to each input file individually.

