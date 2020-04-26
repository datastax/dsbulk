# DataStax Bulk Loader Codecs - Text

This module contains implementations of the ConvertingCodec API to convert to and from common JDK types:

1. Boolean
2. Numbers
3. Temporals (both Java Time API and java.util.Date)
4. Collections (Lists, Maps, Sets)
5. UUID

Some of the above types may also be converted into driver types such ad `UdtValue` and `TupleValue`.

DSBulk itself does not use this module, since, as the time of writing (May 2020), it can only
convert to/from text (Strings and Json). However other projects rely on the conversions provided
in this module, e.g. the DataStax Kafka Connector Sink.