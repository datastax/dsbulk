# DataStax Bulk Loader Codecs

This module groups together submodules related to codec handling in DSBulk:

1. The [dsbulk-codecs-api](./api) submodule contains an API for codec handling in DSBulk.
2. The [dsbulk-codecs-text](./text) submodule contains implementations of that API for plain text 
   and Json.
1. The [dsbulk-codecs-jdk](./jdk) submodule contains implementations of that API for converting to
   and from common JDK types: Boolean, Number, Temporal, UUID, Collections, and some driver types 
   (TupleValue and UdtValue).
