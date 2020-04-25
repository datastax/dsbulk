# DataStax Bulk Loader Codec API

This module contains an API for codec handling in DSBulk:

1. `ConvertingCodec`: a subtype of `TypeCodec` that handles conversions between an "external" Java 
   type and an "internal" one.
3. `ConvertingCodecProvider`: a provider for `ConvertingCodec`s; implementors can
   provide codecs by registering their providers with the Service Loader API.
2. `ConvertingCodecFactory`: a factory for `ConvertingCodec`s; it discovers 
   `ConvertingCodecProvider` implementations through the Service Loader API.
