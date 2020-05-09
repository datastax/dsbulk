# DataStax Bulk Loader Batcher

The Batcher API allows DSBulk to group statements together in batches using different grouping 
criteria and sizing characteristics.

This module has two submodules:

1. The [dsbulk-batcher-api](./api) submodule contains the Batcher API.
2. The [dsbulk-batcher-reactor](./reactor) submodule contains an implementation of the Batcher API
   using Reactor.
