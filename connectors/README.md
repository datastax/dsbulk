# DataStax Bulk Loader Connectors

Connectors form a pluggable abstraction that allows DSBulk to read and write to a variety of
backends.

This module groups together submodules related to connectors:

1. The [dsbulk-connectors-api](./api) submodule contains the Connector API.
2. The [dsbulk-connectors-commons](./commons) submodule contains common base classes for text-based
   connectors.
3. The [dsbulk-connectors-csv](./csv) submodule contains the CSV connector.
4. The [dsbulk-connectors-json](./json) submodule contains the Json connector.
