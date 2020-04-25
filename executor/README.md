# DataStax Bulk Loader Executor

DSBulk's Executor is a pluggable abstraction that allows DSBulk to execute queries using reactive
programming.

**Important**: this module exists solely because initially the DataStax Java driver did not contain 
an API to execute queries reactively. Now that the driver exposes this feature, this module and its 
submodules should be considered as deprecated. DSBulk might remove this API and its implementations 
entirely in the near future, and replace them by the driver's equivalent API.

However, the Executor API includes some features that are not present in the driver, such as the
StatementBatcher class and its child classes, and the Execution Listener API. These features and 
APIs are likely to remain.

This module groups together submodules related to reactive execution of queries:

1. The [dsbulk-executor-api](./api) submodule contains the Executor API.
2. The [dsbulk-executor-reactor](./reactor) submodule contains an implementation of the Executor API
   using Reactor.
2. The [dsbulk-executor-rxjava](./rxjava) submodule contains an implementation of the Executor API
   using RxJava.
