# DataStax Bulk Loader Workflows

Workflows form a pluggable abstraction that allows DSBulk to execute virtually any kind of 
operation.

This module groups together submodules related to workflows:

1. The [dsbulk-workflow-api](./api) submodule contains the Workflow API.
2. The [dsbulk-workflow-commons](./commons) submodule contains common base classes for workflows,
   and especially configuration utilities shared by DSBulk's built-in workflows (load, unload and 
   count).
3. The [dsbulk-workflow-load](./load) submodule contains the Load Workflow.
4. The [dsbulk-workflow-unload](./unload) submodule contains the Unload Workflow.
5. The [dsbulk-workflow-count](./count) submodule contains the Count Workflow.
