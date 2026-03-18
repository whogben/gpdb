# Procedure for Migrating a Standard Single-Object Method to a Bulk Operation Method

This procedure is for converting a single operation tool like graph_get_node to a bulk operation tool like graph_get_nodes.

The objective is to change, one by one, each method that works on a node id, a schema name, or an edge id, to be capable of accepting and working on multiple ids at once.

As you work, use the following procedure to do AT MAXIMUM 1 METHOD AT A TIME.

## Required Decisions For Every Migration

These are global rules for this migration effort. Do not make case-by-case decisions unless this document explicitly says to.

- This is a replace operation, not an add-alongside operation. Remove the old single-item method and update all call sites to the new bulk method.
- Pluralize method names as part of the replacement as we go. Example: `get_node` becomes `get_nodes`, `set_node` becomes `set_nodes`, `delete_schema` becomes `delete_schemas`.
- For bulk get operations, if any requested item does not exist, fail the entire call. Do not silently omit missing items and do not return placeholder `None` values.
- Reject duplicate input identifiers within the same bulk call. Do not deduplicate them automatically.
- Preserving input order in returned results is ideal. However, every returned object must carry its own identity field so callers can always match results to inputs.
- For single-item callers in admin/web/tools, do not create dedicated single-item adapters. Always wrap the single item into a one-item batch request and then unwrap the single result from the batch response.
- When unwrapping a one-item batch response, verify that the response shape is what the caller expects. Do not blindly use the first item unless it is correct to do so for that method.


## 1 - Update Underlying GPDB Implementation

To begin, we must find the relevant underlying GPDB method and update it from a single operation to a bulk operation. Look in src/gpdb/graph.py to find the method that we are interested in.


### Get Methods

If the starting signature is, for example:
```python
async def get_edge(self, id: str) -> EdgeRead | None:
```

The new signature would be changed to:
```python
async def get_edges(self, ids:list[str]) -> list[EdgeRead]:
```

Important behavior for bulk get methods:

- Reject duplicate ids before doing any work.
- If any requested id is missing, fail the entire call.
- Return objects with their identity field present.
- Preserve input order if reasonably possible.

Specific case: `get_node_payloads` should return a list of `NodeReadWithPayload`, but it must have an `id` added to the `NodeReadWithPayload` object, and nodes with no payload should still be returned in the list, with the `id` filled and no payload bytes set.


### Create/Set Methods

If the starting signature is, for example:
```python
async def set_node(self, node: NodeUpsert) -> NodeRead:
```

Then the new signature would become:
```python
async def set_nodes(self, nodes:list[NodeUpsert]) -> list[NodeRead]
```

Important behavior for bulk create/set methods:

- Reject duplicate ids in the input before doing any database writes.
- Perform the entire batch atomically.
- Preserve the existing semantics of omitted fields on update paths.
- Think very carefully about any existing retry or split create/update logic so that the new bulk version does not accidentally introduce partial success behavior.


### Delete Methods

If the starting signature is, for example:
```python
async def delete_schema(self, name: str) -> None:
```

Then the new signature would simply be:
```python
async def delete_schemas(self, names: list[str]) -> None:
```

Important behavior for bulk delete methods:

- Reject duplicate identifiers before doing any work.
- If any one deletion would fail, fail the entire batch.
- Keep the failure context clear so the caller can tell why the batch was rejected.


### Note on Atomicity & Error Handling
The new methods should perform all the work inside one transaction, ensuring that if an error occurs, the entire operation fails and does not partially complete. Any error that occurs with one element should propagate back ultimately resulting in context for the client who is calling it, as it currently does today.

Before finishing the GPDB method, explicitly verify:

- duplicate inputs are rejected,
- missing items cause the entire bulk get to fail,
- the returned objects still include their identity fields,
- the whole operation is still atomic.

## 2 - Update Underlying GPDB Tests

After you update the GPDB implementation, find the gpdb tests that reference it.
These tests will be specific to the gpdb base package and not require the gpdb-admin.
Ensure the tests of the underlying method have been updated correctly to use the new signature, and that the updated tests are passing before we continue.

The updated gpdb tests should cover the new bulk rules when applicable:

- success path for multiple inputs,
- duplicate input rejection,
- failure when one requested item is missing for bulk get methods,
- atomic all-or-nothing behavior when one item in the batch fails,
- preservation of update semantics for omitted optional fields.

At the end of this step, the underlying gpdb package is up to date with the new methods.


## 3 - Update GPDB Admin Services That Call The Underlying GPDB Implementation

After you update the GPDB implementation, find all places that were calling the old method from gpdb admin, and update them to use the new method. Look inside gpdb_admin/src/gpdb/admin/.., especially the entry.py and graph_content.py - as well as inside the admin/web/.. routes.

Locate the tool or tools that wrap the underlying GPDB method and make it available.

For example, if we just updated gpdb's graph.py `register_schema(..)` method, turning it into `register_schemas(..)`, then we will need to update both `update_graph_schema` and `create_graph_schema` in graph_content.py service.


### If Needed, Extract Service Arguments into a Pydantic Model

Some service methods have individual named arguments, instead of wrapping them in a schema. This doesn't work for bulk updating, and so the args must first be extracted into a Pydantic object, before the method can be modified to accept a list of them.

For example with create_graph_schema, the initial signature is:
```python
async def create_graph_schema(
    self,
    *,
    graph_id: str,
    name: str,
    json_schema: dict[str, Any],
    current_user: AdminUser | None,
    kind: str = "node",
    allow_local_system: bool = False,
) -> GraphSchemaDetail:
```

Therefore a Pydantic object would need to be extracted, and in this example the signature would be updated to:
```python
class GraphSchemaCreateParam(BaseModel):
    name: str = Field(..)
    json_schema: dict[str, Any] = Field(..)
    kind: str = Field(default="node")

async def create_graph_schema(
    self,
    *,
    graph_id: str,
    schemas:list[GraphSchemaCreateParam],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphSchemaDetail]:
```

Note 1: See how we only extracted to Pydantic the args that correspond to the bulk edited objects, and did not extract out the common args that apply to all, like graph_id.

Note 2: When extracting the pydantic object for an update method, make all of the fields (except the id) optional, and ensure that we are not passing anything into the underlying gpdb so that data is properly preserved for those methods.

Note 3: Keep single-item service callers on the bulk path too. If a service method is still conceptually "one item" at the call site, it should still wrap the item into a one-element bulk request and unwrap the one result rather than keeping a separate single-item code path.


## 4 - Update GPDB Admin Tool Definitions that use the updated Admin Service Method(s)

Next locate any Tools that use the updated service method(s), they will likely be defined either in admin/entry.py or in admin/tools/.., especially admin/tools/graph.py. These need to be changed to be bulk compatible as well.

### Extract Pydantic Schema Object as Needed

The first step is to determine if the Params object needs to have a new Pydantic model extracted from it to support bulk operations.

For example, for NodeUpdateParams, the initial definition is this:
```python
class NodeUpdateParams(BaseModel):
    """Parameters for updating a graph node. Omitted fields are left unchanged."""

    graph_id: str = Field(..., description="Graph ID.")
    node_id: str = Field(..., description="Node ID.")
    type: str | None = Field(None, description="Node type.")
    data: dict[str, object] | None = Field(None, description="Node data as JSON object.")
    name: str | None = Field(None, description="Node name.")
    schema_name: str | None = Field(None, description="Schema name.")
    owner_id: str | None = Field(None, description="Owner ID.")
    parent_id: str | None = Field(None, description="Parent node ID.")
    tags: list[str] | None = Field(None, description="Node tags.")
    payload_base64: str | None = Field(None, description="Base64-encoded payload data.")
    payload_mime: str | None = Field(None, description="MIME type of the payload.")
    payload_filename: str | None = Field(None, description="Filename for the payload.")
    clear_payload: bool = Field(
        default=False, description="Whether to clear the payload."
    )
```

And it needs to have the bulk elements extracted to a separate model class, so that the Params can be bulk-enabled while remaining fully typed. It would become two classes:
```python
class NodeUpdateParam(BaseModel):
    """Parameters for updating a graph node. Omitted fields are left unchanged."""

    node_id: str = Field(..., description="Node ID.")
    type: str | None = Field(None, description="Node type.")
    data: dict[str, object] | None = Field(None, description="Node data as JSON object.")
    name: str | None = Field(None, description="Node name.")
    schema_name: str | None = Field(None, description="Schema name.")
    owner_id: str | None = Field(None, description="Owner ID.")
    parent_id: str | None = Field(None, description="Parent node ID.")
    tags: list[str] | None = Field(None, description="Node tags.")
    payload_base64: str | None = Field(None, description="Base64-encoded payload data.")
    payload_mime: str | None = Field(None, description="MIME type of the payload.")
    payload_filename: str | None = Field(None, description="Filename for the payload.")
    clear_payload: bool = Field(
        default=False, description="Whether to clear the payload."
    )

class NodeUpdateParams(BaseModel):
    """Parameters for updating a graph node. Omitted fields are left unchanged."""

    graph_id: str = Field(..., description="Graph ID.")
    nodes:list[NodeUpdateParam]
```

Then update the builder, where the params are actually used, so that they call the underlying service in the way that it expects.

Note that the tool param definitions are where the actual documentation comes from, so these need docstrings, while the service param definitions don't require it.


### Update the Web Routes to Use the Updated Tool / Service Method(s)

Our web routes are located in admin/web/.. and may use the tools and/or services internally. Locate all instances of use of the updated tool / service method(s) and make the appropriate modifications for them to work with the new bulk methods.

Think carefully through the modification, considering both the old signature and behavior, and how to adapt this to the new bulk method. For single-item web pages and forms, this means wrapping the one item into a one-item batch request and then unwrapping the one returned result. Do not create dedicated single-item adapter methods just for the web layer.

When adapting single-item callers:

- wrap the item into a one-item batch request,
- unwrap the one returned result intentionally,
- fail clearly if the batch response is not valid for a single-item caller,
- do not blindly assume "take index 0" is always safe without confirming the response shape.

### Update the Appropriate GPDB Admin Tests and ensure they pass 

The admin has its own tests, which will need modifications to adapt to the new schema.

Think carefully - once you locate the relevant tests, look up the new method(s)' signatures and adapt both the params sent by the test, and the responses expected.

Be careful not to lose any of our currently tested functionality, we need to ensure that test coverage stays at least as good or improves.

For admin tests, include batch behavior coverage when it is relevant to the migrated method. At minimum think about:

- successful multi-item calls,
- duplicate input rejection,
- error propagation when one item fails,
- single-item callers still working correctly via one-item batch wrapping.

Also update any README or user-facing documentation that still describes the old replaced signature.


## Final Review Process

After all tests are again passing, perform a thorough review of the work looking for all kinds of errors, including (but not limited to):

-  Does the underlying operation either all-fail or all succeed?  If it is possible to wind up with a partial success, the work is not correct - we should either completely succeed, or make no changes to the graph.

-  Do errors with the underlying operation make it out to the caller of the APIs / MCP client etc? If not, the work is not correct - we must be able to get some context on why things fail back to the caller.

-  Were all old singular names actually removed and replaced with the new pluralized names? This is a replacement migration, not an add-alongside migration.

-  Are all single-item callers now using the one-item batch path instead of retaining a hidden single-item implementation?

-  Are duplicate inputs rejected consistently at every relevant layer?

-  For bulk get methods, does a missing requested item fail the entire call instead of producing a partial response?

-  Do all returned objects still contain their identity fields so callers can map responses correctly?

-  Did all documentation and examples get updated to the new replaced signature?


## Incremental Process

As stated at the start, perform this ENTIRE process for each method that is to be converted to batch, without moving onto the next method until the process and all associated changes and reviews are passing successfully.