"""
worked_multi_source -- fetch from multiple sources in parallel and assemble.

Three fetch steps run concurrently, each simulating a call to a
different data source (user record, order summary, account profile).
A final assembly step depends on all three and combines their outputs
into a single enriched record.

In a real pipeline the fetch steps would make HTTP requests, query
databases, or call internal services. The executor here returns static
dicts keyed by source name to keep the example self-contained.

This is the fan-in pattern applied to data retrieval: independent
sources, single assembly point, no intermediate state accumulation
required because the assembly step receives each source output directly
via var references.

Demonstrates: parallel root steps (no depends_on), three-way fan-in,
              input resolution from multiple prior steps,
              workflow inputs to parameterise the fetch targets.
"""

import pprint

import runfox as rfx

# Simulated data sources, keyed by source name and entity id.
DATA_STORE = {
    "users":   {42: {"id": 42, "name": "Alice", "joined": "2021-03-15"}},
    "orders":  {42: {"user_id": 42, "count": 3, "total": 198.50}},
    "profile": {42: {"user_id": 42, "tier": "gold", "region": "eu-west"}},
}

SPEC = """
name: multi_source

steps:
  - op: fetch_user
    input:
      source: users
      id:     {"var": "input.user_id"}

  - op: fetch_orders
    input:
      source: orders
      id:     {"var": "input.user_id"}

  - op: fetch_profile
    input:
      source: profile
      id:     {"var": "input.user_id"}

  - op: assemble
    depends_on: [fetch_user, fetch_orders, fetch_profile]
    input:
      user:    {"var": "steps.fetch_user.output.record"}
      orders:  {"var": "steps.fetch_orders.output.record"}
      profile: {"var": "steps.fetch_profile.output.record"}

outputs:
  record: {"var": "steps.assemble.output.record"}
"""


def execute(op: str, inputs: dict) -> dict:
    if op == "fetch_user":
        return {"record": DATA_STORE["users"][inputs["id"]]}

    if op == "fetch_orders":
        return {"record": DATA_STORE["orders"][inputs["id"]]}

    if op == "fetch_profile":
        return {"record": DATA_STORE["profile"][inputs["id"]]}

    if op == "assemble":
        return {
            "record": {
                **inputs["user"],
                **inputs["orders"],
                **inputs["profile"],
            }
        }


backend = rfx.Backend(executor=execute)
wf = rfx.Workflow.from_yaml(SPEC, backend, inputs={"user_id": 42})
result = wf.run()

pprint.pprint(result.outcome)
# {'record': {'count': 3,
#             'id': 42,
#             'joined': '2021-03-15',
#             'name': 'Alice',
#             'region': 'eu-west',
#             'tier': 'gold',
#             'total': 198.5,
#             'user_id': 42}}
