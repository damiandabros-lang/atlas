# ATLAS — internal interface contracts (frozen for parallel implementation)

All agents build against these signatures. Do not change them; if you must, document it in your report.

## Package layout
```
atlas/
  core/        enums.py lifecycle.py config.py            (DONE — read-only)
  db/          models.py repositories.py (signatures DONE) session.py seed.py   [DB agent]
  schemas/     __init__.py                                (DONE — read-only)
  adapters/    base.py (DONE) mock.py iosxr.py junos.py registry.py credentials.py templates/*.j2  [Adapters agent]
  services/    config_generator.py allocator.py           [Adapters agent / DB agent]
               service_controller.py temporal_client.py  [API agent]
  workflows/   provision_l3vpn.py decommission_l3vpn.py reconcile_service.py saga.py policies.py [Temporal agent]
  activities/  validation.py allocation.py configuration.py device_ops.py verification.py state.py reconciliation.py [Temporal agent]
  worker/      main.py                                   [Temporal agent]
  api/         main.py deps.py auth.py idempotency.py errors.py routers/{services,devices,health}.py [API agent]
  reconciliation/ engine.py                              [Recon/Obs agent]
  observability/ logging.py metrics.py tracing.py        [Recon/Obs agent]
alembic/, deploy/, .github/, Makefile, README.md, docs/  [Infra agent]
tests/unit/<area>_test_*.py  tests/integration/
```

## Session / unit of work  (`atlas/db/session.py`, DB agent)
```python
def get_engine() -> AsyncEngine                       # cached, from Settings.database_url
def get_sessionmaker() -> async_sessionmaker[AsyncSession]
@asynccontextmanager
async def transaction() -> AsyncIterator[AsyncSession]  # begin; yield; commit / rollback on error
async def get_session() -> AsyncIterator[AsyncSession]  # FastAPI dependency (no auto-commit)
async def init_db_for_tests(url="sqlite+aiosqlite:///:memory:") -> async_sessionmaker  # create_all
```

## Allocator  (`atlas/services/allocator.py`, DB agent)
```python
class ResourceAllocator:
    def __init__(self, session: AsyncSession, settings: Settings): ...
    async def allocate_for_service(self, service: NetworkService, request: L3VPNServiceRequest) -> AllocatedResources
    # Idempotent: re-running returns the same values. Allocates: RT (global, from request or pool),
    # RD per endpoint (global, "<asn>:<n>"), VLAN per endpoint (scope "device:<HOSTNAME>", from request or pool),
    # /30 subnet per endpoint from pe_ce_supernet (global) -> pe_ip = first host, ce_ip = second host,
    # VRF name (global unique). Updates ServiceEndpoint rows (rd, pe_ip, ce_ip, vlan_id).
    async def release_for_service(self, service_id: UUID) -> int
```

## Config generation  (`atlas/services/config_generator.py`, Adapters agent)
```python
def generate_device_configs(resources: AllocatedResources) -> list[DeviceConfig]   # one per endpoint, ordered by sequence
def build_intent(ep: EndpointResources) -> dict   # vendor-neutral: {"vrf": {...}, "interface": {...}, "bgp": {...}}
def checksum(text: str) -> str                     # sha256 hex
```
Intent shape (used by verify + reconciliation):
```json
{"vrf": {"name": "ACME-PROD", "rd": "65000:1001", "rt_import": ["65000:100"], "rt_export": ["65000:100"]},
 "interface": {"name": "GigabitEthernet0/0/0/1", "vlan": 100, "ip": "10.255.0.1", "prefix_length": 30, "vrf": "ACME-PROD", "admin_up": true},
 "bgp": {"asn": 65000, "vrf": "ACME-PROD", "neighbor": "10.255.0.2", "remote_asn": 65001}  // or "static": {"prefix": ..., "next_hop": ...}
}
```

## Adapter registry  (`atlas/adapters/registry.py`, Adapters agent)
```python
def get_adapter(vendor: Vendor, settings: Settings | None = None) -> NetworkAdapter   # cached singletons per vendor
def handle_from_device(device: NetworkDevice) -> DeviceHandle
MockNetworkAdapter(settings)  # atlas/adapters/mock.py; state store class-level/shared per process; .reset(); .set_failure_mode(mode); .state(hostname) -> dict
```

## Temporal client  (`atlas/services/temporal_client.py`, API agent)
```python
async def get_temporal_client() -> temporalio.client.Client   # cached
def provision_workflow_id(service_id: UUID) -> str   # f"provision-l3vpn-{service_id}"
def decommission_workflow_id(service_id: UUID) -> str
def reconcile_workflow_id(service_id: UUID) -> str
```

## Workflows (Temporal agent)
- `ProvisionL3VPNWorkflow.run(ProvisionRequest) -> ProvisionResult`; query `progress -> WorkflowProgress`; signal `cancel()`.
- `DecommissionL3VPNWorkflow.run(DecommissionRequest) -> ProvisionResult`
- `ReconcileServiceWorkflow.run(ReconcileRequest) -> ReconciliationReport`
- Task queue: `Settings.temporal_task_queue`. Activities take/return the Pydantic models in `atlas.schemas` (use `pydantic_data_converter` from `temporalio.contrib.pydantic`).

## Observability (Recon/Obs agent)
```python
atlas.observability.logging.configure_logging(settings); get_logger(**bind) -> structlog BoundLogger; bind_context(correlation_id=..., service_id=..., workflow_id=..., device_id=..., activity=...)
atlas.observability.metrics: WORKFLOW_TOTAL, WORKFLOW_FAILURES_TOTAL, WORKFLOW_DURATION_SECONDS, DEVICE_OPERATIONS_TOTAL, DEVICE_OPERATION_FAILURES_TOTAL, RECONCILIATION_DRIFT_TOTAL ; metrics_asgi_app()  (labels: workflow_type, status | device_role/vendor, operation_type)
atlas.observability.tracing.configure_tracing(settings, app: FastAPI | None = None); temporal_interceptors() -> list
```

## Reconciliation (Recon/Obs agent)
```python
atlas.reconciliation.engine.diff_intent(desired: dict, observed: dict) -> list[DriftDifference]
async def reconcile_service(session, service: NetworkService, mode) -> ReconciliationReport  # uses adapters.get_device_state/verify
```
