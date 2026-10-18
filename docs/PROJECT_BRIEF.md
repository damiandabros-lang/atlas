Damian, przygotowałem kompletny kontekst projektowy, który możesz przekazać bezpośrednio do Claude. Projekt potraktujemy jako rzeczywiste przedsięwzięcie software engineering, a nie prosty przykład automatyzacji sieci.

Chcę, żebyśmy zbudowali system zbliżony funkcjonalnie do klasy Network Service Orchestration Platform, wykorzystujący technologie istotne dla rekrutacji do DriveNets.

Nazwa projektu: ATLAS — Network Service Orchestration Platform

Docelowo ATLAS będzie obsługiwał automatyczny provisioning usług MPLS L3VPN, zarządzanie cyklem życia usług, wykrywanie rozbieżności konfiguracji, odzyskiwanie po awariach i automatyczną walidację sieci.

Poniższy dokument możesz skopiować w całości do Claude jako początkowy kontekst projektu.

⸻

ATLAS

Enterprise Network Service Orchestration Platform

Master Project Brief — v1.0

Project Owner: Damian Dąbroś — CCIE R&S #40482
Technical Director / Lead Developer: Claude
Solution Architect / Engineering Reviewer: GPT-6
Project Type: Production-oriented Network Orchestration Platform
Primary Language: Python 3.12+
Target Environment: Docker / Kubernetes
Architecture: API-driven, distributed, workflow-based orchestration

⸻

1. PROJECT VISION

Build a production-oriented network orchestration platform capable of provisioning, modifying, validating, and decommissioning network services across distributed service-provider infrastructure.

The platform must demonstrate expertise in:

* Python backend engineering.
* Distributed systems architecture.
* Network orchestration.
* Temporal workflow orchestration.
* Kubernetes deployments.
* PostgreSQL database engineering.
* BGP/MPLS service provisioning.
* Network automation and telemetry.
* Failure recovery and transactional consistency.
* Production-grade testing and observability.

The objective is to create a technically credible orchestration system that could serve as the foundation for a commercial Network-as-a-Service platform.

The initial implementation will operate against simulated or virtual network devices.

Do not connect the platform to production infrastructure during development.

The architecture must remain extensible to real network devices through NETCONF, RESTCONF, gNMI, or vendor-specific APIs.

⸻

2. BUSINESS AND TECHNICAL OBJECTIVES

The platform must support the complete lifecycle of network services.

Primary use case

A network operator requests a new MPLS L3VPN service connecting three customer sites.

The system must:

1. Accept the service request through a REST API.
2. Validate the request against business and technical constraints.
3. Allocate required network resources.
4. Persist the desired service configuration.
5. Create a Temporal orchestration workflow.
6. Generate vendor-specific configurations.
7. Provision the required network devices.
8. Validate operational network state.
9. Verify service connectivity.
10. Update the service inventory.
11. Expose the final service status through the API.

If any stage fails, the system must execute a controlled recovery procedure.

⸻

3. HIGH-LEVEL ARCHITECTURE

                         CLIENT / OPERATOR
                                |
                                |
                         REST API REQUEST
                                |
                                v
                    +-----------------------+
                    |     FastAPI Gateway   |
                    |                       |
                    | Authentication        |
                    | Request Validation    |
                    | Service Management    |
                    +-----------+-----------+
                                |
                                v
                    +-----------------------+
                    |   Service Controller  |
                    |                       |
                    | Business Logic        |
                    | Resource Allocation   |
                    | Desired State         |
                    +-----------+-----------+
                                |
                 +--------------+--------------+
                 |                             |
                 v                             v
       +--------------------+       +----------------------+
       | PostgreSQL         |       | Temporal Server      |
       |                    |       |                      |
       | Service Inventory  |       | Workflow Execution   |
       | Desired State      |       | Retry Management     |
       | Resource Pools     |       | Failure Recovery     |
       | Audit Records      |       | Durable Execution    |
       +--------------------+       +-----------+----------+
                                               |
                                               v
                                   +-----------------------+
                                   |  Orchestration Worker |
                                   |                       |
                                   |  Python Activities    |
                                   |  Service Provisioning |
                                   |  Validation           |
                                   |  Reconciliation       |
                                   +-----------+-----------+
                                               |
                                               v
                                   +-----------------------+
                                   | Network Adapter Layer |
                                   |                       |
                                   | NETCONF               |
                                   | RESTCONF              |
                                   | gNMI                  |
                                   | Vendor APIs           |
                                   +-----------+-----------+
                                               |
                              +----------------+----------------+
                              |                |                |
                              v                v                v
                          +-------+        +-------+        +-------+
                          |  PE1  |        |  PE2  |        |  PE3  |
                          |       |        |       |        |       |
                          | BGP   |        | BGP   |        | BGP   |
                          | MPLS  |        | MPLS  |        | MPLS  |
                          +-------+        +-------+        +-------+
                              |                |                |
                              +----------------+----------------+
                                               |
                                         MPLS CORE

Architectural requirement: Temporal must be the authoritative execution engine for long-running orchestration. PostgreSQL stores business and service state, while Temporal maintains durable workflow execution state. These responsibilities must not be conflated.

⸻

4. TECHNOLOGY STACK

Layer	Technology	Purpose
Backend	Python 3.12+	Application development
API	FastAPI	REST services
Validation	Pydantic v2	Request validation
ORM	SQLAlchemy 2.x	Database access
Database	PostgreSQL	Persistent service inventory
Migrations	Alembic	Database schema management
Orchestration	Temporal Python SDK	Durable workflows
Messaging	Temporal Task Queues	Workflow/activity dispatch
Containers	Docker	Application packaging
Orchestration infrastructure	Kubernetes	Deployment and scaling
Packaging	Helm	Kubernetes deployment templates
Network interfaces	NETCONF / RESTCONF / gNMI	Device communication
Automation	Python / Ansible	Network operations
Testing	pytest	Automated tests
API testing	HTTPX	API integration tests
Metrics	Prometheus	Monitoring
Dashboards	Grafana	Visualization
Tracing	OpenTelemetry	Distributed tracing
Logging	Structured JSON	Centralized diagnostics
CI/CD	GitHub Actions	Build, test, deployment

Redis, Kafka and additional messaging infrastructure are deliberately excluded from the initial MVP unless a concrete technical requirement justifies them.

⸻

5. CORE DOMAIN MODEL

The platform must implement the following entities.

5.1 NetworkDevice

Represents a physical or virtual router.

Attributes:

class NetworkDevice:
    id: UUID
    hostname: str
    management_ip: str
    vendor: str
    platform: str
    software_version: str
    role: str
    site_id: UUID
    operational_status: str
    created_at: datetime
    updated_at: datetime

Supported roles:

* PE — Provider Edge
* P — Provider Core
* RR — Route Reflector

Initial vendors:

* Cisco IOS XR
* Juniper Junos
* Mock Network Device

The mock adapter must be implemented first.

⸻

5.2 NetworkService

Represents a customer network service.

class NetworkService:
    id: UUID
    customer_id: UUID
    service_type: str
    service_name: str
    desired_state: dict
    observed_state: dict
    lifecycle_status: str
    version: int
    created_at: datetime
    updated_at: datetime

Supported service types:

MPLS_L3VPN

Future service types:

EVPN
L2VPN
INTERNET_ACCESS
BGP_PEERING
VRF

⸻

5.3 ServiceEndpoint

Represents a customer attachment to a PE router.

class ServiceEndpoint:
    id: UUID
    service_id: UUID
    device_id: UUID
    interface: str
    vlan_id: int | None
    vrf_name: str
    ce_ip_address: str
    pe_ip_address: str
    routing_protocol: str

Each endpoint must support independent validation and provisioning status.

⸻

5.4 ResourceAllocation

Tracks network resources allocated to services.

class ResourceAllocation:
    id: UUID
    service_id: UUID
    resource_type: str
    resource_value: str
    allocation_status: str
    created_at: datetime

Resources include:

* Route Distinguishers.
* Route Targets.
* VLAN IDs.
* VRF names.
* IP subnets.

Resource allocation must be atomic and concurrency-safe.

⸻

6. PRIMARY WORKFLOW — MPLS L3VPN PROVISIONING

The initial implementation must support an MPLS L3VPN connecting three PE routers.

Example:

Customer: ACME Corporation
Service: ACME-L3VPN-001
PE1:
  VRF: ACME-PROD
  RD: 65000:1001
  RT Import: 65000:100
  RT Export: 65000:100
PE2:
  VRF: ACME-PROD
  RD: 65000:1002
  RT Import: 65000:100
  RT Export: 65000:100
PE3:
  VRF: ACME-PROD
  RD: 65000:1003
  RT Import: 65000:100
  RT Export: 65000:100

The orchestration workflow:

START
  |
  v
Validate Request
  |
  v
Check Device Inventory
  |
  v
Allocate Resources
  |
  v
Persist Desired State
  |
  v
Generate Configuration
  |
  v
Pre-Deployment Validation
  |
  v
Provision PE1
  |
  v
Provision PE2
  |
  v
Provision PE3
  |
  v
Validate BGP / VRF / Interfaces
  |
  v
Validate Service Connectivity
  |
  v
Commit Service State
  |
  v
COMPLETE

Important: The sequence above describes logical stages. The implementation must not assume that network configuration across three routers is one atomic transaction.

Each device operation requires explicit tracking, verification and recovery semantics.

⸻

7. TEMPORAL WORKFLOW ENGINE

Temporal is the most important new software engineering component of the project.

Implement a workflow named:

ProvisionL3VPNWorkflow

Conceptual structure:

@workflow.defn
class ProvisionL3VPNWorkflow:
    @workflow.run
    async def run(self, request: ProvisionRequest):
        await workflow.execute_activity(
            validate_request,
            request,
            start_to_close_timeout=timedelta(seconds=30)
        )
        resources = await workflow.execute_activity(
            allocate_resources,
            request,
            start_to_close_timeout=timedelta(seconds=30)
        )
        configs = await workflow.execute_activity(
            generate_configs,
            resources,
            start_to_close_timeout=timedelta(seconds=60)
        )
        for config in configs:
            await workflow.execute_activity(
                provision_device,
                config,
                start_to_close_timeout=timedelta(minutes=5)
            )
        await workflow.execute_activity(
            validate_service,
            request.service_id,
            start_to_close_timeout=timedelta(minutes=2)
        )

This is illustrative pseudocode, not a complete production implementation.

Claude must implement the actual workflow using current Temporal Python SDK conventions.

Mandatory engineering requirements

The implementation must support:

Durable execution

Workflow progress must survive worker restarts.

Idempotency

Repeated activity execution must not duplicate resources or corrupt device configuration.

Retry policies

Transient failures must trigger controlled retries with exponential backoff.

Activity timeouts

Each activity must have explicitly defined execution limits.

Cancellation

Operators must be able to cancel an orchestration request safely.

Compensation

Failed multi-device operations must trigger appropriate compensating actions.

Workflow versioning

Running workflows must remain compatible with supported application upgrades.

Determinism

Workflow code must comply with Temporal replay requirements.

External I/O must be performed through activities, not directly inside workflow logic.

⸻

8. FAILURE RECOVERY AND COMPENSATION

This is a critical evaluation area.

Consider the following scenario:

PE1 provisioning: SUCCESS
PE2 provisioning: SUCCESS
PE3 provisioning: FAILED

The system must not blindly assume that deleting all configurations is safe.

A controlled recovery strategy must:

1. Identify which device operations completed successfully.
2. Determine whether the failure is transient or permanent.
3. Retry only operations that can be safely repeated.
4. Validate actual device state.
5. Execute compensating actions when appropriate.
6. Preserve an audit trail.
7. Mark the service as failed or requiring intervention if recovery cannot be completed safely.

Use a Saga-style compensation model.

Compensating actions must be idempotent and must not remove pre-existing configuration owned by another service.

The system must distinguish between:

REQUESTED
VALIDATING
ALLOCATING
PROVISIONING
VERIFYING
ACTIVE
FAILED
ROLLING_BACK
ROLLED_BACK
MANUAL_INTERVENTION_REQUIRED
DECOMMISSIONING
DECOMMISSIONED

⸻

9. NETWORK ADAPTER ARCHITECTURE

Network-device communication must be abstracted behind a common interface.

from typing import Protocol
class NetworkAdapter(Protocol):
    async def get_device_state(self, device_id: str):
        ...
    async def validate_configuration(self, config: dict):
        ...
    async def apply_configuration(self, config: dict):
        ...
    async def verify_configuration(self, config: dict):
        ...
    async def rollback_configuration(self, operation_id: str):
        ...

Implement:

MockNetworkAdapter
CiscoIOSXRAdapter
JuniperJunosAdapter

Only MockNetworkAdapter is mandatory for the initial MVP.

The mock must support configurable failure injection.

Examples:

FAIL_ON_PE3
TIMEOUT_ON_PE2
BGP_SESSION_DOWN
DEVICE_UNREACHABLE
PARTIAL_CONFIGURATION

The mock must maintain simulated device state rather than simply returning success.

⸻

10. REST API DESIGN

Implement the following endpoints.

Service creation

POST /api/v1/services/l3vpn

Example request:

{
  "customer_id": "550e8400-e29b-41d4-a716-446655440000",
  "service_name": "ACME-L3VPN-001",
  "vrf_name": "ACME-PROD",
  "route_target": "65000:100",
  "endpoints": [
    {
      "device": "PE1",
      "interface": "GigabitEthernet0/0/0/1",
      "vlan": 100
    },
    {
      "device": "PE2",
      "interface": "GigabitEthernet0/0/0/2",
      "vlan": 100
    },
    {
      "device": "PE3",
      "interface": "GigabitEthernet0/0/0/3",
      "vlan": 100
    }
  ]
}

Response:

{
  "service_id": "service-uuid",
  "workflow_id": "workflow-uuid",
  "status": "REQUESTED"
}

Return HTTP 202 Accepted because provisioning is asynchronous.

Additional endpoints

GET    /api/v1/services
GET    /api/v1/services/{service_id}
GET    /api/v1/services/{service_id}/status
GET    /api/v1/services/{service_id}/history
POST   /api/v1/services/{service_id}/validate
POST   /api/v1/services/{service_id}/reconcile
POST   /api/v1/services/{service_id}/cancel
DELETE /api/v1/services/{service_id}
GET    /api/v1/devices
GET    /api/v1/devices/{device_id}
GET    /api/v1/health
GET    /api/v1/metrics

All mutating endpoints must enforce authorization and idempotency requirements.

⸻

11. DATABASE DESIGN

Use PostgreSQL with SQLAlchemy 2.x and Alembic.

Required tables:

customers
network_devices
network_services
service_endpoints
resource_allocations
workflow_executions
device_operations
audit_events

Key requirements

Referential integrity

Use foreign keys and appropriate constraints.

Concurrency control

Prevent concurrent workflows from allocating identical resources.

Optimistic locking

Use version fields for service updates where appropriate.

Transactions

Business-state transitions and resource allocation must be transactionally consistent.

Auditability

Record significant service lifecycle events.

Indexing

Index frequently queried fields such as service ID, customer ID, workflow ID and lifecycle status.

Secrets

Never store plaintext device credentials in the database.

Use Kubernetes Secrets for local development and a dedicated secrets manager for production-oriented deployments.

⸻

12. RECONCILIATION ENGINE

The system must compare desired state with observed network state.

Example:

Desired State:
PE1:
  VRF ACME-PROD
  RT 65000:100
Observed State:
PE1:
  VRF ACME-PROD
  RT 65000:200

The reconciliation engine must detect the mismatch.

Expected output:

{
  "service_id": "service-uuid",
  "device": "PE1",
  "status": "DRIFT_DETECTED",
  "differences": [
    {
      "field": "route_target",
      "desired": "65000:100",
      "observed": "65000:200"
    }
  ]
}

Automatic remediation must be configurable.

For the initial MVP, use:

DETECT_ONLY

Do not automatically modify device configurations without explicit authorization.

⸻

13. OBSERVABILITY

Implement three observability pillars.

Metrics

Prometheus metrics:

atlas_workflow_total
atlas_workflow_failures_total
atlas_workflow_duration_seconds
atlas_device_operations_total
atlas_device_operation_failures_total
atlas_reconciliation_drift_total

Use bounded labels to avoid excessive metric cardinality.

Logging

Structured JSON logs containing:

timestamp
level
service_id
workflow_id
activity
device_id
correlation_id
message

Sensitive information must be redacted.

Tracing

Use OpenTelemetry for distributed tracing across:

FastAPI