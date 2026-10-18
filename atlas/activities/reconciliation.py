"""Reconciliation activity — desired vs observed state via ``atlas.reconciliation.engine``."""

from __future__ import annotations

from temporalio import activity

from atlas.activities._support import get_logger, map_domain_error, non_retryable, record_drift
from atlas.core.enums import AuditEventType, DriftStatus, ReconciliationMode
from atlas.db.repositories import AuditRepository, ServiceRepository
from atlas.db.session import transaction
from atlas.reconciliation.engine import reconcile_service
from atlas.schemas import ReconcileRequest, ReconciliationReport


@activity.defn(name="reconcile_service_activity")
async def reconcile_service_activity(req: ReconcileRequest) -> ReconciliationReport:
    log = get_logger(service_id=req.service_id, correlation_id=req.correlation_id)
    if req.mode == ReconciliationMode.AUTO_REMEDIATE:
        raise non_retryable(
            "AUTO_REMEDIATE requires explicit authorisation (MVP is DETECT_ONLY)", error_type="NotAuthorized"
        )
    try:
        async with transaction() as session:
            service = await ServiceRepository(session).get(req.service_id)
            if service is None:
                raise LookupError(f"service {req.service_id} not found")
            report: ReconciliationReport = await reconcile_service(session, service, req.mode)
            drifted = [d for d in report.devices if d.status == DriftStatus.DRIFT_DETECTED]
            wf_id = activity.info().workflow_id if activity.in_activity() else None
            audit = AuditRepository(session)
            await audit.record(
                AuditEventType.RECONCILIATION_RUN,
                service_id=req.service_id,
                workflow_id=wf_id,
                correlation_id=req.correlation_id,
                message=f"reconciliation ({req.mode}) in_sync={report.in_sync}",
                details={"devices": [d.model_dump(mode="json") for d in report.devices]},
            )
            for drift in drifted:
                await audit.record(
                    AuditEventType.DRIFT_DETECTED,
                    service_id=req.service_id,
                    workflow_id=wf_id,
                    correlation_id=req.correlation_id,
                    message=f"drift on {drift.device}: {len(drift.differences)} differences",
                    details={"differences": [x.model_dump(mode="json") for x in drift.differences]},
                )
    except Exception as exc:
        raise map_domain_error(exc) from exc
    for d in report.devices:
        record_drift(str(d.status))
    log.info("reconciliation_done", in_sync=report.in_sync, drifted=[d.device for d in drifted])
    return report


__all__ = ["reconcile_service_activity"]
