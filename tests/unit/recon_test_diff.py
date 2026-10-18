"""Unit tests for ``atlas.reconciliation.engine.diff_intent``."""

from __future__ import annotations

from uuid import uuid4

from atlas.core.enums import DriftStatus, ReconciliationMode
from atlas.reconciliation.engine import build_report, diff_intent, summarize_field
from atlas.schemas import DeviceDrift


def _intent(rt: str = "65000:100", **overrides) -> dict:
    base = {
        "vrf": {"name": "ACME-PROD", "rd": "65000:1001", "rt_import": [rt], "rt_export": [rt]},
        "interface": {
            "name": "GigabitEthernet0/0/0/1",
            "vlan": 100,
            "ip": "10.255.0.1",
            "prefix_length": 30,
            "vrf": "ACME-PROD",
            "admin_up": True,
        },
        "bgp": {"asn": 65000, "vrf": "ACME-PROD", "neighbor": "10.255.0.2", "remote_asn": 65001},
    }
    for key, value in overrides.items():
        section, _, leaf = key.partition("__")
        base[section][leaf] = value
    return base


def test_in_sync_yields_no_differences() -> None:
    assert diff_intent(_intent(), _intent()) == []


def test_route_target_drift_collapses_to_single_field_matching_brief_shape() -> None:
    service_id = uuid4()
    diffs = diff_intent(_intent("65000:100"), _intent("65000:200"))
    drift = DeviceDrift(
        service_id=service_id, device="PE1", status=DriftStatus.DRIFT_DETECTED, differences=diffs
    )
    assert drift.model_dump(mode="json") == {
        "service_id": str(service_id),
        "device": "PE1",
        "status": "DRIFT_DETECTED",
        "differences": [{"field": "route_target", "desired": "65000:100", "observed": "65000:200"}],
    }


def test_import_only_drift_is_reported_separately() -> None:
    observed = _intent()
    observed["vrf"]["rt_import"] = ["65000:300"]
    diffs = diff_intent(_intent(), observed)
    assert [d.field for d in diffs] == ["route_target_import"]
    assert diffs[0].desired == "65000:100"
    assert diffs[0].observed == "65000:300"


def test_route_target_lists_are_order_insensitive() -> None:
    desired = _intent()
    desired["vrf"]["rt_import"] = ["65000:100", "65000:101"]
    desired["vrf"]["rt_export"] = ["65000:101", "65000:100"]
    observed = _intent()
    observed["vrf"]["rt_import"] = ["65000:101", "65000:100"]
    observed["vrf"]["rt_export"] = ["65000:100", "65000:101"]
    assert diff_intent(desired, observed) == []


def test_missing_interface_section_reports_observed_none() -> None:
    observed = _intent()
    del observed["interface"]
    diffs = {d.field: d for d in diff_intent(_intent(), observed)}
    assert "pe_ip_address" in diffs
    assert diffs["pe_ip_address"].desired == "10.255.0.1"
    assert diffs["pe_ip_address"].observed is None
    assert diffs["vlan_id"].observed is None
    assert diffs["interface.admin_up"].observed is None


def test_scalar_mismatch_uses_dotted_path_and_summary_mapping() -> None:
    diffs = diff_intent(_intent(), _intent(vrf__rd="65000:9999", interface__vlan=200))
    by_field = {d.field: (d.desired, d.observed) for d in diffs}
    assert by_field == {"route_distinguisher": ("65000:1001", "65000:9999"), "vlan_id": (100, 200)}


def test_extra_observed_keys_are_not_drift() -> None:
    observed = _intent()
    observed["operational"] = {"bgp_state": "Established"}
    observed["vrf"]["description"] = "managed by atlas"
    assert diff_intent(_intent(), observed) == []


def test_empty_observed_reports_every_desired_leaf_missing() -> None:
    diffs = diff_intent(_intent(), {})
    assert all(d.observed is None for d in diffs)
    assert {"route_target", "route_distinguisher", "pe_ip_address", "vlan_id"} <= {d.field for d in diffs}


def test_summarize_field_mapping() -> None:
    assert summarize_field("vrf.rt_import") == "route_target_import"
    assert summarize_field("vrf.rt_export") == "route_target_export"
    assert summarize_field("vrf.rd") == "route_distinguisher"
    assert summarize_field("interface.ip") == "pe_ip_address"
    assert summarize_field("interface.prefix_length") == "interface.prefix_length"


def test_build_report_in_sync_flag() -> None:
    sid = uuid4()
    ok = DeviceDrift(service_id=sid, device="PE1", status=DriftStatus.IN_SYNC)
    bad = DeviceDrift(service_id=sid, device="PE2", status=DriftStatus.NOT_PROVISIONED)
    assert build_report(sid, ReconciliationMode.DETECT_ONLY, [ok]).in_sync is True
    report = build_report(sid, ReconciliationMode.DETECT_ONLY, [ok, bad])
    assert report.in_sync is False
    assert report.mode == ReconciliationMode.DETECT_ONLY
    assert len(report.devices) == 2
