"""Generate the three Grafana dashboards in monitoring/ from one definition.

The JSON files are committed (Grafana provisions them from monitoring/), so this
only needs re-running after editing a panel here:

    python scripts/build_dashboards.py
"""

import json
from pathlib import Path
from typing import Any

OUT_DIR = Path(__file__).resolve().parent.parent / "monitoring"

PROM = {"type": "prometheus", "uid": "prometheus"}
SQL = {"type": "grafana-postgresql-datasource", "uid": "syncdb"}

Panel = dict[str, Any]


def _q(expr: str, legend: str = "", ref: str = "A", **extra: Any) -> dict[str, Any]:
    """A Prometheus query target."""
    return {"datasource": PROM, "expr": expr, "legendFormat": legend, "refId": ref, **extra}


def quantiles(metric: str, selector: str = "", window: str = "1m") -> list[dict[str, Any]]:
    """p50/p95/p99 targets for a histogram."""
    inner = f"sum by (le) (rate({metric}_bucket{{{selector}}}[{window}]))"
    return [
        _q(f"histogram_quantile({q}, {inner})", f"p{int(q * 100)}", ref)
        for q, ref in ((0.5, "A"), (0.95, "B"), (0.99, "C"))
    ]


def stat(title: str, expr: str, x: int, y: int, unit: str = "short", **opts: Any) -> Panel:
    """Single-number panel."""
    return {
        "type": "stat",
        "title": title,
        "datasource": PROM,
        "gridPos": {"h": 5, "w": 6, "x": x, "y": y},
        "targets": [_q(expr)],
        "fieldConfig": {"defaults": {"unit": unit, **opts}, "overrides": []},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "value",
            "graphMode": "area",
            "textMode": "auto",
        },
    }


def timeseries(
    title: str,
    targets: list[dict[str, Any]],
    x: int,
    y: int,
    w: int = 12,
    h: int = 8,
    unit: str = "short",
    **extra: Any,
) -> Panel:
    """Line-chart panel."""
    return {
        "type": "timeseries",
        "title": title,
        "datasource": PROM,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": targets,
        "fieldConfig": {
            "defaults": {"unit": unit, "custom": {"fillOpacity": 10, "lineWidth": 2}},
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "table", "placement": "bottom", "calcs": ["mean", "max"]},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
        **extra,
    }


def dashboard(uid: str, title: str, description: str, panels: list[Panel]) -> dict[str, Any]:
    """Wrap panels in a dashboard; panel ids are assigned in order."""
    for panel_id, panel in enumerate(panels, start=1):
        panel["id"] = panel_id
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "tags": ["sync-platform"],
        "schemaVersion": 41,
        "version": 1,
        "editable": True,
        "time": {"from": "now-1h", "to": "now"},
        "refresh": "10s",
        "timezone": "browser",
        "templating": {"list": []},
        "annotations": {"list": []},
        "links": [
            {
                "title": "Sync Platform",
                "type": "dashboards",
                "tags": ["sync-platform"],
                "asDropdown": False,
                "includeVars": False,
                "keepTime": True,
            }
        ],
        "panels": panels,
    }


def overview() -> dict[str, Any]:
    """Dashboard 1: is it working, how much, how well."""
    processed = 'sync_events_total{status=~"synced|failed"}'
    return dashboard(
        "sync-overview",
        "Sync Platform - System Overview",
        "Volume, success rate, processor state and error trend.",
        [
            stat(
                "Syncs accepted (time range)",
                'sum(increase(sync_events_total{status="accepted"}[$__range]))',
                0,
                0,
                decimals=0,
            ),
            stat(
                "Success rate (time range)",
                f'sum(increase(sync_events_total{{status="synced"}}[$__range])) '
                f"/ sum(increase({processed}[$__range]))",
                6,
                0,
                unit="percentunit",
                decimals=1,
                min=0,
                max=1,
                thresholds={
                    "mode": "absolute",
                    "steps": [
                        {"color": "red", "value": None},
                        {"color": "orange", "value": 0.9},
                        {"color": "green", "value": 0.99},
                    ],
                },
            ),
            stat("Processor concurrency (now)", "sum(processor_concurrent_syncs)", 12, 0),
            stat(
                "Processor running",
                "max(processor_running)",
                18,
                0,
                mappings=[
                    {
                        "type": "value",
                        "options": {
                            "0": {"text": "DOWN", "color": "red"},
                            "1": {"text": "UP", "color": "green"},
                        },
                    }
                ],
            ),
            timeseries(
                "Throughput (events/s)",
                [
                    _q('sum(rate(sync_events_total{status="accepted"}[1m]))', "accepted (API)"),
                    _q(f"sum(rate({processed}[1m]))", "processed (worker)", "B"),
                ],
                0,
                5,
                unit="reqps",
            ),
            timeseries(
                "Processor concurrency & batch size",
                [
                    _q("sum(processor_concurrent_syncs)", "concurrent syncs"),
                    _q("sum(processor_batch_size)", "batch size", "B"),
                ],
                12,
                5,
            ),
            timeseries(
                "Error rate (last 24h)",
                [
                    _q(
                        f'sum(rate(sync_events_total{{status="failed"}}[5m])) '
                        f"/ sum(rate({processed}[5m]))",
                        "failed / processed",
                    )
                ],
                0,
                13,
                w=12,
                unit="percentunit",
                timeFrom="24h",
            ),
            timeseries(
                "Errors by type (last 24h, per second)",
                [_q("sum by (type) (rate(errors_total[5m]))", "{{type}}")],
                12,
                13,
                unit="short",
                timeFrom="24h",
            ),
        ],
    )


def performance() -> dict[str, Any]:
    """Dashboard 2: latency percentiles at each stage, and throughput."""
    return dashboard(
        "sync-performance",
        "Sync Platform - Performance",
        "Latency percentiles per stage (API, Kafka, processor, end to end) and throughput.",
        [
            timeseries(
                "API latency - POST /api/v1/sync",
                quantiles("sync_api_latency_seconds", 'status="accepted"'),
                0,
                0,
                unit="s",
            ),
            timeseries(
                "Processor latency - Kafka consume -> DB update",
                quantiles("sync_processing_latency_seconds", 'outcome=~"synced|failed"'),
                12,
                0,
                unit="s",
            ),
            {
                "type": "heatmap",
                "title": "Kafka publish latency distribution",
                "datasource": PROM,
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
                "targets": [
                    _q(
                        "sum by (le) (increase(sync_kafka_publish_latency_seconds_bucket"
                        '{result="ok"}[1m]))',
                        "{{le}}",
                        format="heatmap",
                    )
                ],
                "fieldConfig": {"defaults": {}, "overrides": []},
                "options": {
                    "calculate": False,
                    "yAxis": {"unit": "s"},
                    "color": {"scheme": "Oranges", "mode": "scheme"},
                    "cellGap": 1,
                },
            },
            timeseries(
                "Kafka publish latency",
                quantiles("sync_kafka_publish_latency_seconds", 'result="ok"'),
                12,
                8,
                unit="s",
            ),
            timeseries(
                "End-to-end latency - row created -> processed",
                quantiles("sync_latency_seconds"),
                0,
                16,
                unit="s",
            ),
            timeseries(
                "Transform + validate latency p95 by source system",
                [
                    _q(
                        "histogram_quantile(0.95, sum by (le, source_system) "
                        "(rate(sync_transform_latency_seconds_bucket[1m])))",
                        "{{source_system}}",
                    )
                ],
                12,
                16,
                unit="s",
            ),
            timeseries(
                "Throughput (events/s)",
                [
                    _q('sum(rate(sync_events_total{status="accepted"}[1m]))', "accepted (API)"),
                    _q(
                        'sum(rate(sync_events_total{status=~"synced|failed"}[1m]))',
                        "processed (worker)",
                        "B",
                    ),
                    _q(
                        'sum(rate(sync_api_latency_seconds_count{status="replayed"}[1m]))',
                        "idempotent replays",
                        "C",
                    ),
                ],
                0,
                24,
                w=24,
                unit="reqps",
            ),
        ],
    )


def errors() -> dict[str, Any]:
    """Dashboard 3: what is failing, where, and why."""
    top_errors_sql = (
        "SELECT left(error_message, 200) AS error_message,\n"
        "       count(*) AS failures,\n"
        "       max(processed_at) AS last_seen\n"
        "FROM sync_events\n"
        "WHERE status = 'failed' AND $__timeFilter(processed_at)\n"
        "GROUP BY 1\nORDER BY failures DESC\nLIMIT 10"
    )
    recent_sql = (
        "SELECT processed_at AS time, id AS sync_id, entity_id, source_system,\n"
        "       destination_system, left(error_message, 160) AS error_message\n"
        "FROM sync_events\n"
        "WHERE status = 'failed' AND $__timeFilter(processed_at)\n"
        "ORDER BY processed_at DESC\nLIMIT 50"
    )

    def table(title: str, sql: str, y: int) -> Panel:
        return {
            "type": "table",
            "title": title,
            "datasource": SQL,
            "gridPos": {"h": 9, "w": 24, "x": 0, "y": y},
            "targets": [
                {
                    "datasource": SQL,
                    "refId": "A",
                    "format": "table",
                    "rawQuery": True,
                    "editorMode": "code",
                    "rawSql": sql,
                }
            ],
            "fieldConfig": {"defaults": {}, "overrides": []},
            "options": {"showHeader": True, "cellHeight": "sm"},
        }

    return dashboard(
        "sync-errors",
        "Sync Platform - Errors",
        "Errors by type and source system (Prometheus) and failure reasons (Postgres).",
        [
            timeseries(
                "Error rate by type (per second)",
                [_q("sum by (type) (rate(errors_total[5m]))", "{{type}}")],
                0,
                0,
            ),
            {
                "type": "bargauge",
                "title": "Errors by type (time range)",
                "datasource": PROM,
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
                "targets": [
                    _q(
                        "sum by (type) (increase(errors_total[$__range]))",
                        "{{type}}",
                        instant=True,
                    )
                ],
                "fieldConfig": {"defaults": {"decimals": 0}, "overrides": []},
                "options": {"orientation": "horizontal", "displayMode": "gradient"},
            },
            timeseries(
                "Failed syncs by source system (per second)",
                [
                    _q(
                        'sum by (source_system) (rate(sync_events_total{status="failed"}[5m]))',
                        "{{source_system}}",
                    )
                ],
                0,
                8,
            ),
            {
                "type": "bargauge",
                "title": "Failed syncs by source system (time range)",
                "datasource": PROM,
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
                "targets": [
                    _q(
                        "sum by (source_system) "
                        '(increase(sync_events_total{status="failed"}[$__range]))',
                        "{{source_system}}",
                        instant=True,
                    )
                ],
                "fieldConfig": {"defaults": {"decimals": 0}, "overrides": []},
                "options": {"orientation": "horizontal", "displayMode": "gradient"},
            },
            table("Top 10 error messages (from sync_events)", top_errors_sql, 16),
            table("Recent failed syncs", recent_sql, 25),
        ],
    )


def main() -> None:
    """Write all three dashboard files."""
    for name, build in (
        ("overview", overview),
        ("performance", performance),
        ("errors", errors),
    ):
        path = OUT_DIR / f"grafana_dashboard_{name}.json"
        path.write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8")
        print(f"wrote {path.relative_to(OUT_DIR.parent)}")


if __name__ == "__main__":
    main()
