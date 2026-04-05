"use client";

import type { StatusResponse, SourceHealth } from "../lib/types";
import { formatDateFull } from "../lib/format";
import { useLocale } from "../lib/i18n";

function healthColor(status: string): string {
  if (status === "healthy") return "green";
  if (status === "degraded") return "amber";
  return "red";
}

function SourceCard({ source }: { source: SourceHealth }) {
  const color = healthColor(source.status);
  return (
    <div className="source-item">
      <span className={`status-dot status-dot-${color} ${source.status === "healthy" ? "" : "status-dot-pulse"}`} />
      <div className="source-info">
        <div className="source-name">{source.name}</div>
        <div className="source-detail">
          {source.status}
          {source.freshness_hours != null && ` · ${source.freshness_hours.toFixed(1)}h ago`}
          {source.null_rate != null && ` · null ${(source.null_rate * 100).toFixed(1)}%`}
        </div>
      </div>
    </div>
  );
}

export default function StatusPanel({ data }: { data: StatusResponse }) {
  const { t } = useLocale();

  const servingColor = data.serving_mode === "promoted" ? "green" : data.serving_mode === "heuristic-fallback" ? "amber" : "blue";

  return (
    <div className="card section">
      <div className="card-header">
        <div className="card-title">
          <span className="card-title-icon">🔧</span> {t("systemStatus")}
        </div>
        <span className={`badge badge-${servingColor}`}>
          <span className={`status-dot status-dot-${servingColor}`} />
          {data.serving_mode}
        </span>
      </div>
      <div className="card-body">
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 14, marginBottom: 20 }}>
          <div className="kpi-card" style={{ padding: "14px 16px" }}>
            <div className="kpi-label">{t("model")}</div>
            <div style={{ fontFamily: "var(--font-mono)", fontWeight: 600, fontSize: "0.9rem", color: "var(--text-primary)" }}>
              {data.latest_model.model_type}
            </div>
            {data.latest_model.version && (
              <div className="kpi-sub">{data.latest_model.version}</div>
            )}
            {data.latest_model.promoted_at && (
              <div className="kpi-sub">{t("promoted")} {formatDateFull(data.latest_model.promoted_at)}</div>
            )}
          </div>

          {data.latest_ingestion && (
            <div className="kpi-card" style={{ padding: "14px 16px" }}>
              <div className="kpi-label">{t("latestIngestion")}</div>
              {Object.entries(data.latest_ingestion).slice(0, 3).map(([k, v]) => (
                <div key={k} className="kpi-sub">{k}: {String(v)}</div>
              ))}
            </div>
          )}

          {data.latest_forecast && (
            <div className="kpi-card" style={{ padding: "14px 16px" }}>
              <div className="kpi-label">{t("latestForecast")}</div>
              {Object.entries(data.latest_forecast).slice(0, 3).map(([k, v]) => (
                <div key={k} className="kpi-sub">{k}: {String(v)}</div>
              ))}
            </div>
          )}

          {data.latest_training && (
            <div className="kpi-card" style={{ padding: "14px 16px" }}>
              <div className="kpi-label">{t("latestTraining")}</div>
              {Object.entries(data.latest_training).slice(0, 3).map(([k, v]) => (
                <div key={k} className="kpi-sub">{k}: {String(v)}</div>
              ))}
            </div>
          )}
        </div>

        <div style={{ fontSize: "0.72rem", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--text-muted)", marginBottom: 12 }}>
          {t("sourceHealth")}
        </div>
        <div className="source-grid">
          {data.source_health.map((s) => (
            <SourceCard key={s.name} source={s} />
          ))}
        </div>
      </div>
    </div>
  );
}
