"use client";

import dynamic from "next/dynamic";
import type { PerformanceResponse } from "../lib/types";
import { formatPrice, formatPercent } from "../lib/format";
import { useLocale } from "../lib/i18n";
import { useThemeTokens } from "../lib/theme";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

function CalibrationBar({ calibration }: { calibration: PerformanceResponse["calibration"] }) {
  const { t } = useLocale();

  const below = calibration.below_p10 * 100;
  const within = calibration.within_band * 100;
  const above = calibration.above_p90 * 100;

  return (
    <div>
      <div style={{ fontSize: "0.72rem", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--text-muted)", marginBottom: 8 }}>
        {t("calibrationCoverage")}
      </div>
      <div className="calibration-bar">
        <div className="calibration-segment" style={{ width: `${below}%`, background: "var(--accent-red-dim)", color: "var(--accent-red)" }}>
          {below > 8 ? `${below.toFixed(1)}%` : ""}
        </div>
        <div className="calibration-segment" style={{ width: `${within}%`, background: "var(--accent-blue-dim)", color: "var(--accent-blue)" }}>
          {within.toFixed(1)}%
        </div>
        <div className="calibration-segment" style={{ width: `${above}%`, background: "var(--accent-amber-dim)", color: "var(--accent-amber)" }}>
          {above > 8 ? `${above.toFixed(1)}%` : ""}
        </div>
      </div>
      <div className="calibration-legend">
        <div className="calibration-legend-item">
          <div className="calibration-legend-dot" style={{ background: "var(--accent-red)" }} />
          {t("belowP10")}
        </div>
        <div className="calibration-legend-item">
          <div className="calibration-legend-dot" style={{ background: "var(--accent-blue)" }} />
          {t("withinBand")}
        </div>
        <div className="calibration-legend-item">
          <div className="calibration-legend-dot" style={{ background: "var(--accent-amber)" }} />
          {t("aboveP90")}
        </div>
      </div>
    </div>
  );
}

export default function PerformancePanel({ data }: { data: PerformanceResponse }) {
  const { t } = useLocale();
  const theme = useThemeTokens();

  const actualLabel = t("actual");
  const predictedLabel = t("predicted");

  const backtestOption = data.latest_backtest_curve.length > 0 ? {
    backgroundColor: "transparent",
    tooltip: {
      trigger: "axis" as const,
      backgroundColor: theme.tooltipBg,
      borderColor: theme.tooltipBorder,
      borderWidth: 1,
      textStyle: { color: theme.tooltipText, fontFamily: theme.fontSans, fontSize: 12 },
    },
    legend: {
      data: [actualLabel, predictedLabel],
      top: 0,
      right: 0,
      textStyle: { color: theme.legendText, fontSize: 11, fontFamily: theme.fontSans },
      itemWidth: 14,
      itemHeight: 3,
    },
    grid: { top: 30, right: 16, bottom: 28, left: 50 },
    xAxis: {
      type: "category" as const,
      data: data.latest_backtest_curve.map((p) => {
        const d = new Date(p.timestamp);
        return `${d.getHours()}:00`;
      }),
      axisLine: { lineStyle: { color: theme.axisLine } },
      axisLabel: { color: theme.axisLabel, fontSize: 10, fontFamily: theme.fontMono },
      axisTick: { show: false },
    },
    yAxis: {
      type: "value" as const,
      axisLine: { show: false },
      axisLabel: { color: theme.axisLabel, fontSize: 10, fontFamily: theme.fontMono },
      splitLine: { lineStyle: { color: theme.splitLine, type: "dashed" as const } },
    },
    series: [
      {
        name: actualLabel,
        type: "line",
        data: data.latest_backtest_curve.map((p) => p.actual),
        lineStyle: { color: theme.accentGreen, width: 2 },
        itemStyle: { color: theme.accentGreen },
        symbol: "none",
        smooth: 0.1,
      },
      {
        name: predictedLabel,
        type: "line",
        data: data.latest_backtest_curve.map((p) => p.predicted),
        lineStyle: { color: theme.accentBlue, width: 2, type: "dashed" as const },
        itemStyle: { color: theme.accentBlue },
        symbol: "none",
        smooth: 0.1,
      },
    ],
  } : null;

  return (
    <div className="card">
      <div className="card-header">
        <div className="card-title">
          <span className="card-title-icon">🧪</span> {t("modelPerformance")}
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <span className="badge badge-blue">{data.champion_model.model_type}</span>
          {data.champion_model.version && (
            <span className="badge badge-green">{data.champion_model.version}</span>
          )}
        </div>
      </div>
      <div className="card-body">
        <div className="table-scroll" style={{ marginBottom: 20 }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>{t("model")}</th>
                <th>{t("slice")}</th>
                <th>MAE</th>
                <th>RMSE</th>
                <th>sMAPE</th>
                <th>{t("coverage")}</th>
              </tr>
            </thead>
            <tbody>
              {data.benchmarks.map((b, i) => (
                <tr key={i}>
                  <td className="cell-name">{b.name}</td>
                  <td>{b.slice_name}</td>
                  <td className="cell-highlight">{formatPrice(b.mae)}</td>
                  <td>{formatPrice(b.rmse)}</td>
                  <td>{formatPercent(b.smape)}</td>
                  <td>{b.coverage_p10_p90 != null ? formatPercent(b.coverage_p10_p90) : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <CalibrationBar calibration={data.calibration} />

        {backtestOption && (
          <div style={{ marginTop: 20 }}>
            <div style={{ fontSize: "0.72rem", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--text-muted)", marginBottom: 8 }}>
              {t("latestBacktest")}
            </div>
            <div style={{ height: 200 }}>
              <ReactECharts
                key={theme.mode}
                option={backtestOption}
                style={{ height: "100%", width: "100%" }}
                opts={{ renderer: "canvas" }}
              />
            </div>
          </div>
        )}

        <div style={{ marginTop: 16, fontSize: "0.75rem", color: "var(--text-muted)" }}>
          {t("lastPromotion")}: <span style={{ color: "var(--text-secondary)" }}>{data.last_promotion_decision}</span>
        </div>
      </div>
    </div>
  );
}
