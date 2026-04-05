"use client";

import dynamic from "next/dynamic";
import type { DayAheadResponse } from "../lib/types";
import { formatPrice, formatHour } from "../lib/format";
import { useLocale } from "../lib/i18n";
import { useThemeTokens } from "../lib/theme";
import InfoButton from "./InfoButton";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

export default function DayAheadChart({ data }: { data: DayAheadResponse }) {
  const { t } = useLocale();
  const theme = useThemeTokens();

  const hours = data.forecast.map((_, i) => formatHour(i));
  const p10 = data.forecast.map((f) => f.p10);
  const p50 = data.forecast.map((f) => f.p50);
  const p90 = data.forecast.map((f) => f.p90);

  const bestSet = new Set(data.best_hours);
  const worstSet = new Set(data.worst_hours);

  const bestScatter = data.forecast
    .map((f, i) => (bestSet.has(i) ? [i, f.p50] : null))
    .filter(Boolean);
  const worstScatter = data.forecast
    .map((f, i) => (worstSet.has(i) ? [i, f.p50] : null))
    .filter(Boolean);

  const expectedLabel = t("expectedPrice");
  const bestLabel = t("bestHoursLabel");
  const expensiveLabel = t("expensiveHours");
  const minLabel = t("minimum");
  const maxLabel = t("maximum");

  const option = {
    backgroundColor: "transparent",
    tooltip: {
      trigger: "axis" as const,
      backgroundColor: theme.tooltipBg,
      borderColor: theme.tooltipBorder,
      borderWidth: 1,
      textStyle: {
        color: theme.tooltipText,
        fontFamily: theme.fontSans,
        fontSize: 12,
      },
      formatter: (params: Array<{ axisValue: string; seriesName: string; value: number | number[]; color: string }>) => {
        const hour = params[0]?.axisValue ?? "";
        let html = `<div style="font-weight:600;margin-bottom:6px">${hour}</div>`;
        params.forEach((p) => {
          if (p.seriesName === "_band") return;
          const val = Array.isArray(p.value) ? p.value[1] : p.value;
          html += `<div style="display:flex;align-items:center;gap:6px;margin:2px 0">
            <span style="width:8px;height:8px;border-radius:2px;background:${p.color}"></span>
            <span>${p.seriesName}</span>
            <span style="margin-left:auto;font-family:${theme.fontMono};font-weight:600">${formatPrice(val as number)} €</span>
          </div>`;
        });
        return html;
      },
    },
    grid: {
      top: 32,
      right: 20,
      bottom: 36,
      left: 58,
    },
    xAxis: {
      type: "category" as const,
      data: hours,
      axisLine: { lineStyle: { color: theme.axisLine } },
      axisLabel: {
        color: theme.axisLabel,
        fontSize: 11,
        fontFamily: theme.fontMono,
        interval: 1,
      },
      axisTick: { show: false },
      splitLine: { show: false },
    },
    yAxis: {
      type: "value" as const,
      name: "€/MWh",
      nameTextStyle: {
        color: theme.axisLabel,
        fontSize: 11,
        fontFamily: theme.fontSans,
        padding: [0, 40, 0, 0],
      },
      axisLine: { show: false },
      axisLabel: {
        color: theme.axisLabel,
        fontSize: 11,
        fontFamily: theme.fontMono,
      },
      splitLine: {
        lineStyle: { color: theme.splitLine, type: "dashed" as const },
      },
    },
    series: [
      {
        name: "_band",
        type: "line",
        data: p90,
        lineStyle: { opacity: 0 },
        areaStyle: { color: theme.bandFill },
        symbol: "none",
        z: 1,
        silent: true,
      },
      {
        name: "_band",
        type: "line",
        data: p10,
        lineStyle: { opacity: 0 },
        areaStyle: { color: theme.cardBg },
        symbol: "none",
        z: 2,
        silent: true,
      },
      {
        name: minLabel,
        type: "line",
        data: p10,
        lineStyle: {
          color: theme.bandLine,
          width: 1,
          type: "dashed" as const,
        },
        itemStyle: { color: theme.bandLine },
        symbol: "none",
        z: 3,
      },
      {
        name: maxLabel,
        type: "line",
        data: p90,
        lineStyle: {
          color: theme.bandLine,
          width: 1,
          type: "dashed" as const,
        },
        itemStyle: { color: theme.bandLine },
        symbol: "none",
        z: 3,
      },
      {
        name: expectedLabel,
        type: "line",
        data: p50,
        lineStyle: { color: theme.accentBlue, width: 2.5 },
        itemStyle: { color: theme.accentBlue },
        symbol: "circle",
        symbolSize: 3,
        z: 5,
        smooth: 0.15,
      },
      {
        name: bestLabel,
        type: "scatter",
        data: bestScatter,
        itemStyle: {
          color: theme.accentGreen,
          shadowColor: theme.greenGlow,
          shadowBlur: 8,
        },
        symbolSize: 10,
        z: 10,
      },
      {
        name: expensiveLabel,
        type: "scatter",
        data: worstScatter,
        itemStyle: {
          color: theme.accentRed,
          shadowColor: theme.redGlow,
          shadowBlur: 8,
        },
        symbolSize: 10,
        z: 10,
      },
    ],
  };

  return (
    <div className="card section">
      <div className="card-header">
        <div className="card-title">
          <span className="card-title-icon">📈</span>
          {t("tomorrowForecast")}
          <InfoButton text={t("infoPredictionBand")} />
        </div>
        <span className="data-badge data-badge-prediction">🔮 {t("prediction")}</span>
      </div>
      <div className="card-body-flush">
        <div className="chart-wrapper">
          <ReactECharts
            key={theme.mode}
            option={option}
            style={{ height: "100%", minHeight: 340 }}
            opts={{ renderer: "canvas" }}
          />
        </div>
      </div>
    </div>
  );
}
