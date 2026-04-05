"use client";

import dynamic from "next/dynamic";
import type { MarketContextResponse } from "../lib/types";
import { formatPrice } from "../lib/format";
import { useLocale } from "../lib/i18n";
import { useThemeTokens } from "../lib/theme";
import InfoButton from "./InfoButton";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

function formatTS(ts: string) {
  const d = new Date(ts);
  return `${d.getHours()}:00`;
}

function formatTSFull(ts: string) {
  const d = new Date(ts);
  const day = d.getDate();
  const month = d.toLocaleDateString("en-US", { month: "short" });
  return `${day} ${month} ${String(d.getHours()).padStart(2, "0")}:00`;
}

export default function MarketChart({ data }: { data: MarketContextResponse }) {
  const { t } = useLocale();
  const theme = useThemeTokens();

  const demandLabel = t("demand");
  const timestamps = data.hourly.map((h) => formatTS(h.timestamp));
  const pvpc = data.hourly.map((h) => h.pvpc_eur_mwh ?? null);
  const spot = data.hourly.map((h) => h.spot_eur_mwh ?? null);
  const demand = data.hourly.map((h) => h.demand_actual_mw ?? null);

  const option = {
    backgroundColor: "transparent",
    tooltip: {
      trigger: "axis" as const,
      backgroundColor: theme.tooltipBg,
      borderColor: theme.tooltipBorder,
      borderWidth: 1,
      textStyle: { color: theme.tooltipText, fontFamily: theme.fontSans, fontSize: 12 },
      formatter: (params: Array<{ axisValue: string; seriesName: string; value: number | null; color: string; dataIndex: number }>) => {
        const idx = params[0]?.dataIndex ?? 0;
        const label = idx < data.hourly.length ? formatTSFull(data.hourly[idx].timestamp) : params[0]?.axisValue ?? "";
        let html = `<div style="font-weight:600;margin-bottom:6px">${label}</div>`;
        params.forEach((p) => {
          if (p.value == null) return;
          const unit = p.seriesName === demandLabel ? " MW" : " €";
          const val = p.seriesName === demandLabel ? Math.round(p.value).toLocaleString() : formatPrice(p.value);
          html += `<div style="display:flex;align-items:center;gap:6px;margin:2px 0">
            <span style="width:8px;height:8px;border-radius:2px;background:${p.color}"></span>
            <span>${p.seriesName}</span>
            <span style="margin-left:auto;font-family:${theme.fontMono};font-weight:600">${val}${unit}</span>
          </div>`;
        });
        return html;
      },
    },
    legend: {
      data: ["PVPC", "Spot", demandLabel],
      top: 0,
      right: 0,
      textStyle: { color: theme.legendText, fontSize: 11, fontFamily: theme.fontSans },
      itemWidth: 14,
      itemHeight: 3,
    },
    grid: { top: 36, right: 60, bottom: 46, left: 58 },
    xAxis: {
      type: "category" as const,
      data: timestamps,
      axisLine: { lineStyle: { color: theme.axisLine } },
      axisLabel: {
        color: theme.axisLabel,
        fontSize: 10,
        fontFamily: theme.fontMono,
        interval: (_idx: number, value: string) => {
          const hour = parseInt(value);
          return hour % 6 === 0;
        },
        formatter: (value: string, idx: number) => {
          const hour = parseInt(value);
          if (hour === 0 && idx < data.hourly.length) {
            const d = new Date(data.hourly[idx].timestamp);
            return `{date|${d.getDate()} ${d.toLocaleDateString("en-US", { month: "short" })}}\n{hour|${value}}`;
          }
          return value;
        },
        rich: {
          date: { fontSize: 9, color: theme.legendText, fontWeight: 600, lineHeight: 16, fontFamily: theme.fontSans },
          hour: { fontSize: 10, color: theme.axisLabel, fontFamily: theme.fontMono },
        },
      },
      axisTick: { show: false },
    },
    yAxis: [
      {
        type: "value" as const,
        name: "€/MWh",
        nameTextStyle: { color: theme.axisLabel, fontSize: 10, padding: [0, 36, 0, 0] },
        axisLine: { show: false },
        axisLabel: { color: theme.axisLabel, fontSize: 10, fontFamily: theme.fontMono },
        splitLine: { lineStyle: { color: theme.splitLine, type: "dashed" as const } },
      },
      {
        type: "value" as const,
        name: "MW",
        nameTextStyle: { color: theme.axisLabel, fontSize: 10, padding: [0, 0, 0, 36] },
        axisLine: { show: false },
        axisLabel: { color: theme.axisLabel, fontSize: 10, fontFamily: theme.fontMono },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: "PVPC",
        type: "line",
        data: pvpc,
        lineStyle: { color: theme.accentBlue, width: 2 },
        itemStyle: { color: theme.accentBlue },
        symbol: "none",
        smooth: 0.1,
        connectNulls: true,
        yAxisIndex: 0,
      },
      {
        name: "Spot",
        type: "line",
        data: spot,
        lineStyle: { color: theme.accentAmber, width: 2 },
        itemStyle: { color: theme.accentAmber },
        symbol: "none",
        smooth: 0.1,
        connectNulls: true,
        yAxisIndex: 0,
      },
      {
        name: demandLabel,
        type: "line",
        data: demand,
        lineStyle: { color: theme.accentPurple, width: 1.5, opacity: 0.6 },
        areaStyle: { color: theme.purpleFill },
        itemStyle: { color: theme.accentPurple },
        symbol: "none",
        smooth: 0.1,
        connectNulls: true,
        yAxisIndex: 1,
      },
    ],
  };

  return (
    <div className="card">
      <div className="card-header">
        <div className="card-title">
          <span className="card-title-icon">⚡</span>
          {t("recentPrices")}
          <InfoButton text={t("infoSpotPrice")} />
        </div>
        <span className="data-badge data-badge-real">📊 {t("realData")}</span>
      </div>
      <div className="card-body-flush">
        <div className="chart-wrapper chart-wrapper-sm">
          <ReactECharts
            key={theme.mode}
            option={option}
            style={{ height: "100%", minHeight: 280 }}
            opts={{ renderer: "canvas" }}
          />
        </div>
      </div>
    </div>
  );
}
