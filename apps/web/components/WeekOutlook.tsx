"use client";

import dynamic from "next/dynamic";
import type { WeekAheadResponse } from "../lib/types";
import { formatPrice, formatWeekday, formatHour, formatDate } from "../lib/format";
import { useLocale } from "../lib/i18n";
import { useThemeTokens } from "../lib/theme";
import InfoButton from "./InfoButton";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

function hoursToRanges(hours: number[]): string {
  if (hours.length === 0) return "—";
  const sorted = [...hours].sort((a, b) => a - b);
  const ranges: string[] = [];
  let start = sorted[0];
  let end = sorted[0];
  for (let i = 1; i < sorted.length; i++) {
    if (sorted[i] === end + 1) {
      end = sorted[i];
    } else {
      ranges.push(start === end ? formatHour(start) : `${formatHour(start)}–${formatHour(end + 1)}`);
      start = sorted[i];
      end = sorted[i];
    }
  }
  ranges.push(start === end ? formatHour(start) : `${formatHour(start)}–${formatHour(end + 1)}`);
  return ranges.join(", ");
}

export function WeekChart({ data }: { data: WeekAheadResponse }) {
  const { t } = useLocale();
  const theme = useThemeTokens();

  const days = data.daily_bands.map((b) => {
    const wd = formatWeekday(b.day);
    const d = formatDate(b.day);
    return `${wd} ${d}`;
  });

  const option = {
    backgroundColor: "transparent",
    tooltip: {
      trigger: "axis" as const,
      backgroundColor: theme.tooltipBg,
      borderColor: theme.tooltipBorder,
      borderWidth: 1,
      textStyle: { color: theme.tooltipText, fontFamily: theme.fontSans, fontSize: 12 },
      formatter: (params: Array<{ axisValue: string; seriesName: string; value: number; color: string }>) => {
        const day = params[0]?.axisValue ?? "";
        let html = `<div style="font-weight:600;margin-bottom:6px">${day}</div>`;
        params.forEach((p) => {
          if (p.seriesName === "_base" || p.seriesName === "_band") return;
          html += `<div style="display:flex;align-items:center;gap:6px;margin:2px 0">
            <span style="width:8px;height:8px;border-radius:2px;background:${p.color}"></span>
            <span>${p.seriesName}</span>
            <span style="margin-left:auto;font-family:${theme.fontMono};font-weight:600">${formatPrice(p.value)} €</span>
          </div>`;
        });
        return html;
      },
    },
    grid: { top: 28, right: 16, bottom: 32, left: 54 },
    xAxis: {
      type: "category" as const,
      data: days,
      axisLine: { lineStyle: { color: theme.axisLine } },
      axisLabel: { color: theme.axisLabel, fontSize: 10, fontFamily: theme.fontSans },
      axisTick: { show: false },
    },
    yAxis: {
      type: "value" as const,
      name: "€/MWh",
      nameTextStyle: { color: theme.axisLabel, fontSize: 10, padding: [0, 36, 0, 0] },
      axisLine: { show: false },
      axisLabel: { color: theme.axisLabel, fontSize: 10, fontFamily: theme.fontMono },
      splitLine: { lineStyle: { color: theme.splitLine, type: "dashed" as const } },
    },
    series: [
      {
        name: "_base",
        type: "bar",
        data: data.daily_bands.map((b) => b.mean_p10),
        stack: "band",
        itemStyle: { color: "transparent" },
        emphasis: { itemStyle: { color: "transparent" } },
        silent: true,
      },
      {
        name: t("possibleRange"),
        type: "bar",
        data: data.daily_bands.map((b) => b.mean_p90 - b.mean_p10),
        stack: "band",
        itemStyle: {
          color: theme.rangeFill,
          borderColor: theme.rangeBorder,
          borderWidth: 1,
          borderRadius: [4, 4, 0, 0],
        },
      },
      {
        name: t("expectedPrice"),
        type: "line",
        data: data.daily_bands.map((b) => b.mean_p50),
        lineStyle: { color: theme.accentBlue, width: 2.5 },
        itemStyle: { color: theme.accentBlue },
        symbol: "circle",
        symbolSize: 7,
        z: 10,
      },
    ],
  };

  return (
    <div className="card">
      <div className="card-header">
        <div className="card-title">
          <span className="card-title-icon">📅</span>
          {t("weekForecast")}
          <InfoButton text={t("infoWeekForecast")} />
        </div>
        <span className="data-badge data-badge-prediction">🔮 {t("prediction")}</span>
      </div>
      <div className="card-body-flush">
        <div className="chart-wrapper chart-wrapper-sm">
          <ReactECharts
            key={theme.mode}
            option={option}
            style={{ height: "100%", minHeight: 260 }}
            opts={{ renderer: "canvas" }}
          />
        </div>
      </div>
    </div>
  );
}

export function CheapestWindowsTable({ data }: { data: WeekAheadResponse }) {
  const { t } = useLocale();

  if (data.cheapest_windows.length === 0) return null;

  return (
    <div className="card section">
      <div className="card-header">
        <div className="card-title">
          {t("cheapestWindows")}
          <InfoButton text={t("infoCheapestWindows")} />
        </div>
      </div>
      <div className="card-body-flush">
        <div className="table-scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>{t("day")}</th>
                <th>{t("best2h")}</th>
                <th>{t("avg2h")}</th>
                <th>{t("best4h")}</th>
                <th>{t("avg4h")}</th>
                <th>{t("peakRisk")}</th>
              </tr>
            </thead>
            <tbody>
              {data.cheapest_windows.map((w) => (
                <tr key={w.day}>
                  <td className="cell-name">{formatWeekday(w.day)} {formatDate(w.day)}</td>
                  <td>
                    <span className="hour-tag hour-tag-green">{hoursToRanges(w.best_two_hour_window)}</span>
                  </td>
                  <td>{formatPrice(w.avg_two_hour_price)}</td>
                  <td>
                    <span className="hour-tag hour-tag-green">{hoursToRanges(w.best_four_hour_window)}</span>
                  </td>
                  <td>{formatPrice(w.avg_four_hour_price)}</td>
                  <td>
                    <span className="hour-tag hour-tag-red">{hoursToRanges(w.peak_risk_hours)}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

export default function WeekOutlook({ data }: { data: WeekAheadResponse }) {
  return (
    <>
      <WeekChart data={data} />
    </>
  );
}
