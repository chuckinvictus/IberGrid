"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import type {
  DayAheadResponse,
  WeekAheadResponse,
  MarketContextResponse,
  PerformanceResponse,
  StatusResponse,
  AsyncState,
} from "../lib/types";
import {
  getDayAhead,
  getWeekAhead,
  getMarketContext,
  getPerformance,
  getStatus,
} from "../lib/api";
import {
  formatPrice,
  formatHour,
  tomorrowISO,
  daysAgoISO,
  daysAheadISO,
} from "../lib/format";
import { useLocale } from "../lib/i18n";
import InfoButton from "../components/InfoButton";
import DayAheadChart from "../components/DayAheadChart";
import WeekOutlook from "../components/WeekOutlook";
import { CheapestWindowsTable } from "../components/WeekOutlook";
import MarketChart from "../components/MarketChart";
import PerformancePanel from "../components/PerformancePanel";
import StatusPanel from "../components/StatusPanel";

const THEME_SWITCH_CLASS = "theme-switching";

function applyTheme(isDark: boolean, animate: boolean) {
  const root = document.documentElement;

  if (animate) {
    root.classList.remove(THEME_SWITCH_CLASS);
    void root.offsetWidth;
    root.classList.add(THEME_SWITCH_CLASS);
    window.setTimeout(() => {
      root.classList.remove(THEME_SWITCH_CLASS);
    }, 650);
  }

  root.setAttribute("data-theme", isDark ? "dark" : "light");
}

function BrandIcon({ size = 32 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <linearGradient id="bolt-grad" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="#3b82f6" />
          <stop offset="100%" stopColor="#06b6d4" />
        </linearGradient>
      </defs>
      <rect width="32" height="32" rx="6" fill="var(--bg-inset)" />
      <path d="M18 5L10 18h5l-1 9 8-13h-5l1-9z" fill="url(#bolt-grad)" />
    </svg>
  );
}

function HeroWave() {
  return (
    <div className="hero-wave-container" aria-hidden="true">
      <svg className="hero-wave" viewBox="0 0 1440 320" preserveAspectRatio="none">
        <defs>
          <linearGradient id="wave-grad-1" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor="rgba(59,130,246,0.30)" />
            <stop offset="50%" stopColor="rgba(6,182,212,0.22)" />
            <stop offset="100%" stopColor="rgba(59,130,246,0.12)" />
          </linearGradient>
          <linearGradient id="wave-grad-2" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor="rgba(6,182,212,0.18)" />
            <stop offset="50%" stopColor="rgba(59,130,246,0.24)" />
            <stop offset="100%" stopColor="rgba(6,182,212,0.10)" />
          </linearGradient>
        </defs>
        <path className="hero-wave-path-1" fill="url(#wave-grad-1)"
          d="M0,160 C200,60 480,280 720,160 C960,40 1200,260 1440,140 L1440,320 L0,320 Z" />
        <path className="hero-wave-path-2" fill="url(#wave-grad-2)"
          d="M0,200 C180,80 420,300 660,180 C900,60 1140,280 1440,160 L1440,320 L0,320 Z" />
      </svg>
    </div>
  );
}

function Skeleton({ className }: { className?: string }) {
  return <div className={`skeleton ${className ?? ""}`} />;
}

function ErrorCard({ message }: { message: string }) {
  return (
    <div className="error-panel">
      <span className="error-icon">⚠</span>
      {message}
    </div>
  );
}

function ThemeToggle() {
  const [dark, setDark] = useState(true);

  useEffect(() => {
    const stored = localStorage.getItem("ibergrid-theme");
    const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
    const isDark = stored ? stored === "dark" : prefersDark;
    setDark(isDark);
    applyTheme(isDark, false);
  }, []);

  const toggleTheme = useCallback(() => {
    const next = !dark;
    setDark(next);
    const val = next ? "dark" : "light";
    applyTheme(next, true);
    localStorage.setItem("ibergrid-theme", val);
  }, [dark]);

  return (
    <button className="theme-toggle" onClick={toggleTheme} aria-label={dark ? "Switch to light mode" : "Switch to dark mode"}>
      {dark ? (
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="12" cy="12" r="5"/>
          <line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/>
          <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/>
          <line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/>
          <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>
        </svg>
      ) : (
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
        </svg>
      )}
    </button>
  );
}

export default function Dashboard() {
  const { locale, toggle, t } = useLocale();
  const dashboardRef = useRef<HTMLDivElement>(null);

  const [dayAhead, setDayAhead] = useState<AsyncState<DayAheadResponse>>({ status: "loading" });
  const [weekAhead, setWeekAhead] = useState<AsyncState<WeekAheadResponse>>({ status: "loading" });
  const [market, setMarket] = useState<AsyncState<MarketContextResponse>>({ status: "loading" });
  const [performance, setPerformance] = useState<AsyncState<PerformanceResponse>>({ status: "loading" });
  const [status, setStatus] = useState<AsyncState<StatusResponse>>({ status: "loading" });
  const [techOpen, setTechOpen] = useState(false);

  useEffect(() => {
    const tomorrow = tomorrowISO();
    const from = daysAgoISO(2);
    const to = daysAheadISO(1);

    getDayAhead(tomorrow)
      .then((data) => setDayAhead({ status: "success", data }))
      .catch((e) => setDayAhead({ status: "error", error: e.message }));

    getWeekAhead(tomorrow)
      .then((data) => setWeekAhead({ status: "success", data }))
      .catch((e) => setWeekAhead({ status: "error", error: e.message }));

    getMarketContext(from, to)
      .then((data) => setMarket({ status: "success", data }))
      .catch((e) => setMarket({ status: "error", error: e.message }));

    getPerformance()
      .then((data) => setPerformance({ status: "success", data }))
      .catch((e) => setPerformance({ status: "error", error: e.message }));

    getStatus()
      .then((data) => setStatus({ status: "success", data }))
      .catch((e) => setStatus({ status: "error", error: e.message }));
  }, []);

  const dayData = dayAhead.status === "success" ? dayAhead.data : null;

  const meanP50 = dayData
    ? dayData.forecast.reduce((s, f) => s + f.p50, 0) / dayData.forecast.length
    : null;
  const minP50 = dayData ? Math.min(...dayData.forecast.map((f) => f.p50)) : null;
  const maxP50 = dayData ? Math.max(...dayData.forecast.map((f) => f.p50)) : null;
  const bestHour = dayData && dayData.best_hours.length > 0 ? dayData.best_hours[0] : null;
  const worstHour = dayData && dayData.worst_hours.length > 0 ? dayData.worst_hours[0] : null;
  const savingPct = minP50 != null && maxP50 != null && maxP50 > 0
    ? Math.round(((maxP50 - minP50) / maxP50) * 100)
    : null;

  const scrollToDashboard = () => {
    dashboardRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  return (
    <>
      {/* ── Landing Hero ── */}
      <section className="hero">
        <div className="hero-grid-bg" aria-hidden="true" />
        <div className="hero-glow hero-glow-1" aria-hidden="true" />
        <div className="hero-glow hero-glow-2" aria-hidden="true" />
        <HeroWave />

        <div className="hero-nav">
          <div className="hero-nav-brand">
            <BrandIcon size={28} />
            <span className="hero-nav-name">IberGrid</span>
          </div>
          <div className="hero-nav-right">
            <div className="lang-toggle" onClick={toggle} role="button" tabIndex={0} aria-label="Toggle language">
              <span className={`lang-option ${locale === "en" ? "lang-option-active" : ""}`}>EN</span>
              <span className={`lang-option ${locale === "es" ? "lang-option-active" : ""}`}>ES</span>
            </div>
            <ThemeToggle />
          </div>
        </div>

        <div className="hero-content">
          <div className="hero-price-wrap" aria-live="polite">
            {dayAhead.status === "loading" && (
              <div className="hero-price-pill">
                <div className="hero-price-loading" />
              </div>
            )}
            {dayAhead.status === "error" && (
              <div className="hero-price-pill hero-price-pill-error">
                <span className="hero-price-error">{t("heroPriceUnavailable")}</span>
              </div>
            )}
            {dayAhead.status === "success" && meanP50 != null && (
              <div className="hero-price-pill hero-price-pill-ready">
                <span className="hero-price-label">{t("heroPriceLabel")}</span>
                <div className="hero-price-main">
                  <span className="hero-price-value">{formatPrice(meanP50)}</span>
                  <span className="hero-price-unit">€/MWh</span>
                </div>
              </div>
            )}
          </div>
          <h1 className="hero-headline">{t("heroHeadline")}</h1>
          <p className="hero-desc">{t("heroDesc")}</p>

          <button className="hero-cta" onClick={scrollToDashboard}>
            {t("heroCta")}
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <path d="M8 3v10M4 9l4 4 4-4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </button>
        </div>

        <div className="hero-fade" aria-hidden="true" />
      </section>

      {/* ── Dashboard ── */}
      <main className="dashboard" ref={dashboardRef}>
        {/* KPI Strip */}
        <div className="kpi-strip" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))" }}>
          {dayData ? (
            <>
              <div className="kpi-card">
                <div className="kpi-label">
                  {t("avgPrice")}
                  <InfoButton text={t("infoPVPC")} />
                </div>
                <div className="kpi-value" style={{ color: "var(--accent-blue)" }}>
                  {formatPrice(meanP50!)}
                  <span className="kpi-unit">€/MWh</span>
                </div>
                <div className="kpi-sub">{t("avgPriceSub")}</div>
              </div>
              <div className="kpi-card">
                <div className="kpi-label">
                  {t("bestTime")}
                  <InfoButton text={t("infoBestHours")} />
                </div>
                <div className="kpi-value" style={{ color: "var(--accent-green)" }}>
                  {bestHour != null ? formatHour(bestHour) : "—"}
                </div>
                <div className="kpi-sub">
                  {bestHour != null
                    ? `${formatPrice(dayData.forecast[bestHour].p50)} €/MWh — ${t("bestTimeSub")}`
                    : t("noData")}
                </div>
              </div>
              <div className="kpi-card">
                <div className="kpi-label">
                  {t("avoidTime")}
                  <InfoButton text={t("infoAvoidHours")} />
                </div>
                <div className="kpi-value" style={{ color: "var(--accent-red)" }}>
                  {worstHour != null ? formatHour(worstHour) : "—"}
                </div>
                <div className="kpi-sub">
                  {worstHour != null
                    ? `${formatPrice(dayData.forecast[worstHour].p50)} €/MWh — ${t("avoidTimeSub")}`
                    : t("noData")}
                </div>
              </div>
              <div className="kpi-card">
                <div className="kpi-label">
                  {t("potentialSaving")}
                  <InfoButton text={t("infoSavings")} />
                </div>
                <div className="kpi-value" style={{ color: "var(--accent-green)" }}>
                  {savingPct != null ? `${savingPct}%` : "—"}
                </div>
                <div className="kpi-sub">{t("potentialSavingSub")}</div>
              </div>
            </>
          ) : dayAhead.status === "loading" ? (
            Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="skeleton-kpi" />
            ))
          ) : (
            <div style={{ gridColumn: "1 / -1" }}>
              <ErrorCard message={dayAhead.status === "error" ? dayAhead.error : t("noData")} />
            </div>
          )}
        </div>

        {/* Day-Ahead Chart */}
        {dayAhead.status === "loading" && (
          <div className="section">
            <Skeleton className="skeleton-chart" />
          </div>
        )}
        {dayAhead.status === "error" && (
          <div className="section">
            <div className="card">
              <div className="card-header">
                <div className="card-title">
                  <span className="card-title-icon">📈</span> {t("tomorrowForecast")}
                </div>
              </div>
              <div className="card-body">
                <ErrorCard message={dayAhead.error} />
              </div>
            </div>
          </div>
        )}
        {dayData && <DayAheadChart data={dayData} />}

        {/* Week Outlook + Market Context */}
        <div className="grid-2 section">
          <div>
            {weekAhead.status === "loading" && <Skeleton className="skeleton-chart" />}
            {weekAhead.status === "error" && (
              <div className="card">
                <div className="card-header">
                  <div className="card-title">
                    <span className="card-title-icon">📅</span> {t("weekForecast")}
                  </div>
                </div>
                <div className="card-body">
                  <ErrorCard message={weekAhead.error} />
                </div>
              </div>
            )}
            {weekAhead.status === "success" && <WeekOutlook data={weekAhead.data} />}
          </div>
          <div>
            {market.status === "loading" && <Skeleton className="skeleton-chart" />}
            {market.status === "error" && (
              <div className="card">
                <div className="card-header">
                  <div className="card-title">
                    <span className="card-title-icon">⚡</span> {t("recentPrices")}
                  </div>
                </div>
                <div className="card-body">
                  <ErrorCard message={market.error} />
                </div>
              </div>
            )}
            {market.status === "success" && <MarketChart data={market.data} />}
          </div>
        </div>

        {/* Cheapest Windows — full width */}
        {weekAhead.status === "success" && <CheapestWindowsTable data={weekAhead.data} />}

        {/* Collapsible Technical Details */}
        <div className="section">
          <div className="tech-toggle" onClick={() => setTechOpen((o) => !o)} role="button" tabIndex={0}>
            <div className="tech-toggle-left">
              <span>🔬</span>
              <div>
                <div className="tech-toggle-title">{t("technicalDetails")}</div>
                <div className="tech-toggle-sub">{t("technicalDetailsSub")}</div>
              </div>
            </div>
            <span className={`tech-toggle-arrow ${techOpen ? "tech-toggle-arrow-open" : ""}`}>▶</span>
          </div>
          <div className="tech-content" style={{ maxHeight: techOpen ? "3000px" : "0px" }}>
            <div className="grid-2" style={{ marginBottom: 20 }}>
              <div>
                {performance.status === "loading" && <Skeleton className="skeleton-chart" />}
                {performance.status === "error" && (
                  <div className="card">
                    <div className="card-header">
                      <div className="card-title">
                        <span className="card-title-icon">🧪</span> {t("modelPerformance")}
                      </div>
                    </div>
                    <div className="card-body">
                      <ErrorCard message={performance.error} />
                    </div>
                  </div>
                )}
                {performance.status === "success" && <PerformancePanel data={performance.data} />}
              </div>
              <div>
                {status.status === "loading" && <Skeleton className="skeleton-table" />}
                {status.status === "error" && (
                  <div className="card">
                    <div className="card-header">
                      <div className="card-title">
                        <span className="card-title-icon">🔧</span> {t("systemStatus")}
                      </div>
                    </div>
                    <div className="card-body">
                      <ErrorCard message={status.error} />
                    </div>
                  </div>
                )}
                {status.status === "success" && <StatusPanel data={status.data} />}
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <footer style={{ textAlign: "center", padding: "24px 0", fontSize: "0.72rem", color: "var(--text-muted)" }}>
          {t("footer")}
        </footer>
      </main>
    </>
  );
}
