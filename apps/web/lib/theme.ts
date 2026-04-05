"use client";

import { useEffect, useState } from "react";

type ThemeTokens = {
  mode: "dark" | "light";
  fontSans: string;
  fontMono: string;
  cardBg: string;
  accentBlue: string;
  accentGreen: string;
  accentRed: string;
  accentAmber: string;
  accentPurple: string;
  tooltipBg: string;
  tooltipBorder: string;
  tooltipText: string;
  legendText: string;
  axisLine: string;
  axisLabel: string;
  splitLine: string;
  bandFill: string;
  bandLine: string;
  rangeFill: string;
  rangeBorder: string;
  purpleFill: string;
  greenGlow: string;
  redGlow: string;
};

const FALLBACK_TOKENS: ThemeTokens = {
  mode: "dark",
  fontSans: "system-ui, sans-serif",
  fontMono: "ui-monospace, SFMono-Regular, monospace",
  cardBg: "#111827",
  accentBlue: "#3b82f6",
  accentGreen: "#10b981",
  accentRed: "#ef4444",
  accentAmber: "#f59e0b",
  accentPurple: "#8b5cf6",
  tooltipBg: "rgba(10, 15, 26, 0.94)",
  tooltipBorder: "rgba(59, 130, 246, 0.22)",
  tooltipText: "#f1f5f9",
  legendText: "#94a3b8",
  axisLine: "rgba(255, 255, 255, 0.08)",
  axisLabel: "#64748b",
  splitLine: "rgba(255, 255, 255, 0.05)",
  bandFill: "rgba(59, 130, 246, 0.12)",
  bandLine: "rgba(59, 130, 246, 0.30)",
  rangeFill: "rgba(59, 130, 246, 0.15)",
  rangeBorder: "rgba(59, 130, 246, 0.32)",
  purpleFill: "rgba(139, 92, 246, 0.08)",
  greenGlow: "rgba(16, 185, 129, 0.40)",
  redGlow: "rgba(239, 68, 68, 0.40)",
};

const TOKENS = {
  cardBg: "--bg-card",
  accentBlue: "--accent-blue",
  accentGreen: "--accent-green",
  accentRed: "--accent-red",
  accentAmber: "--accent-amber",
  accentPurple: "--accent-purple",
  tooltipBg: "--chart-tooltip-bg",
  tooltipBorder: "--chart-tooltip-border",
  tooltipText: "--chart-tooltip-text",
  legendText: "--chart-legend-text",
  axisLine: "--chart-axis-line",
  axisLabel: "--chart-axis-label",
  splitLine: "--chart-split-line",
  bandFill: "--chart-band-fill",
  bandLine: "--chart-band-line",
  rangeFill: "--chart-range-fill",
  rangeBorder: "--chart-range-border",
  purpleFill: "--chart-purple-fill",
  greenGlow: "--chart-green-glow",
  redGlow: "--chart-red-glow",
} as const;

function readThemeTokens(): ThemeTokens {
  if (typeof window === "undefined") {
    return FALLBACK_TOKENS;
  }

  const root = document.documentElement;
  const rootStyles = window.getComputedStyle(root);
  const bodyStyles = window.getComputedStyle(document.body);
  const mode = root.getAttribute("data-theme") === "light" ? "light" : "dark";
  const tokens: ThemeTokens = { ...FALLBACK_TOKENS, mode };
  const fontSans = bodyStyles.getPropertyValue("--font-plus-jakarta-sans").trim();
  const fontMono = bodyStyles.getPropertyValue("--font-jetbrains-mono").trim();

  if (fontSans) {
    tokens.fontSans = fontSans;
  }

  if (fontMono) {
    tokens.fontMono = fontMono;
  }

  for (const [key, cssVar] of Object.entries(TOKENS) as Array<[keyof typeof TOKENS, string]>) {
    const value = rootStyles.getPropertyValue(cssVar).trim();
    if (value) {
      tokens[key] = value;
    }
  }

  return tokens;
}

export function useThemeTokens(): ThemeTokens {
  const [tokens, setTokens] = useState<ThemeTokens>(FALLBACK_TOKENS);

  useEffect(() => {
    const update = () => setTokens(readThemeTokens());

    update();

    const observer = new MutationObserver(update);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme"],
    });

    return () => observer.disconnect();
  }, []);

  return tokens;
}
