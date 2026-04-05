import type {
  DayAheadResponse,
  WeekAheadResponse,
  MarketContextResponse,
  PerformanceResponse,
  StatusResponse,
} from "./types";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8000";

async function fetchApi<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(
      (body as { detail?: string }).detail ?? `${res.status} ${res.statusText}`
    );
  }
  return res.json() as Promise<T>;
}

export function getDayAhead(date: string): Promise<DayAheadResponse> {
  return fetchApi(`/api/v1/forecast/day-ahead?date=${date}`);
}

export function getWeekAhead(from: string): Promise<WeekAheadResponse> {
  return fetchApi(`/api/v1/forecast/week-ahead?from=${from}`);
}

export function getMarketContext(
  from: string,
  to: string
): Promise<MarketContextResponse> {
  return fetchApi(
    `/api/v1/context/market?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`
  );
}

export function getPerformance(): Promise<PerformanceResponse> {
  return fetchApi("/api/v1/model/performance/latest");
}

export function getStatus(): Promise<StatusResponse> {
  return fetchApi("/api/v1/status/latest");
}
