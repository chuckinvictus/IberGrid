export type SourceHealth = {
  name: string;
  last_observed_at?: string | null;
  status: string;
  detail?: string | null;
  freshness_hours?: number | null;
  row_count?: number | null;
  null_rate?: number | null;
  metrics_json?: Record<string, unknown> | null;
};

export type DriverImpact = {
  name: string;
  score: number;
  direction: string;
};

export type HourlyForecast = {
  timestamp: string;
  p10: number;
  p50: number;
  p90: number;
  risk_level: string;
  relative_cheapness_score: number;
  savings_vs_daily_mean: number;
  utility?: {
    is_cheapest_2h_candidate?: boolean;
    is_cheapest_4h_candidate?: boolean;
    is_peak_risk?: boolean;
  };
  local_explanations?: {
    confidence?: string;
    positive_drivers?: DriverImpact[];
    negative_drivers?: DriverImpact[];
  };
};

export type HistoryPoint = {
  timestamp: string;
  pvpc_eur_mwh: number;
  spot_eur_mwh: number;
};

export type DayAheadResponse = {
  forecast_run_id: number;
  forecast: HourlyForecast[];
  history: HistoryPoint[];
  best_hours: number[];
  worst_hours: number[];
  metadata: {
    generated_at: string;
    serving_mode: string;
    fallback_reason?: string | null;
    model_version: string;
    source_health: SourceHealth[];
    source_health_state?: string;
    degradation_notes?: string[];
    freshness: StatusResponse;
  };
};

export type WeekDailyBand = {
  day: string;
  mean_p10: number;
  mean_p50: number;
  mean_p90: number;
  min_p50: number;
  max_p50: number;
  risk_level: string;
  relative_cheapness_score: number;
  aggregate_savings_signal: number;
};

export type CheapestWindow = {
  day: string;
  best_two_hour_window: number[];
  best_four_hour_window: number[];
  avg_two_hour_price: number;
  avg_four_hour_price: number;
  peak_risk_hours: number[];
};

export type WeeklyExplanation = {
  horizon_bucket: string;
  confidence: string;
  positive_drivers: DriverImpact[];
  negative_drivers: DriverImpact[];
};

export type WeekAheadResponse = {
  daily_bands: WeekDailyBand[];
  cheapest_windows: CheapestWindow[];
  weekly_explanations: WeeklyExplanation[];
  metadata: {
    generated_at: string;
    serving_mode: string;
    model_version: string;
    source_health: SourceHealth[];
  };
};

export type MarketHourly = {
  timestamp: string;
  pvpc_eur_mwh?: number | null;
  spot_eur_mwh?: number | null;
  demand_actual_mw?: number | null;
  demand_forecast_mw?: number | null;
  temperature_c?: number | null;
  wind_speed_kmh?: number | null;
  shortwave_radiation_wm2?: number | null;
};

export type MarketContextResponse = {
  hourly: MarketHourly[];
  generation_mix_daily: Record<string, unknown>[];
  source_health: SourceHealth[];
};

export type Benchmark = {
  name: string;
  slice_name: string;
  mae: number;
  rmse: number;
  smape: number;
  quantile_loss_p10?: number | null;
  quantile_loss_p50?: number | null;
  quantile_loss_p90?: number | null;
  coverage_p10_p90?: number | null;
  cheapest_window_hit_rate?: number | null;
};

export type BacktestPoint = {
  timestamp: string;
  actual: number;
  predicted: number;
};

export type PerformanceResponse = {
  benchmarks: Benchmark[];
  calibration: {
    below_p10: number;
    within_band: number;
    above_p90: number;
  };
  latest_backtest_curve: BacktestPoint[];
  champion_model: {
    version?: string | null;
    model_type: string;
    promotion_summary: Record<string, unknown>;
  };
  last_promotion_decision: string;
  source_health: SourceHealth[];
};

export type StatusResponse = {
  latest_ingestion?: Record<string, unknown> | null;
  latest_training?: Record<string, unknown> | null;
  latest_forecast?: Record<string, unknown> | null;
  latest_model: {
    version?: string | null;
    model_type: string;
    promoted_at?: string | null;
  };
  source_health: SourceHealth[];
  serving_mode: string;
};

export type AsyncState<T> =
  | { status: "loading" }
  | { status: "error"; error: string }
  | { status: "success"; data: T };
