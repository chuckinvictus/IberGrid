"use client";

import { createContext, useContext, useState, useCallback, type ReactNode } from "react";

export type Locale = "en" | "es";

const translations = {
  subtitle: { en: "Electricity Price Intelligence", es: "Inteligencia de Precios Eléctricos" },
  online: { en: "Online", es: "En línea" },

  avgPrice: { en: "Average Price", es: "Precio Medio" },
  avgPriceSub: { en: "Expected average for tomorrow", es: "Media esperada para mañana" },
  bestTime: { en: "Best Time to Use Energy", es: "Mejor Hora para Consumir" },
  bestTimeSub: { en: "Cheapest hour — run your appliances now", es: "Hora más barata — pon tus electrodomésticos" },
  avoidTime: { en: "Avoid This Hour", es: "Evitar Esta Hora" },
  avoidTimeSub: { en: "Most expensive — reduce your usage", es: "Hora más cara — reduce tu consumo" },
  potentialSaving: { en: "Potential Savings", es: "Ahorro Potencial" },
  potentialSavingSub: { en: "By shifting usage to the cheapest hour", es: "Cambiando el consumo a la hora más barata" },

  noData: { en: "No data", es: "Sin datos" },

  tomorrowForecast: { en: "Tomorrow's Price Forecast", es: "Previsión de Precios de Mañana" },
  weekForecast: { en: "Weekly Price Forecast", es: "Previsión Semanal de Precios" },
  recentPrices: { en: "Recent Prices & Demand", es: "Precios y Demanda Recientes" },
  technicalDetails: { en: "Technical Details", es: "Detalles Técnicos" },
  technicalDetailsSub: { en: "Model metrics, calibration, and data sources", es: "Métricas del modelo, calibración y fuentes de datos" },

  prediction: { en: "Prediction", es: "Predicción" },
  realData: { en: "Real Data", es: "Datos Reales" },

  expectedPrice: { en: "Expected Price", es: "Precio Esperado" },
  possibleRange: { en: "Possible Range", es: "Rango Posible" },
  bestHoursLabel: { en: "Best Hours", es: "Mejores Horas" },
  expensiveHours: { en: "Expensive Hours", es: "Horas Caras" },
  minimum: { en: "Minimum", es: "Mínimo" },
  maximum: { en: "Maximum", es: "Máximo" },

  cheapestWindows: { en: "Best Windows to Use Energy", es: "Mejores Ventanas para Consumir" },
  day: { en: "Day", es: "Día" },
  best2h: { en: "Best 2h", es: "Mejor 2h" },
  avg2h: { en: "Avg Price", es: "Precio Medio" },
  best4h: { en: "Best 4h", es: "Mejor 4h" },
  avg4h: { en: "Avg Price", es: "Precio Medio" },
  peakRisk: { en: "Expensive", es: "Caro" },

  demand: { en: "Demand", es: "Demanda" },

  model: { en: "Model", es: "Modelo" },
  slice: { en: "Slice", es: "Segmento" },
  coverage: { en: "Coverage", es: "Cobertura" },
  calibrationCoverage: { en: "Calibration Coverage", es: "Cobertura de Calibración" },
  belowP10: { en: "Below P10", es: "Bajo P10" },
  withinBand: { en: "Within Band", es: "Dentro de Banda" },
  aboveP90: { en: "Above P90", es: "Sobre P90" },
  latestBacktest: { en: "Latest Backtest", es: "Último Backtest" },
  actual: { en: "Actual", es: "Real" },
  predicted: { en: "Predicted", es: "Predicción" },
  lastPromotion: { en: "Last promotion", es: "Última promoción" },
  modelPerformance: { en: "Model Performance", es: "Rendimiento del Modelo" },

  systemStatus: { en: "System Status", es: "Estado del Sistema" },
  latestIngestion: { en: "Latest Ingestion", es: "Última Ingesta" },
  latestForecast: { en: "Latest Forecast", es: "Última Previsión" },
  latestTraining: { en: "Latest Training", es: "Último Entrenamiento" },
  promoted: { en: "Promoted", es: "Promovido" },
  sourceHealth: { en: "Source Health", es: "Estado de Fuentes" },

  footer: {
    en: "IberGrid Intelligence Platform · Peninsular Spain · Read-only public product",
    es: "Plataforma IberGrid · España Peninsular · Producto público de solo lectura",
  },

  infoPVPC: {
    en: "PVPC (Voluntary Price for Small Consumers) is the regulated electricity tariff in Spain. If you have a regulated tariff with your energy provider, this is the price you pay per kWh.",
    es: "El PVPC (Precio Voluntario para el Pequeño Consumidor) es la tarifa regulada de electricidad en España. Si tienes tarifa regulada con tu compañía eléctrica, este es el precio que pagas por kWh.",
  },
  infoPredictionBand: {
    en: "The shaded area shows the range where the actual price is likely to fall (80% probability). The line shows the most likely price. Wider bands mean more uncertainty.",
    es: "El área sombreada muestra el rango donde probablemente caerá el precio real (80% de probabilidad). La línea muestra el precio más probable. Bandas más anchas indican más incertidumbre.",
  },
  infoBestHours: {
    en: "These are the cheapest hours of the day. Schedule high-consumption appliances (washing machine, dishwasher, EV charging) during these hours to save on your electricity bill.",
    es: "Estas son las horas más baratas del día. Programa electrodomésticos de alto consumo (lavadora, lavavajillas, carga de coche eléctrico) en estas horas para ahorrar en tu factura.",
  },
  infoAvoidHours: {
    en: "These are the most expensive hours. Try to avoid running heavy appliances during these times to keep your bill low.",
    es: "Estas son las horas más caras del día. Intenta evitar electrodomésticos de alto consumo en estas horas para mantener tu factura baja.",
  },
  infoSpotPrice: {
    en: "The Spot price is the wholesale electricity price set by the daily market. The PVPC tariff is calculated from this price plus regulated charges.",
    es: "El precio Spot es el precio mayorista de la electricidad fijado por el mercado diario. La tarifa PVPC se calcula a partir de este precio más cargos regulados.",
  },
  infoDemand: {
    en: "Electricity demand is the total amount of power being consumed across Spain. Higher demand usually pushes prices up.",
    es: "La demanda eléctrica es la cantidad total de energía consumida en España. Mayor demanda suele hacer subir los precios.",
  },
  infoWeekForecast: {
    en: "This chart shows expected daily average prices for the coming week. Use it to plan energy-intensive tasks on cheaper days.",
    es: "Este gráfico muestra los precios medios diarios esperados para la próxima semana. Úsalo para planificar tareas de alto consumo en los días más baratos.",
  },
  infoCheapestWindows: {
    en: "The best 2-hour and 4-hour blocks to concentrate your energy use each day. Plan laundry, cooking, or EV charging during these windows.",
    es: "Los mejores bloques de 2 y 4 horas para concentrar tu consumo cada día. Planifica coladas, cocina o carga de coche eléctrico en estas ventanas.",
  },
  infoSavings: {
    en: "This shows how much you could save per MWh by shifting your appliance usage from the most expensive hour to the cheapest hour.",
    es: "Muestra cuánto podrías ahorrar por MWh cambiando el uso de electrodomésticos de la hora más cara a la más barata.",
  },

  heroHeadline: {
    en: "Know tomorrow's electricity price today.",
    es: "Conoce el precio de la luz de mañana, hoy.",
  },
  heroDesc: {
    en: "AI-powered PVPC forecasts with uncertainty bands for peninsular Spain. Open data, zero cost.",
    es: "Previsiones PVPC con inteligencia artificial y bandas de incertidumbre para la España peninsular. Datos abiertos, coste cero.",
  },
  heroCta: {
    en: "See the forecast",
    es: "Ver la previsión",
  },
  heroPriceLabel: {
    en: "Expected average for tomorrow",
    es: "Media prevista para mañana",
  },
  heroCheapestAt: {
    en: "Cheapest at",
    es: "Más barata a las",
  },
  heroPriceUnavailable: {
    en: "Tomorrow forecast unavailable",
    es: "Previsión de mañana no disponible",
  },
  heroLive: {
    en: "Live",
    es: "En vivo",
  },
} as const;

type TranslationKey = keyof typeof translations;

type I18nContextValue = {
  locale: Locale;
  toggle: () => void;
  t: (key: TranslationKey) => string;
};

const I18nContext = createContext<I18nContextValue | null>(null);

export function I18nProvider({ children }: { children: ReactNode }) {
  const [locale, setLocale] = useState<Locale>("es");

  const toggle = useCallback(() => {
    setLocale((prev) => (prev === "en" ? "es" : "en"));
  }, []);

  const t = useCallback(
    (key: TranslationKey): string => translations[key][locale],
    [locale]
  );

  return (
    <I18nContext.Provider value={{ locale, toggle, t }}>
      {children}
    </I18nContext.Provider>
  );
}

export function useLocale() {
  const ctx = useContext(I18nContext);
  if (!ctx) throw new Error("useLocale must be used within I18nProvider");
  return ctx;
}
