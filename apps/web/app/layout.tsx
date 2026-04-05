import type { Metadata } from "next";
import { Plus_Jakarta_Sans, JetBrains_Mono } from "next/font/google";
import "./globals.css";
import { I18nProvider } from "../lib/i18n";

const plusJakartaSans = Plus_Jakarta_Sans({
  subsets: ["latin"],
  display: "swap",
  variable: "--font-plus-jakarta-sans",
});

const jetBrainsMono = JetBrains_Mono({
  subsets: ["latin"],
  display: "swap",
  variable: "--font-jetbrains-mono",
});

export const metadata: Metadata = {
  title: "IberGrid — Electricity Price Intelligence",
  description:
    "Next-day and weekly PVPC electricity price forecasts for peninsular Spain with quantile bands, market context, and model diagnostics.",
  icons: {
    icon: {
      url: "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><defs><linearGradient id='g' x1='0' y1='0' x2='1' y2='1'><stop offset='0%25' stop-color='%233b82f6'/><stop offset='100%25' stop-color='%2306b6d4'/></linearGradient></defs><rect width='32' height='32' rx='6' fill='%23111827'/><path d='M18 5L10 18h5l-1 9 8-13h-5l1-9z' fill='url(%23g)'/></svg>",
      type: "image/svg+xml",
    },
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className={`${plusJakartaSans.className} ${plusJakartaSans.variable} ${jetBrainsMono.variable}`}>
        <I18nProvider>{children}</I18nProvider>
      </body>
    </html>
  );
}
