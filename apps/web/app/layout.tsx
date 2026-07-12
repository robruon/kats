import type { Metadata } from "next";
import "./globals.css";
import { EngineProvider } from "@/components/EngineContext";

export const metadata: Metadata = {
  title: "KronosTrade",
  description: "Autonomous trading system dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="h-full">
      <body className="h-full">
        <EngineProvider>{children}</EngineProvider>
      </body>
    </html>
  );
}
