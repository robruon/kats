"use client";

import { Header } from "@/components/Header";
import { JournalView } from "@/components/JournalView";

export default function JournalPage() {
  return (
    <div className="flex flex-col h-screen overflow-hidden">
      <Header />
      <div className="flex-1 min-h-0">
        <JournalView />
      </div>
    </div>
  );
}
