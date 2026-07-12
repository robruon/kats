import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./hooks/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        mono: ["'JetBrains Mono'", "monospace"],
      },
      colors: {
        surface: {
          DEFAULT: "#07090f",
          1: "#0c1018",
          2: "#111827",
          3: "#1a2332",
        },
        border: {
          DEFAULT: "#1a2332",
          2: "#243044",
        },
        ink: {
          DEFAULT: "#dde4ef",
          2: "#7a8fa8",
          3: "#3a4f66",
        },
        accent: {
          amber: "#f5a623",
          green: "#22d47a",
          red: "#f04545",
          blue: "#4fa8f7",
          purple: "#a78bfa",
        },
      },
      animation: {
        pulse2: "pulse2 2s cubic-bezier(0.4,0,0.6,1) infinite",
      },
      keyframes: {
        pulse2: {
          "0%,100%": { opacity: "1" },
          "50%": { opacity: ".4" },
        },
      },
    },
  },
  plugins: [],
};

export default config;
