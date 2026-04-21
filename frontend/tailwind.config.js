/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      keyframes: {
        shimmer: {
          '0%': { transform: 'translateX(-150%)' },
          '100%': { transform: 'translateX(450%)' },
        },
      },
      animation: {
        shimmer: 'shimmer 1.5s ease-in-out infinite',
      },
      colors: {
        dark: {
          bg: '#1a1a1a',
          'bg-secondary': '#242424',
          'bg-tertiary': '#2e2e2e',
          border: '#3a3a3a',
          text: '#e5e5e5',
          'text-secondary': '#a0a0a0',
        }
      }
    },
  },
  plugins: [],
}
