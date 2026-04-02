import { create } from 'zustand';
import { devtools, persist, createJSONStorage } from 'zustand/middleware';

interface AnalysisResult {
  id: string;
  type: string;
  data: Record<string, unknown>;
  timestamp: Date;
}

interface AnalysisState {
  // Results
  analysisResults: AnalysisResult[];
  isAnalyzing: boolean;

  // Files
  csvFile: File | null;
  shapeFile: File | null;

  // Mode
  dataAnalysisMode: boolean;

  // Actions
  addAnalysisResult: (result: AnalysisResult) => void;
  clearAnalysisResults: () => void;
  setAnalyzing: (analyzing: boolean) => void;
  setCsvFile: (file: File | null) => void;
  setShapeFile: (file: File | null) => void;
  setDataAnalysisMode: (mode: boolean) => void;
}

export const useAnalysisStore = create<AnalysisState>()(
  devtools(
    persist(
      (set) => ({
        analysisResults: [],
        isAnalyzing: false,
        csvFile: null,
        shapeFile: null,
        dataAnalysisMode: false,

        addAnalysisResult: (result) =>
          set((state) => ({
            analysisResults: [...state.analysisResults, result],
          })),

        clearAnalysisResults: () =>
          set({ analysisResults: [] }),

        setAnalyzing: (analyzing) =>
          set({ isAnalyzing: analyzing }),

        setCsvFile: (file) =>
          set({ csvFile: file }),

        setShapeFile: (file) =>
          set({ shapeFile: file }),

        setDataAnalysisMode: (mode) =>
          set({ dataAnalysisMode: mode }),
      }),
      {
        name: 'analysis-storage',
        storage: createJSONStorage(() => sessionStorage),
        // Only persist the mode flag — files can't be serialized
        partialize: (state) => ({
          dataAnalysisMode: state.dataAnalysisMode,
        }),
      }
    ),
    { name: 'analysis-store' }
  )
);
