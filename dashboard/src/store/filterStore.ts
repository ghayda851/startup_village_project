import { create } from 'zustand';
import type { FilterState } from '../types';

interface FilterStore extends FilterState {
  setPeriod: (period: FilterState['period']) => void;
  setSite: (site: FilterState['site']) => void;
  resetFilters: () => void;
}

const initialState: FilterState = {
  period: 'ALL',
  site: null,
};

export const useFilterStore = create<FilterStore>((set) => ({
  ...initialState,

  setPeriod: (period) => set({ period }),

  setSite: (site) => set({ site }),

  resetFilters: () => set(initialState),
}));
