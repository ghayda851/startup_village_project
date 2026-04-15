import React from 'react';
import { X } from 'lucide-react';
import { cn } from '../utils/cn';

interface FilterBarProps {
  filters: Record<string, string | null>;
  onFilterChange: (key: string, value: string | null) => void;
  onReset: () => void;
  children?: React.ReactNode;
}

export const FilterBar: React.FC<FilterBarProps> = ({
  filters,
  onFilterChange,
  onReset,
  children,
}) => {
  const activeFilters = Object.entries(filters).filter(([, value]) => value !== null);
  const hasActiveFilters = activeFilters.length > 0;

  return (
    <div className="mb-6 flex flex-col gap-4 rounded-lg border border-slate-200 bg-slate-50 p-4">
      <div className="flex items-center gap-3">
        <span className="text-sm font-medium text-slate-700">Filters:</span>
        {children}
        {hasActiveFilters && (
          <button
            onClick={onReset}
            className="ml-auto flex items-center gap-2 text-sm text-sky-600 hover:text-sky-700"
          >
            <X size={16} />
            Clear all
          </button>
        )}
      </div>

      {hasActiveFilters && (
        <div className="flex flex-wrap gap-2">
          {activeFilters.map(([key, value]) => (
            <button
              key={key}
              onClick={() => onFilterChange(key, null)}
              className={cn(
                'inline-flex items-center gap-2 rounded-full px-3 py-1 text-sm',
                'bg-sky-100 text-sky-700 hover:bg-sky-200'
              )}
            >
              <span>
                {key}: {value}
              </span>
              <X size={14} />
            </button>
          ))}
        </div>
      )}
    </div>
  );
};
