import React from 'react';
import { cn } from '../utils/cn';

interface ChartCardProps {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
  loading?: boolean;
  className?: string;
}

export const ChartCard: React.FC<ChartCardProps> = ({
  title,
  subtitle,
  children,
  loading = false,
  className = '',
}) => {
  return (
    <div
      className={cn(
        'flex flex-col rounded-lg border border-slate-200 bg-white p-6 shadow-sm transition-all hover:shadow-md',
        className
      )}
    >
      <div className="mb-4">
        <h3 className="text-lg font-semibold text-slate-900">{title}</h3>
        {subtitle && <p className="text-sm text-slate-500">{subtitle}</p>}
      </div>

      {loading ? (
        <div className="flex h-64 items-center justify-center">
          <div className="h-12 w-12 animate-spin rounded-full border-4 border-slate-200 border-t-sky-500" />
        </div>
      ) : (
        <div className="flex-1">{children}</div>
      )}
    </div>
  );
};
