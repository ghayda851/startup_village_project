import React from 'react';
import { TrendingUp, TrendingDown } from 'lucide-react';
import { cn } from '../utils/cn';

interface KpiCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  trend?: number; // percentage
  icon?: React.ReactNode;
  variant?: 'default' | 'success' | 'warning' | 'danger';
  loading?: boolean;
}

const variantStyles = {
  default: 'bg-slate-50 border-slate-200',
  success: 'bg-green-50 border-green-200',
  warning: 'bg-amber-50 border-amber-200',
  danger: 'bg-red-50 border-red-200',
};

const variantTextStyles = {
  default: 'text-slate-900',
  success: 'text-green-900',
  warning: 'text-amber-900',
  danger: 'text-red-900',
};

const variantTrendStyles = {
  default: 'text-slate-600',
  success: 'text-green-600',
  warning: 'text-amber-600',
  danger: 'text-red-600',
};

export const KpiCard: React.FC<KpiCardProps> = ({
  title,
  value,
  subtitle,
  trend,
  icon,
  variant = 'default',
  loading = false,
}) => {
  const showTrend = trend !== undefined && trend !== null;

  return (
    <div
      className={cn(
        'flex flex-col rounded-lg border p-6 transition-all hover:shadow-md',
        variantStyles[variant]
      )}
    >
      <div className="mb-2 flex items-start justify-between">
        <h3 className="text-sm font-medium text-slate-600">{title}</h3>
        {icon && <div className="text-xl text-slate-400">{icon}</div>}
      </div>

      {loading ? (
        <div className="h-8 w-24 animate-pulse rounded bg-slate-200" />
      ) : (
        <p className={cn('text-2xl font-bold', variantTextStyles[variant])}>{value}</p>
      )}

      {subtitle && (
        <p className="mt-1 text-xs text-slate-500">{subtitle}</p>
      )}

      {showTrend && !loading && (
        <div className={cn('mt-3 flex items-center gap-1 text-sm', variantTrendStyles[variant])}>
          {trend >= 0 ? (
            <>
              <TrendingUp size={16} />
              <span>{Math.abs(trend)}%</span>
            </>
          ) : (
            <>
              <TrendingDown size={16} />
              <span>{Math.abs(trend)}%</span>
            </>
          )}
        </div>
      )}
    </div>
  );
};
