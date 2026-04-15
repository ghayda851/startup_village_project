import React from 'react';
import { format } from 'date-fns';

interface PageHeaderProps {
  title: string;
  subtitle?: string;
  children?: React.ReactNode;
  lastRefresh?: string;
}

export const PageHeader: React.FC<PageHeaderProps> = ({
  title,
  subtitle,
  children,
  lastRefresh,
}) => {
  return (
    <div className="mb-8">
      <div className="mb-2 flex items-start justify-between">
        <div>
          <h1 className="text-4xl font-bold text-slate-900">{title}</h1>
          {subtitle && <p className="text-slate-600">{subtitle}</p>}
        </div>
        {children}
      </div>

      {lastRefresh && (
        <p className="text-xs text-slate-500">
          Last updated: {format(new Date(lastRefresh), 'MMM dd, yyyy HH:mm')}
        </p>
      )}
    </div>
  );
};
