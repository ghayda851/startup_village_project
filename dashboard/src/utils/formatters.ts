export const formatNumber = (value: number, decimals: number = 0): string => {
  return new Intl.NumberFormat('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(value);
};

export const formatPercentage = (value: number, decimals: number = 2): string => {
  return `${formatNumber(value, decimals)}%`;
};

export const formatCurrency = (value: number, currency: string = 'USD'): string => {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency,
  }).format(value);
};

export const getColorByOccupancy = (
  rate: number
): 'red' | 'amber' | 'green' => {
  if (rate > 100) return 'red';
  if (rate < 40) return 'amber';
  return 'green';
};

export const getColorValue = (color: 'red' | 'amber' | 'green'): string => {
  const colors = {
    red: '#ef4444',
    amber: '#f59e0b',
    green: '#10b981',
  };
  return colors[color];
};
