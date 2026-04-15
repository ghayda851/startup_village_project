# API Migration Guide: Mock Data → Real Backend

## Overview
This guide shows how to replace mock data with real API calls from your FastAPI backend.

---

## Step 1: Configure API Base URL

**File**: `.env`

```env
# Development
VITE_API_BASE_URL=http://localhost:8000

# Production
# VITE_API_BASE_URL=https://api.startup-village.com
```

The app automatically picks this up:
```typescript
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'
```

---

## Step 2: Migrate Space Executive Page

**File**: `src/pages/SpaceExecutivePage.tsx`

### Before (Mock Data)
```typescript
import React from 'react';
import { mockSpaceGlobalKpi, mockSpaceKpiBySize } from '../mocks/data';

const SpaceExecutivePage: React.FC = () => {
  const globalKpi = mockSpaceGlobalKpi;
  const siteKpis = mockSpaceKpiBySize;
  
  // ... rest of component
};
```

### After (Real API)
```typescript
import React from 'react';
import { useSpaceGlobalKpi, useSpaceKpiBySize } from '../hooks/useSpace';
import { ErrorAlert } from '../components/ErrorAlert';

const SpaceExecutivePage: React.FC = () => {
  const { data: globalKpi, isLoading: globalLoading, error: globalError } = useSpaceGlobalKpi();
  const { data: siteKpis = [], isLoading: sitesLoading, error: sitesError } = useSpaceKpiBySize();

  if (globalLoading || sitesLoading) {
    return <LoadingState />;
  }

  if (globalError || sitesError) {
    return <ErrorAlert message="Failed to load space data" />;
  }

  if (!globalKpi) {
    return <ErrorAlert message="No space data available" />;
  }

  // ... rest of component remains the same
};
```

---

## Step 3: Migrate Tickets Executive Page

**File**: `src/pages/TicketsExecutivePage.tsx`

### Before
```typescript
import { mockTicketCard, mockTicketByMonth, mockTicketCategory } from '../mocks/data';

const TicketsExecutivePage: React.FC = () => {
  const ticketCard = mockTicketCard;
  const monthlyData = mockTicketByMonth;
  const categoryData = mockTicketCategory;
  // ...
};
```

### After
```typescript
import { useTicketCards, useTicketsByMonth, useTicketCategory } from '../hooks/useTickets';
import { useFilterStore } from '../store/filterStore';

const TicketsExecutivePage: React.FC = () => {
  const { period } = useFilterStore();

  const { data: ticketCard, isLoading: cardsLoading, error: cardsError } = useTicketCards();
  const { data: monthlyData = [], isLoading: monthLoading } = useTicketsByMonth(period);
  const { data: categoryData = [], isLoading: catLoading } = useTicketCategory(period);

  const isLoading = cardsLoading || monthLoading || catLoading;

  if (isLoading) {
    return <LoadingState />;
  }

  if (cardsError) {
    return <ErrorAlert message="Failed to load ticket data" />;
  }

  if (!ticketCard) {
    return <ErrorAlert message="No ticket data available" />;
  }

  // ... rest of component remains the same
};
```

---

## Step 4: Create Error Alert Component

**File**: `src/components/ErrorAlert.tsx` (Optional but recommended)

```typescript
import React from 'react';
import { AlertCircle } from 'lucide-react';

interface ErrorAlertProps {
  message: string;
  retry?: () => void;
}

export const ErrorAlert: React.FC<ErrorAlertProps> = ({ message, retry }) => {
  return (
    <div className="rounded-lg border border-red-200 bg-red-50 p-6">
      <div className="flex items-start gap-3">
        <AlertCircle className="mt-0.5 text-red-600" size={20} />
        <div className="flex-1">
          <h3 className="font-semibold text-red-900">Error Loading Data</h3>
          <p className="text-sm text-red-700">{message}</p>
          {retry && (
            <button
              onClick={retry}
              className="mt-3 text-sm font-medium text-red-600 hover:text-red-700"
            >
              Try Again
            </button>
          )}
        </div>
      </div>
    </div>
  );
};
```

---

## Step 5: Create Loading State Component

**File**: `src/components/LoadingState.tsx` (Optional)

```typescript
import React from 'react';

export const LoadingState: React.FC = () => {
  return (
    <div className="space-y-6">
      {/* KPI Cards Skeleton */}
      <div className="grid gap-6 md:grid-cols-4">
        {[1, 2, 3, 4].map((i) => (
          <div key={i} className="h-32 animate-pulse rounded-lg bg-slate-200" />
        ))}
      </div>

      {/* Chart Skeletons */}
      <div className="h-80 animate-pulse rounded-lg bg-slate-200" />
      <div className="grid gap-6 md:grid-cols-2">
        {[1, 2].map((i) => (
          <div key={i} className="h-80 animate-pulse rounded-lg bg-slate-200" />
        ))}
      </div>
    </div>
  );
};
```

---

## Step 6: Add React Query DevTools (Optional)

Great for debugging queries in development:

```bash
npm install -D @tanstack/react-query-devtools
```

**Update** `src/main.tsx`:
```typescript
import { QueryClientProvider } from '@tanstack/react-query';
import { ReactQueryDevtools } from '@tanstack/react-query-devtools';

<QueryClientProvider client={queryClient}>
  <App />
  <ReactQueryDevtools initialIsOpen={false} />
</QueryClientProvider>
```

---

## Step 7: Testing the Connection

### Check requests in DevTools
1. Open browser Dev Tools (F12)
2. Go to Network tab
3. Make sure requests are going to your backend
4. Verify response format matches expectations

### Common Issues & Solutions

**Issue**: "Cannot GET /api/space/global"
- **Solution**: Verify `VITE_API_BASE_URL` is correct
- Check backend is running on the specified port
- Ensure endpoint paths match exactly

**Issue**: CORS error in console
- **Solution**: Backend needs CORS configuration
- Add to your FastAPI app:
```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

**Issue**: "Unknown property" errors in browser
- **Solution**: Check response format matches interfaces in `src/types/index.ts`
- Ensure field names match exactly (case-sensitive)
- API should return snake_case (matching DB columns)

---

## Step 8: Implement Global Filters

**File**: `src/pages/TicketsExecutivePage.tsx`

Wire up period filter:

```typescript
const { period, setPeriod } = useFilterStore();
const monthlyData = useTicketsByMonth(period).data || [];

// Handle period selection from chart
const handlePeriodChange = (month: string) => {
  setPeriod(month);
};
```

---

## Step 9: Query Invalidation (Advanced)

After mutations, invalidate caches:

```typescript
import { useQueryClient } from '@tanstack/react-query';

const queryClient = useQueryClient();

// After updating data
await queryClient.invalidateQueries({
  queryKey: ['space', 'global']
});
```

---

## Step 10: Parallel Data Loading

Load multiple data sources efficiently:

```typescript
const [
  globalQuery,
  sitesQuery,
  spaceTypeQuery,
  orgTypeQuery,
] = useQueries({
  queries: [
    { queryKey: ['space', 'global'], queryFn: () => spaceApi.getGlobalKpi() },
    { queryKey: ['space', 'by-site'], queryFn: () => spaceApi.getKpiBySize() },
    { queryKey: ['space', 'by-site-space-type'], queryFn: () => spaceApi.getKpiBySpaceType() },
    { queryKey: ['space', 'by-site-org-type'], queryFn: () => spaceApi.getKpiByOrgType() },
  ],
});

const isLoading = [globalQuery, sitesQuery, spaceTypeQuery, orgTypeQuery]
  .some(q => q.isLoading);
```

---

## API Response Format Examples

### Expected Response Format A (with metadata)
```json
{
  "data": {
    "total_spaces": 1250,
    "total_area_m2": 45000,
    "total_capacity": 5500,
    "total_employees": 4200
  },
  "last_refresh": "2024-03-30T22:00:00Z"
}
```

### Expected Response Format B (raw data)
```json
{
  "total_spaces": 1250,
  "total_area_m2": 45000,
  "total_capacity": 5500,
  "total_employees": 4200
}
```

The frontend handles both automatically!

---

## Complete Example: Space Page Migration

Here's the full migrated component:

```typescript
import React from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, 
         ResponsiveContainer, Cell } from 'recharts';
import { Home, Users, SquareM } from 'lucide-react';
import { PageHeader } from '../components/PageHeader';
import { KpiCard } from '../components/KpiCard';
import { ChartCard } from '../components/ChartCard';
import { ErrorAlert } from '../components/ErrorAlert';
import { LoadingState } from '../components/LoadingState';
import { useSpaceGlobalKpi, useSpaceKpiBySize } from '../hooks/useSpace';
import { formatNumber, formatPercentage } from '../utils/formatters';

const SpaceExecutivePage: React.FC = () => {
  const { 
    data: globalKpi, 
    isLoading: globalLoading, 
    error: globalError,
    refetch: refetchGlobal 
  } = useSpaceGlobalKpi();

  const { 
    data: siteKpis = [], 
    isLoading: sitesLoading,
    error: sitesError,
    refetch: refetchSites
  } = useSpaceKpiBySize();

  const isLoading = globalLoading || sitesLoading;
  const error = globalError || sitesError;

  if (isLoading) {
    return <LoadingState />;
  }

  if (error) {
    return (
      <ErrorAlert 
        message={error?.message || 'Failed to load space data'}
        retry={() => {
          refetchGlobal();
          refetchSites();
        }}
      />
    );
  }

  if (!globalKpi) {
    return <ErrorAlert message="No space data available" />;
  }

  const getOccupancyColor = (rate: number): string => {
    if (rate > 100) return '#ef4444';
    if (rate < 40) return '#f59e0b';
    return '#10b981';
  };

  const chartData = siteKpis.map((site) => ({
    name: site.site,
    occupancy_rate_pct: site.occupancy_rate_pct,
    color: getOccupancyColor(site.occupancy_rate_pct),
  }));

  return (
    <div className="space-y-8">
      <PageHeader
        title="Space Executive Overview"
        subtitle="Real-time insights into your workspace utilization"
        lastRefresh={globalKpi.as_of_ts}
      />

      {/* Rest of component remains exactly the same */}
      {/* KPI Cards, Charts, etc. */}
    </div>
  );
};

export default SpaceExecutivePage;
```

---

## Verification Checklist

- [ ] `.env` file configured with backend URL
- [ ] Development server running (`npm run dev`)
- [ ] Backend server running and accessible
- [ ] No CORS errors in browser console
- [ ] Network tab shows requests to correct endpoints
- [ ] Response data matches TypeScript interfaces
- [ ] No "Cannot find module" errors
- [ ] Components render without blank/missing data
- [ ] Loading states appear briefly
- [ ] Error states display properly

---

## Performance Tips

1. **Selective Re-fetching**: Only refetch when necessary
   ```typescript
   refetch(); // Only when user explicitly requests
   ```

2. **Disable Auto-Polling**: Reduce server load
   ```typescript
   const { data } = useSpaceGlobalKpi({
     refetchInterval: false, // Don't auto-refresh
   });
   ```

3. **Stale While Revalidate**: Show cached data while fetching
   ```typescript
   const { data } = useSpaceGlobalKpi({
     staleTime: 5 * 60 * 1000, // 5 minutes
   });
   ```

---

## Next Steps

After successful migration:
1. Add pagination to table views
2. Implement advanced filtering
3. Add data export functionality
4. Set up real-time updates
5. Deploy to production

---

Good luck with your integration! 🚀
