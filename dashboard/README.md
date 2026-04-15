# Startup Village Dashboard

A production-ready React dashboard for monitoring workspace utilization and ticket management across a startup ecosystem. Built with modern best practices using Vite, TypeScript, TailwindCSS, and React Query.

## 🎯 Features

### Space Dashboard
- **Executive Overview**: Real-time KPIs for total spaces, area, capacity, and occupancy rates
- **Utilization by Site**: Bar charts with color-coded occupancy levels (red >100%, amber <40%, green normal)
- **Under/Over Utilization Analysis**: Identify under-utilized and overcrowded spaces
- **Space Efficiency Metrics**: Density, per-employee metrics, and area distribution

### Tickets Dashboard
- **Executive Overview**: Ticket volume, open/resolved counts, technician stats
- **Monthly Trend**: Line chart showing ticket trends over time
- **Category Breakdown**: Distribution of tickets by category with top 10 aggregation
- **Technician Workload**: Leaderboard and heatmap of technician assignment and priority distribution

## 🧱 Tech Stack

- **Frontend Framework**: React 18 with TypeScript
- **Build Tool**: Vite
- **Styling**: TailwindCSS
- **State Management**: Zustand (global filters) + React Query (server state)
- **API Client**: Axios with interceptors and error handling
- **Charts**: Recharts
- **Tables**: TanStack Table (prepared for data grid/table views)
- **Icons**: Lucide React
- **Utilities**: date-fns for date formatting, classnames for conditional styling

## 📂 Project Structure

```
src/
├── components/          # Reusable UI components
│   ├── KpiCard.tsx     # KPI metric card component
│   ├── ChartCard.tsx   # Chart container component
│   ├── PageHeader.tsx  # Page title and metadata
│   └── FilterBar.tsx   # Filter controls
├── pages/              # Page components
│   ├── SpaceExecutivePage.tsx
│   └── TicketsExecutivePage.tsx
├── hooks/              # React Query hooks
│   ├── useSpace.ts
│   └── useTickets.ts
├── services/           # API communication
│   ├── api.ts          # Axios instance with interceptors
│   └── endpoints.ts    # API endpoint functions
├── store/              # Zustand stores
│   └── filterStore.ts
├── types/              # TypeScript interfaces
│   └── index.ts
├── utils/              # Utility functions
│   ├── cn.ts           # Classname helper
│   └── formatters.ts   # Number/date formatting
├── constants/          # App constants
│   └── index.ts
├── mocks/              # Mock data for development
│   └── data.ts
├── App.tsx             # Main app component
└── main.tsx            # Entry point
```

## 🚀 Getting Started

### Installation

```bash
npm install
```

### Environment Setup

Create a `.env` file in the root:

```env
VITE_API_BASE_URL=http://localhost:8000
```

### Development

```bash
npm run dev
```

This starts the Vite dev server at `http://localhost:5173`.

### Build

```bash
npm run build
```

### Preview

```bash
npm run preview
```

## 🔌 API Integration

The dashboard connects to a FastAPI backend that serves data from PostgreSQL. All API calls are made through a centralized Axios instance:

### Base Configuration

- **Base URL**: Controlled via `VITE_API_BASE_URL` environment variable
- **Timeout**: 30 seconds
- **Request/Response Logging**: Enabled in development mode

### API Endpoints

#### Space Endpoints
- `GET /api/space/global` - Global KPI metrics
- `GET /api/space/by-site` - KPIs aggregated by site
- `GET /api/space/by-site-space-type` - Space type distribution
- `GET /api/space/by-site-org-type` - Organization type distribution
- `GET /api/space/rooms` - Room-level details (supports filters)

#### Tickets Endpoints
- `GET /api/tickets/cards` - Summary KPI cards
- `GET /api/tickets/by-month` - Monthly trend data
- `GET /api/tickets/by-year` - Yearly summary
- `GET /api/tickets/priority` - Priority distribution (filtered by period)
- `GET /api/tickets/category` - Category breakdown
- `GET /api/tickets/location` - Location distribution
- `GET /api/tickets/by-technician` - Technician workload
- `GET /api/tickets/heatmap` - Technician × Priority matrix
- `GET /api/tickets/list` - Detailed ticket listings with pagination

### Response Format

APIs support both formats:

```typescript
// Format A
{ data: T, last_refresh?: string }

// Format B
raw T | T[]
```

The frontend automatically handles both gracefully.

### Query Parameters

**Period Filter** (Tickets):
- Values: `"ALL"` or `"YYYY-MM"`
- Affects: priority, category, location, by-technician, heatmap endpoints

**Site Filter** (Space):
- Values: Match database site names
- Affects: room queries and drill-down views

## 📊 Data Types

All data types are defined in `src/types/index.ts`. Key types include:

- `SpaceKpiGlobal` - Global space metrics
- `SpaceKpiBySite` - Per-site aggregations
- `SpaceRoom` - Individual room details
- `TicketCard` - Summary KPIs
- `TicketByMonth` - Time-series data
- `TicketPriority`, `TicketCategory`, etc. - Breakdown analytics

## 🎨 Component Overview

### KpiCard
Displays a single metric with optional trending indicators.

```tsx
<KpiCard
  title="Total Spaces"
  value={formatNumber(1250)}
  icon={<Home />}
  variant="success"
/>
```

### ChartCard
Container for charts and visualizations.

```tsx
<ChartCard title="Occupancy by Site">
  <ResponsiveContainer>
    <BarChart data={data}>
      {/* Recharts configuration */}
    </BarChart>
  </ResponsiveContainer>
</ChartCard>
```

### PageHeader
Page title and metadata display.

```tsx
<PageHeader
  title="Space Executive Overview"
  lastRefresh={globalKpi.as_of_ts}
/>
```

### FilterBar
Global filter controls with active filter display.

```tsx
<FilterBar
  filters={filters}
  onFilterChange={handleFilterChange}
  onReset={handleReset}
>
  {/* Filter controls */}
</FilterBar>
```

## 🔄 State Management

### Global Filters (Zustand)

```typescript
import { useFilterStore } from './store/filterStore'

const { period, site, setPeriod, setSite } = useFilterStore()
```

### Server State (React Query)

```typescript
import { useSpaceGlobalKpi } from './hooks/useSpace'

const { data, isLoading, error } = useSpaceGlobalKpi()
```

## 📈 Mock Data

Development uses mock data from `src/mocks/data.ts`. To use real API:

1. Replace mock data imports with actual hooks
2. Update components to handle loading/error states
3. Configure real API endpoint in `.env`

Example:

```typescript
// Before (mock)
const globalKpi = mockSpaceGlobalKpi

// After (real API)
const { data: globalKpi, isLoading } = useSpaceGlobalKpi()
```

## 🎯 Color Coding

### Occupancy Rate
- **Red** (`#ef4444`): >100% occupied
- **Amber** (`#f59e0b`): <40% occupied
- **Green** (`#10b981`): 40-100% occupied

## 🌐 Responsive Design

- Mobile-first approach
- Breakpoints: sm (640px), md (768px), lg (1024px), xl (1280px)
- TailwindCSS grid system with responsive cols configuration

## 📝 Development Notes

### Adding New Pages

1. Create component in `src/pages/`
2. Add navigation item in `App.tsx`
3. Import and render in main app

### Adding New API Endpoints

1. Define type in `src/types/index.ts`
2. Add function to `src/services/endpoints.ts`
3. Create hook in `src/hooks/useTickets.ts` or `useSpace.ts`
4. Use hook in components

### Error Handling

All API errors are normalized in the Axios interceptor. Errors include:
- HTTP error responses
- Network timeouts
- Parsing errors

## 🚀 Performance Optimization

- **Query caching**: 5-minute stale time, 30-minute garbage collection
- **Code splitting**: Vite automatic chunk splitting
- **Image optimization**: External assets loaded on demand
- **Component memoization**: React.memo for expensive components

## 📚 Dependencies

See `package.json` for full dependency list. Key packages:

```json
{
  "@tanstack/react-query": "^5.0.0",
  "@tanstack/react-table": "^8.0.0",
  "axios": "^1.6.0",
  "recharts": "^2.10.0",
  "zustand": "^4.4.0",
  "tailwindcss": "^3.4.0",
  "typescript": "^5.2.0",
  "vite": "^5.0.0"
}
```

## 🔐 Security

- CORS configuration handled at backend
- API base URL configured via environment variables
- No sensitive data stored in localStorage by default
- TypeScript strict mode enabled

## 📞 Support

For issues or questions:
1. Check the project structure and existing examples
2. Review the types in `src/types/index.ts`
3. Check API response handling in `src/services/endpoints.ts`

## 📄 License

Copyright © 2024 Startup Village. All rights reserved.

import reactDom from 'eslint-plugin-react-dom'

export default defineConfig([
  globalIgnores(['dist']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      // Other configs...
      // Enable lint rules for React
      reactX.configs['recommended-typescript'],
      // Enable lint rules for React DOM
      reactDom.configs.recommended,
    ],
    languageOptions: {
      parserOptions: {
        project: ['./tsconfig.node.json', './tsconfig.app.json'],
        tsconfigRootDir: import.meta.dirname,
      },
      // other options...
    },
  },
])
```
