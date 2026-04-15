import React, { useState } from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts';
import { Loader } from 'lucide-react';
import { ChartCard } from '../components/ChartCard';
import { formatNumber, formatPercentage } from '../utils/formatters';
import {
  useSpaceGlobalKpi,
  useSpaceKpiBySize,
  useSpaceKpiBySpaceType,
  useSpaceKpiByOrgType,
} from '../hooks/useSpace';

const SpaceExecutivePage: React.FC = () => {
  const [activeTab, setActiveTab] = useState<string>('global');
  
  // Fetch data from APIs
  const { data: globalKpi, isLoading: loadingGlobal, error: errorGlobal } = useSpaceGlobalKpi();
  const { data: siteKpis, isLoading: loadingSites, error: errorSites } = useSpaceKpiBySize();
  
  // Debug logging
  React.useEffect(() => {
    console.log('Space Global Data:', { globalKpi, loadingGlobal, errorGlobal });
    console.log('Space Site Data:', { siteKpis, loadingSites, errorSites });
  }, [globalKpi, loadingGlobal, errorGlobal, siteKpis, loadingSites, errorSites]);
  
  // Only fetch site-specific data when not on global tab
  const selectedSite = activeTab === 'global' ? '' : activeTab;
  
  const { data: spaceTypeData, isLoading: loadingSpaceType } = useSpaceKpiBySpaceType(selectedSite);
  const { data: orgTypeData, isLoading: loadingOrgType } = useSpaceKpiByOrgType(selectedSite);

  if (loadingGlobal || loadingSites) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader size={32} className="text-sky-600 animate-spin" />
      </div>
    );
  }

  if (errorGlobal || errorSites) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6 text-red-700">
        <p>Erreur lors du chargement des données. Veuillez réessayer.</p>
        {errorGlobal && <p className="text-sm mt-2">Global Error: {String(errorGlobal)}</p>}
        {errorSites && <p className="text-sm mt-2">Sites Error: {String(errorSites)}</p>}
      </div>
    );
  }

  if (!globalKpi || !siteKpis) {
    return <div className="text-center py-8">Aucune donnée disponible</div>;
  }

  // Prepare comparison data from sites
  const comparisonData = siteKpis.map(site => ({
    name: site.site,
    employees: site.total_employees,
    surface: Math.round(site.total_area_m2 / 100),
    capacity: site.total_capacity,
  }));

  // Prepare space type data for selected site
  const selectedSpaceTypeData = spaceTypeData?.map(item => ({
    type: item.space_type,
    area: item.sum_area_m2,
    capacity: item.sum_capacity,
    employees: item.sum_employees,
  })) || [];

  // Prepare org type data for selected site
  const selectedOrgTypeData = orgTypeData?.map(item => ({
    org: item.organization_type,
    employees: item.sum_employees,
    spaces: item.spaces_count,
    area: item.sum_area_m2,
  })) || [];

  // Occupancy status breakdown for comparison
  const occupancyData = siteKpis.map(site => ({
    name: site.site,
    loue: site.leased_spaces,
    nonLoue: site.non_leased_spaces,
  }));

  // Build tabs: always include Global, plus one tab per site
  const tabs = [
    { id: 'global', label: 'Vue Globale' },
    ...siteKpis.map(site => ({
      id: site.site,
      label: `${site.site.split(' ').pop()?.charAt(0).toUpperCase()}${site.site.split(' ').pop()?.slice(1).toLowerCase()} (${site.total_spaces} espaces)`,
    })),
  ];

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold text-slate-900">Space Distribution</h1>
        <p className="mt-2 text-slate-600">Répartition et analyse des espaces - Startup Village</p>
        <p className="mt-1 text-sm text-slate-500">Last Updated: {globalKpi.as_of_ts?.split(' ')[0]}</p>
      </div>

      {/* Tabs */}
      <div className="flex gap-4 border-b border-slate-200 overflow-x-auto">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`px-4 py-3 font-medium text-sm border-b-2 transition-all whitespace-nowrap ${
              activeTab === tab.id
                ? 'border-sky-600 text-sky-600'
                : 'border-transparent text-slate-600 hover:text-slate-900'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* KPI Cards Grid - Context-aware based on active tab */}
      <div className="grid gap-4 md:grid-cols-4">
        {activeTab === 'global' && (
          <div className="rounded-lg bg-white p-6 border border-slate-200">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-600">Nombre de Villages</p>
                <p className="text-2xl font-bold text-slate-900 mt-2">{globalKpi.villages_count}</p>
                <p className="text-xs text-slate-500 mt-2">Répartition Espace</p>
              </div>
              <div className="text-sky-600"></div>
            </div>
          </div>
        )}
        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Total Espaces</p>
          <p className="text-2xl font-bold text-slate-900 mt-2">{formatNumber(globalKpi.total_spaces)}</p>
          <p className="text-xs text-slate-500 mt-2">nombre des espaces bruts</p>
        </div>

        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Surface Totale</p>
          <p className="text-2xl font-bold text-slate-900 mt-2">{formatNumber(globalKpi.total_area_m2)} m²</p>
          <p className="text-xs text-slate-500 mt-2">surface brute</p>
        </div>

        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Capacité Totale</p>
          <p className="text-2xl font-bold text-slate-900 mt-2">{formatNumber(globalKpi.total_capacity)}</p>
          <p className="text-xs text-slate-500 mt-2">postes de travail</p>
        </div>

        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Employés Actuels</p>
          <p className="text-2xl font-bold text-slate-900 mt-2">{formatNumber(globalKpi.total_employees)}</p>
          <p className="text-xs text-slate-500 mt-2">{activeTab === 'global' ? 'villageois actifs' : 'actifs'}</p>
        </div>
      </div>

      {/* Second Row of KPIs */}
      <div className="grid gap-4 md:grid-cols-4">
        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Espaces Loués</p>
          <p className="text-2xl font-bold text-slate-900 mt-2">{globalKpi.leased_spaces}</p>
          <p className="text-xs text-slate-500 mt-2">{globalKpi.non_leased_spaces} non loués</p>
        </div>

        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Taux d'Occupation</p>
          <p className="text-2xl font-bold text-slate-900 mt-2">{formatPercentage(globalKpi.weighted_occupancy_rate_pct)}</p>
          <p className="text-xs text-slate-500 mt-2">moyenne pondérée</p>
        </div>

        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Densité Moyenne</p>
          <p className="text-2xl font-bold text-slate-900 mt-2">{formatNumber(globalKpi.avg_density_emp_per_100m2)}</p>
          <p className="text-xs text-slate-500 mt-2">employés / 100m²</p>
        </div>

        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Disparités Occupation</p>
          <p className="text-2xl font-bold text-slate-900 mt-2">{globalKpi.underutilized_spaces_lt_40pct} / {globalKpi.overoccupied_spaces_gt_100pct}</p>
          <p className="text-xs text-slate-500 mt-2">sous-util / sur-occup</p>
        </div>
      </div>

      {/* Comparison Charts - Context-aware based on selected tab */}
      {activeTab === 'global' ? (
        <div className="grid gap-6 md:grid-cols-2">
          <ChartCard title="Comparaison des Villages - Métriques Clés" subtitle="Surface, Employés et Capacité par Village">
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={comparisonData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip />
                <Legend />
                <Bar dataKey="employees" fill="#10b981" />
                <Bar dataKey="surface" fill="#f59e0b" />
                <Bar dataKey="capacity" fill="#0ea5e9" />
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard title="Comparaison des Villages - Statut Location" subtitle="Espaces loués vs non loués par Village">
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={occupancyData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip />
                <Legend />
                <Bar dataKey="loue" fill="#10b981" name="Loué" />
                <Bar dataKey="nonLoue" fill="#ef4444" name="Non Loué" />
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>
        </div>
      ) : (
        <div className="grid gap-6 md:grid-cols-2">
          <ChartCard title="Surface par Type d'Espace" subtitle={`Répartition pour ${activeTab}`}>
            {loadingSpaceType ? (
              <div className="h-64 flex items-center justify-center text-slate-500">
                <Loader size={24} className="animate-spin" />
              </div>
            ) : selectedSpaceTypeData.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={selectedSpaceTypeData} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis type="number" tick={{ fontSize: 12 }} />
                  <YAxis dataKey="type" type="category" tick={{ fontSize: 12 }} width={150} />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="area" fill="#0ea5e9" name="Surface (m²)" />
                  <Bar dataKey="capacity" fill="#10b981" name="Capacité" />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-64 flex items-center justify-center text-slate-500">Pas de données</div>
            )}
          </ChartCard>

          <ChartCard title="Distribution par Type d'Organisation" subtitle={`Répartition pour ${activeTab}`}>
            {loadingOrgType ? (
              <div className="h-64 flex items-center justify-center text-slate-500">
                <Loader size={24} className="animate-spin" />
              </div>
            ) : selectedOrgTypeData.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={selectedOrgTypeData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="org" tick={{ fontSize: 12 }} />
                  <YAxis tick={{ fontSize: 12 }} />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="employees" fill="#0ea5e9" name="Employés" />
                  <Bar dataKey="spaces" fill="#10b981" name="Espaces" />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-64 flex items-center justify-center text-slate-500">Pas de données</div>
            )}
          </ChartCard>
        </div>
      )}
    </div>
  );
};

export default SpaceExecutivePage;
