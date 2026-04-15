import React, { useState } from 'react';
import {
  BarChart,
  Bar,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  AreaChart,
  Area,
  Legend,
} from 'recharts';
import { AlertCircle, User, Wrench, Shield, Monitor, Droplet, Loader } from 'lucide-react';
import { ChartCard } from '../components/ChartCard';
import { formatNumber, formatPercentage } from '../utils/formatters';
import {
  useTicketCards,
  useTicketsByMonth,
  useTicketsByYear,
  useTicketPriority,
  useTicketCategory,
  useTicketByTechnician,
  useTicketHeatmap,
} from '../hooks/useTickets';

const TicketsExecutivePage: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'global' | 'technician' | 'category' | 'list'>('global');

  // Fetch data from real APIs
  const { data: ticketCard, isLoading: loadingCards } = useTicketCards('ALL');
  const { data: monthlyData, isLoading: loadingMonthly } = useTicketsByMonth('ALL');
  const { data: yearlyData, isLoading: loadingYearly } = useTicketsByYear('ALL');
  const { data: priorityData, isLoading: loadingPriority } = useTicketPriority('ALL');
  const { data: categoryData, isLoading: loadingCategory } = useTicketCategory('ALL');
  const { data: technicianData, isLoading: loadingTechnician } = useTicketByTechnician('ALL');
  const { data: heatmapData } = useTicketHeatmap('ALL');

  const COLORS = ['#0ea5e9', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899'];

  // Process heatmap data to create technician+priority breakdown
  const technicianPriorityData = React.useMemo(() => {
    if (!heatmapData || !Array.isArray(heatmapData)) return [];
    
    const techMap = new Map<string, any>();
    
    heatmapData.forEach((item: any) => {
      const key = item.last_updater_full_name;
      if (!techMap.has(key)) {
        techMap.set(key, {
          technician: key,
          'Très Haute': 0,
          'Haute': 0,
          'Moyenne': 0,
          'Basse': 0,
          'Minimale': 0,
        });
      }
      const tech = techMap.get(key);
      const priority = item.priority || 'Autre';
      if (tech[priority] !== undefined) {
        tech[priority] += item.ticket_count;
      }
    });
    
    return Array.from(techMap.values()).sort((a, b) => {
      const aTotal = Object.entries(a).reduce((sum: number, [key, val]: [string, any]) => {
        return key !== 'technician' && typeof val === 'number' ? sum + val : sum;
      }, 0);
      const bTotal = Object.entries(b).reduce((sum: number, [key, val]: [string, any]) => {
        return key !== 'technician' && typeof val === 'number' ? sum + val : sum;
      }, 0);
      return bTotal - aTotal;
    });
  }, [heatmapData]);

  const getCategoryIcon = (category: string | null) => {
    if (!category) return <Monitor size={16} />;
    const lowerCategory = category.toLowerCase();
    if (lowerCategory.includes('hardware') || lowerCategory.includes('matériel')) return <Wrench size={16} />;
    if (lowerCategory.includes('software') || lowerCategory.includes('logiciel')) return <Monitor size={16} />;
    if (lowerCategory.includes('network') || lowerCategory.includes('réseau')) return <Shield size={16} />;
    if (lowerCategory.includes('facility') || lowerCategory.includes('électricité') || lowerCategory.includes('plomberie')) return <Droplet size={16} />;
    if (lowerCategory.includes('security') || lowerCategory.includes('sécurité')) return <AlertCircle size={16} />;
    return <Monitor size={16} />;
  };

  // Create status distribution data from ticketCard
  const statusData = ticketCard ? [
    { status: 'Ouvert', ticket_count: ticketCard.is_open, fill: '#f59e0b' },
    { status: 'Résolu', ticket_count: ticketCard.is_resolved, fill: '#10b981' },
  ] : [];

  const tabs = [
    { id: 'global', label: 'Vue Globale' },
    { id: 'technician', label: 'Par Technician' },
    { id: 'category', label: 'Par Catégorie' },
    { id: 'list', label: 'Liste Tickets' },
  ];

  // Loading state
  if (loadingCards || loadingMonthly || loadingYearly || loadingPriority || loadingCategory || loadingTechnician) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader size={32} className="text-sky-600 animate-spin" />
      </div>
    );
  }

  if (!ticketCard) {
    return <div className="text-center py-8">Aucune donnée disponible</div>;
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-slate-900">Tickets IT</h1>
        <p className="mt-2 text-slate-600">Suivi des demandes et interventions techniques GLPI</p>
        <p className="mt-1 text-sm text-slate-500">Last Updated: {ticketCard.gold_kpi_build_ts?.split(' ')[0]}</p>
      </div>

      {/* Tab Navigation */}
      <div className="flex gap-4 border-b border-slate-200 overflow-x-auto">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id as any)}
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

      {/* KPI Cards */}
      <div className="grid gap-4 md:grid-cols-6">
        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Total Tickets</p>
          <p className="text-2xl font-bold text-slate-900 mt-2">{formatNumber(ticketCard.total_tickets)}</p>
          <p className="text-xs text-slate-500 mt-2">Tous les temps | GLPI</p>
        </div>

        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Ouverts</p>
          <div className="flex items-baseline gap-2 mt-2">
            <p className="text-2xl font-bold text-slate-900">{formatNumber(ticketCard.is_open)}</p>
            <span className="text-sm text-amber-600">({formatPercentage(ticketCard.open_rate * 100)})</span>
          </div>
          <p className="text-xs text-slate-500 mt-2">Non résolus</p>
        </div>

        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Résolus</p>
          <p className="text-2xl font-bold text-slate-900 mt-2">{formatNumber(ticketCard.is_resolved)}</p>
          <p className="text-xs text-slate-500 mt-2">Tickets fermés</p>
        </div>

        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Taux Ouvert</p>
          <div className="flex items-baseline gap-2 mt-2">
            <p className="text-2xl font-bold text-slate-900">{formatPercentage(ticketCard.open_rate * 100)}</p>
          </div>
          <p className="text-xs text-slate-500 mt-2">% des tickets</p>
        </div>

        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Priorité Très Haute</p>
          <p className="text-2xl font-bold text-red-600 mt-2">
            {priorityData?.find(p => p.priority_code === 5)?.ticket_count || 0}
          </p>
          <p className="text-xs text-slate-500 mt-2">Tickets critiques</p>
        </div>

        <div className="rounded-lg bg-white p-6 border border-slate-200">
          <p className="text-sm text-slate-600">Technicians</p>
          <p className="text-2xl font-bold text-slate-900 mt-2">{formatNumber(ticketCard.technicians)}</p>
          <p className="text-xs text-slate-500 mt-2">Équipe IT active</p>
        </div>
      </div>

      {/* Main Content Based on Tab */}
      {activeTab === 'global' && (
        <>
          {/* Global View Charts */}
          <div className="grid gap-6 md:grid-cols-2">
            <ChartCard title="Distribution par Priorité" subtitle="Données: GLPI | Source: API">
              {priorityData && priorityData.length > 0 ? (
                <ResponsiveContainer width="100%" height={250}>
                  <PieChart>
                    <Pie
                      data={priorityData}
                      cx="50%"
                      cy="50%"
                      labelLine={true}
                      label={({ priority, ticket_count }: any) => `${priority}: ${ticket_count}`}
                      outerRadius={80}
                      dataKey="ticket_count"
                      nameKey="priority"
                    >
                      {priorityData.map((_, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value) => `${value} tickets`} />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <div className="h-64 flex items-center justify-center text-slate-500">Pas de données</div>
              )}
            </ChartCard>

            <ChartCard title="Distribution par Statut" subtitle="Données: GLPI | Source: API">
              {statusData && statusData.length > 0 ? (
                <ResponsiveContainer width="100%" height={250}>
                  <PieChart>
                    <Pie
                      data={statusData}
                      cx="50%"
                      cy="50%"
                      labelLine={true}
                      label={({ status, ticket_count }: any) => `${status}: ${ticket_count}`}
                      outerRadius={80}
                      dataKey="ticket_count"
                      nameKey="status"
                    >
                      {statusData.map((_, index) => (
                        <Cell key={`cell-${index}`} fill={index === 0 ? '#f59e0b' : '#10b981'} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value) => `${value} tickets`} />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <div className="h-64 flex items-center justify-center text-slate-500">Pas de données</div>
              )}
            </ChartCard>
          </div>

          {/* Tickets per Year */}
          {yearlyData && yearlyData.length > 0 && (
            <ChartCard title="Tickets par Année" subtitle="Données: GLPI | Source: API">
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={yearlyData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="year" tick={{ fontSize: 12 }} />
                  <YAxis tick={{ fontSize: 12 }} />
                  <Tooltip />
                  <Bar dataKey="ticket_count" fill="#0ea5e9" radius={[8, 8, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </ChartCard>
          )}

          {/* Monthly Evolution */}
          {monthlyData && monthlyData.length > 0 && (
            <ChartCard title="Évolution des Tickets par Mois" subtitle="Données: GLPI | Source: API">
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={monthlyData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="period" tick={{ fontSize: 10 }} />
                  <YAxis tick={{ fontSize: 12 }} />
                  <Tooltip />
                  <Area
                    type="monotone"
                    dataKey="ticket_count"
                    fill="#0ea5e9"
                    stroke="#0284c7"
                    fillOpacity={0.3}
                    name="Tickets"
                  />
                </AreaChart>
              </ResponsiveContainer>
            </ChartCard>
          )}
        </>
      )}

      {activeTab === 'technician' && (
        <>
          {/* Stacked Bar Chart: Charge par Technician et Priorité */}
          {technicianPriorityData && technicianPriorityData.length > 0 && (
            <ChartCard title="Charge par Technician et Priorité" subtitle="Données: 02/05/2023 - 22/01/2026 | Source: GLPI">
              <ResponsiveContainer width="100%" height={400}>
                <BarChart data={technicianPriorityData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="technician" tick={{ fontSize: 12 }} />
                  <YAxis tick={{ fontSize: 12 }} />
                  <Tooltip 
                    contentStyle={{ backgroundColor: '#fff', border: '1px solid #ccc' }}
                    formatter={(value) => `${value}`}
                  />
                  <Legend />
                  <Bar dataKey="Très Haute" stackId="priority" fill="#ef4444" name="Très Haute" />
                  <Bar dataKey="Haute" stackId="priority" fill="#f59e0b" name="Haute" />
                  <Bar dataKey="Moyenne" stackId="priority" fill="#10b981" name="Moyenne" />
                  <Bar dataKey="Basse" stackId="priority" fill="#0ea5e9" name="Basse" />
                  <Bar dataKey="Minimale" stackId="priority" fill="#8b5cf6" name="Minimale" />
                </BarChart>
              </ResponsiveContainer>
            </ChartCard>
          )}

          {/* Technician Cards */}
          {technicianData && technicianData.length > 0 && (
            <div className="grid gap-4 md:grid-cols-5">
              {technicianData.slice(0, 5).map((tech) => (
                <div key={tech.last_updater_user_id} className="rounded-lg bg-white p-6 border border-slate-200">
                  <div className="flex items-center gap-2 mb-3">
                    <User size={16} className="text-sky-600" />
                    <p className="font-semibold text-slate-900 text-sm">{tech.last_updater_full_name}</p>
                  </div>
                  <p className="text-2xl font-bold text-slate-900">{formatNumber(tech.ticket_count)}</p>
                  <p className="text-xs text-slate-500">{formatPercentage((tech.ticket_count / ticketCard.total_tickets) * 100)} du total</p>
                </div>
              ))}
            </div>
          )}
        </>
      )}

      {activeTab === 'category' && (
        <>
          {/* Category View */}
          {categoryData && categoryData.length > 0 && (
            <div className="grid gap-6 md:grid-cols-2">
              <ChartCard title="Tickets par Catégorie" subtitle="Données: GLPI | Source: API">
                <ResponsiveContainer width="100%" height={400}>
                  <BarChart data={categoryData.sort((a, b) => b.ticket_count - a.ticket_count)} layout="vertical">
                    <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                    <XAxis type="number" tick={{ fontSize: 12 }} />
                    <YAxis dataKey="category" type="category" tick={{ fontSize: 10 }} width={150} />
                    <Tooltip />
                    <Bar dataKey="ticket_count" fill="#0ea5e9" radius={[0, 8, 8, 0]}>
                      {categoryData.map((_, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </ChartCard>

              <ChartCard title="Répartition par Catégorie" subtitle="Données: GLPI | Source: API">
                <ResponsiveContainer width="100%" height={400}>
                  <PieChart>
                    <Pie
                      data={categoryData}
                      dataKey="ticket_count"
                      nameKey="category"
                      cx="50%"
                      cy="50%"
                      outerRadius={80}
                      label={({ category, share }: any) => share ? `${category}: ${(share * 100).toFixed(1)}%` : ''}
                    >
                      {categoryData.map((_, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value) => `${value} tickets`} />
                  </PieChart>
                </ResponsiveContainer>
              </ChartCard>
            </div>
          )}

          {/* Category Cards */}
          {categoryData && categoryData.length > 0 && (
            <div className="grid gap-4 md:grid-cols-4">
              {categoryData.slice(0, 4).map((cat, index) => (
                <div key={`${cat.category_id}-${index}`} className="rounded-lg bg-white p-6 border border-slate-200">
                  <div className="flex items-center gap-2 mb-3">
                    <div className="p-2 rounded bg-sky-100 text-sky-600">
                      {getCategoryIcon(cat.category)}
                    </div>
                    <p className="font-semibold text-slate-900 text-sm">{cat.category || 'Non catégorisé'}</p>
                  </div>
                  <p className="text-2xl font-bold text-slate-900">{formatNumber(cat.ticket_count)}</p>
                  <p className="text-xs text-slate-500">{formatPercentage(cat.share * 100)} du total</p>
                </div>
              ))}
            </div>
          )}
        </>
      )}

      {activeTab === 'list' && (
        <ChartCard title="Liste Tickets">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="px-4 py-2 text-left font-semibold text-slate-900">Catégorie</th>
                  <th className="px-4 py-2 text-left font-semibold text-slate-900">Count</th>
                  <th className="px-4 py-2 text-left font-semibold text-slate-900">% du total</th>
                  <th className="px-4 py-2 text-left font-semibold text-slate-900">Share</th>
                </tr>
              </thead>
              <tbody>
                {categoryData?.slice(0, 10).map((cat) => (
                  <tr key={cat.category_id} className="border-b border-slate-100 hover:bg-slate-50">
                    <td className="px-4 py-3">{cat.category || 'Non catégorisé'}</td>
                    <td className="px-4 py-3">{cat.ticket_count}</td>
                    <td className="px-4 py-3">{formatPercentage(cat.share * 100)}%</td>
                    <td className="px-4 py-3">
                      <div className="w-24 bg-slate-200 rounded-full h-2">
                        <div
                          className="bg-sky-600 h-2 rounded-full"
                          style={{ width: `${cat.share * 100}%` }}
                        ></div>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </ChartCard>
      )}
    </div>
  );
};

export default TicketsExecutivePage;
