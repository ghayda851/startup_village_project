import React, { useState, useMemo } from 'react';
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts';
import { Loader, AlertCircle } from 'lucide-react';
import { ChartCard } from '../components/ChartCard';
import { formatNumber } from '../utils/formatters';
import {
  useReservationGlobalKpi,
  useReservationCurrent,
  useReservationByItem,
  useReservationByUser,
  useReservationTrendsByMonth,
  useReservationPeakPeriods,
  useReservationDurationDistribution,
} from '../hooks/useReservations';

const ReservationExecutivePage: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'global' | 'items' | 'users'>('global');

  // Fetch all data
  const { data: globalKpi, isLoading: loadingGlobal, error: errorGlobal } = useReservationGlobalKpi();
  const { data: currentReservations, isLoading: loadingCurrent } = useReservationCurrent(500);
  const { data: byItem, isLoading: loadingByItem } = useReservationByItem(500);
  const { data: byUser, isLoading: loadingByUser } = useReservationByUser(10);
  const { data: trendsByMonth, isLoading: loadingTrends } = useReservationTrendsByMonth();
  const { data: peakPeriods, isLoading: loadingPeaks } = useReservationPeakPeriods();
  const { data: durationDistribution, isLoading: loadingDuration } = useReservationDurationDistribution();

  const isLoading = loadingGlobal || loadingCurrent || loadingByItem || loadingByUser || loadingTrends || loadingPeaks || loadingDuration;

  // Calculate derived metrics
  const peakPeriod = useMemo(() => {
    return peakPeriods?.[0]?.start_hour_2h_window || 'N/A';
  }, [peakPeriods]);

  // Over-booked rooms: high reservation count (bottlenecks)
  const overBookedRooms = useMemo(() => {
    if (!byItem || !currentReservations) return [];
    
    const roomMetrics = new Map<number, any>();
    
    currentReservations.forEach(res => {
      const key = res.reservation_item_id;
      if (!roomMetrics.has(key)) {
        roomMetrics.set(key, {
          itemId: res.reservation_item_id,
          itemName: res.reservation_item_name,
          totalReservations: 0,
          totalHours: 0,
          durations: [] as number[],
          hoursValues: [] as number[],
        });
      }
      const room = roomMetrics.get(key);
      room.totalReservations += 1;
      room.totalHours += res.duration_hours;
      room.durations.push(res.duration_hours);
    });

    // Calculate averages and identify high-pressure rooms
    const processed = Array.from(roomMetrics.values())
      .map(room => ({
        itemId: room.itemId,
        name: room.itemName,
        occupancy: Math.min(100, Math.round((room.totalReservations / 10) * 100)), // normalized to max 100%
        avgDuration: (room.totalHours / room.totalReservations).toFixed(1),
        reservationCount: room.totalReservations,
        pressure: room.totalReservations > 15 ? 'Critical' : room.totalReservations > 10 ? 'High' : 'Medium',
      }))
      .sort((a, b) => b.reservationCount - a.reservationCount)
      .slice(0, 10);

    return processed;
  }, [byItem, currentReservations]);

  // Underused assets: low reservation count
  const underusedAssets = useMemo(() => {
    if (!byItem) return [];
    
    return byItem
      .filter(item => item.reservations_count < 5) // Low usage threshold
      .map(item => ({
        itemId: item.reservation_item_id,
        name: item.reservation_item_name,
        reservations: item.reservations_count,
        hours: item.reserved_hours.toFixed(1),
        avgDuration: (item.avg_duration_hours).toFixed(1),
      }))
      .sort((a, b) => a.reservations - b.reservations)
      .slice(0, 10);
  }, [byItem]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader size={32} className="text-sky-600 animate-spin" />
      </div>
    );
  }

  if (errorGlobal) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6 text-red-700">
        <div className="flex items-start gap-3">
          <AlertCircle size={20} className="flex-shrink-0 mt-0.5" />
          <div>
            <p className="font-semibold">Erreur lors du chargement des données</p>
            <p className="text-sm mt-1">{String(errorGlobal)}</p>
          </div>
        </div>
      </div>
    );
  }

  if (!globalKpi) {
    return <div className="text-center py-8 text-slate-600">Aucune donnée disponible</div>;
  }

  // Prepare chart data
  const trendData = trendsByMonth?.map(item => ({
    month: item.start_month,
    reservations: item.reservations_count,
    hours: item.reserved_hours,
    avgDuration: parseFloat(item.avg_duration_hours.toFixed(2)),
  })) || [];

  const durationData = durationDistribution?.map(item => ({
    bucket: item.duration_bucket,
    count: item.reservations_count,
  })) || [];

  const itemsData = byItem?.slice(0, 10).map(item => ({
    name: item.reservation_item_name,
    count: item.reservations_count,
    hours: item.reserved_hours,
  })) || [];

  const usersData = byUser?.map(user => ({
    name: user.user_full_name || user.user_login,
    count: user.reservations_count,
    hours: user.reserved_hours,
  })) || [];

  const tabs = [
    { id: 'global', label: 'Vue Globale' },
    { id: 'items', label: 'Par Ressource' },
    { id: 'users', label: 'Par Utilisateur' },
  ];

  return (
    <div className="space-y-8">
      {/* Page Header */}
      <div>
        <h1 className="text-3xl font-bold text-slate-900">Réservations</h1>
        <p className="text-slate-600 mt-2">Analyse des réservations et utilisation des ressources</p>
      </div>

      {/* KPI Cards - 4 columns */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg border border-slate-200 p-6">
          <div className="flex items-center justify-between">
            <p className="text-sm font-medium text-slate-600">Total Réservations</p>
          </div>
          <p className="text-3xl font-bold text-slate-900 mt-2">
            {formatNumber(globalKpi.total_reservations)}
          </p>
        </div>
        <div className="bg-white rounded-lg border border-slate-200 p-6">
          <div className="flex items-center justify-between">
            <p className="text-sm font-medium text-slate-600">Heures Réservées</p>
          </div>
          <p className="text-3xl font-bold text-sky-600 mt-2">
            {formatNumber(globalKpi.reserved_hours)} hrs
          </p>
          <p className="text-xs text-slate-500 mt-2">Jan 2024 - Dec 2025 | total</p>
        </div>
        <div className="bg-white rounded-lg border border-slate-200 p-6">
          <div className="flex items-center justify-between">
            <p className="text-sm font-medium text-slate-600">Durée Moyenne</p>
          </div>
          <p className="text-3xl font-bold text-emerald-600 mt-2">
            {globalKpi.avg_duration_hours.toFixed(1)} hrs
          </p>
          <p className="text-xs text-slate-500 mt-2">Jan 2024 - Dec 2025 | moyenne</p>
        </div>
        <div className="bg-yellow-50 rounded-lg border border-yellow-200 p-6">
          <div className="flex items-center justify-between">
            <p className="text-sm font-medium text-slate-600">Créneau de Pointe</p>
          </div>
          <p className="text-3xl font-bold text-yellow-700 mt-2">
            {peakPeriod}
          </p>
          <p className="text-xs text-slate-500 mt-2">Jan 2024 - Dec 2025 | demande max</p>
        </div>
      </div>

      {/* Over-Booked and Underused Section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Over-Booked Rooms */}
        <div className="bg-white rounded-lg border border-slate-200 p-6">
          <div className="mb-6">
            <h3 className="text-lg font-bold text-slate-900">Over-Booked Rooms (Bottlenecks)</h3>
            <p className="text-sm text-slate-600 mt-1">Rooms blocking growth</p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="px-3 py-2 text-left font-semibold text-slate-900">Room</th>
                  <th className="px-3 py-2 text-left font-semibold text-slate-900">Occupancy</th>
                  <th className="px-3 py-2 text-left font-semibold text-slate-900">Avg Duration</th>
                  <th className="px-3 py-2 text-left font-semibold text-slate-900">Pressure</th>
                </tr>
              </thead>
              <tbody>
                {overBookedRooms.slice(0, 5).map((room, idx) => (
                  <tr key={idx} className="border-b border-slate-100 hover:bg-slate-50">
                    <td className="px-3 py-3 text-slate-700">{room.name}</td>
                    <td className="px-3 py-3">
                      <div className="flex items-center gap-2">
                        <div className="w-16 bg-slate-200 rounded-full h-2">
                          <div
                            className={`h-2 rounded-full ${
                              room.occupancy > 80
                                ? 'bg-red-500'
                                : room.occupancy > 60
                                ? 'bg-yellow-500'
                                : 'bg-green-500'
                            }`}
                            style={{ width: `${room.occupancy}%` }}
                          />
                        </div>
                        <span className="text-xs font-medium text-slate-700">{room.occupancy}%</span>
                      </div>
                    </td>
                    <td className="px-3 py-3 text-slate-700">{room.avgDuration}h</td>
                    <td className="px-3 py-3">
                      <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-semibold ${
                        room.pressure === 'Critical'
                          ? 'bg-red-100 text-red-800'
                          : room.pressure === 'High'
                          ? 'bg-yellow-100 text-yellow-800'
                          : 'bg-blue-100 text-blue-800'
                      }`}>
                        {room.pressure}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Underused Shared Assets */}
        <div className="bg-white rounded-lg border border-slate-200 p-6">
          <div className="mb-6">
            <h3 className="text-lg font-bold text-slate-900">Underused Shared Assets</h3>
            <p className="text-sm text-slate-600 mt-1">Monetization or reallocation candidates</p>
          </div>
          {underusedAssets.length === 0 ? (
            <div className="py-8 text-center">
              <p className="text-sm text-slate-500">No underused assets found</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-200">
                    <th className="px-3 py-2 text-left font-semibold text-slate-900">Room</th>
                    <th className="px-3 py-2 text-left font-semibold text-slate-900">Reservations</th>
                    <th className="px-3 py-2 text-left font-semibold text-slate-900">Surface</th>
                    <th className="px-3 py-2 text-left font-semibold text-slate-900">Capacity</th>
                  </tr>
                </thead>
                <tbody>
                  {underusedAssets.slice(0, 5).map((asset, idx) => (
                    <tr key={idx} className="border-b border-slate-100 hover:bg-slate-50">
                      <td className="px-3 py-3 text-slate-700 font-medium">{asset.name}</td>
                      <td className="px-3 py-3">
                        <span className="inline-flex items-center px-2 py-1 bg-slate-100 text-slate-800 rounded text-xs font-medium">
                          {asset.reservations}
                        </span>
                      </td>
                      <td className="px-3 py-3 text-slate-700">—</td>
                      <td className="px-3 py-3 text-slate-700">—</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 border-b border-slate-200">
        {tabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id as typeof activeTab)}
            className={`px-4 py-3 font-medium text-sm border-b-2 transition-colors ${
              activeTab === tab.id
                ? 'border-sky-600 text-sky-600'
                : 'border-transparent text-slate-600 hover:text-slate-900'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Global View */}
      {activeTab === 'global' && (
        <div className="space-y-6">
          {/* Trends by Month */}
          <ChartCard
            title="Tendances Mensuelles"
            subtitle="Réservations et heures utilisées par mois"
          >
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={trendData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="month" />
                <YAxis yAxisId="left" />
                <YAxis yAxisId="right" orientation="right" />
                <Tooltip />
                <Legend />
                <Line
                  yAxisId="left"
                  type="monotone"
                  dataKey="reservations"
                  stroke="#0ea5e9"
                  name="Réservations"
                  dot={{ fill: '#0ea5e9', r: 4 }}
                />
                <Line
                  yAxisId="right"
                  type="monotone"
                  dataKey="hours"
                  stroke="#10b981"
                  name="Heures"
                  dot={{ fill: '#10b981', r: 4 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </ChartCard>

          {/* Duration Distribution */}
          <ChartCard
            title="Distribution des Durées"
            subtitle="Répartition des durées de réservation"
          >
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={durationData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="bucket" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="count" fill="#8b5cf6" radius={[8, 8, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>
        </div>
      )}

      {/* Items View */}
      {activeTab === 'items' && (
        <ChartCard
          title="Réservations par Ressource"
          subtitle="Top 10 ressources les plus réservées"
        >
          <ResponsiveContainer width="100%" height={400}>
            <BarChart data={itemsData} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" />
              <YAxis dataKey="name" type="category" width={150} />
              <Tooltip />
              <Legend />
              <Bar dataKey="count" fill="#0ea5e9" name="Réservations" />
              <Bar dataKey="hours" fill="#10b981" name="Heures" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      )}

      {/* Users View */}
      {activeTab === 'users' && (
        <ChartCard
          title="Réservations par Utilisateur"
          subtitle="Top utilisateurs par nombre de réservations"
        >
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={usersData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" angle={-45} textAnchor="end" height={100} />
              <YAxis yAxisId="left" />
              <YAxis yAxisId="right" orientation="right" />
              <Tooltip />
              <Legend />
              <Bar yAxisId="left" dataKey="count" fill="#0ea5e9" name="Réservations" />
              <Bar yAxisId="right" dataKey="hours" fill="#10b981" name="Heures" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      )}

      {/* Current Reservations Table */}
      <ChartCard
        title="Réservations Récentes"
        subtitle={`${currentReservations?.length || 0} dernières réservations`}
      >
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200">
                <th className="px-4 py-2 text-left font-semibold text-slate-900">Utilisateur</th>
                <th className="px-4 py-2 text-left font-semibold text-slate-900">Ressource</th>
                <th className="px-4 py-2 text-left font-semibold text-slate-900">Début</th>
                <th className="px-4 py-2 text-left font-semibold text-slate-900">Durée</th>
                <th className="px-4 py-2 text-left font-semibold text-slate-900">Statut</th>
              </tr>
            </thead>
            <tbody>
              {currentReservations?.slice(0, 20).map(reservation => (
                <tr key={reservation.reservation_id} className="border-b border-slate-100 hover:bg-slate-50">
                  <td className="px-4 py-2 text-slate-700">{reservation.user_full_name}</td>
                  <td className="px-4 py-2 text-slate-700">{reservation.reservation_item_name}</td>
                  <td className="px-4 py-2 text-slate-700">{reservation.start_date} {reservation.start_hour}h</td>
                  <td className="px-4 py-2 text-slate-700">{reservation.duration_hours.toFixed(2)}h</td>
                  <td className="px-4 py-2">
                    <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                      reservation.is_invalid_duration
                        ? 'bg-red-100 text-red-800'
                        : 'bg-green-100 text-green-800'
                    }`}>
                      {reservation.is_invalid_duration ? 'Invalide' : 'Valide'}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </ChartCard>
    </div>
  );
};

export default ReservationExecutivePage;