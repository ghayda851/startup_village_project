import { BarChart3, Home, MapPin, Users, SquareM, Ticket, Grid2X2, Wallet, Zap, Network, Share2, Package, Settings, TrendingUp } from 'lucide-react';
import { cn } from '../utils/cn';

interface SidebarItem {
  id: string;
  label: string;
  icon: React.ComponentType<{ size: number; className?: string }>;
}

const sidebarItems: SidebarItem[] = [
  { id: 'overview', label: 'Overview', icon: Home },
  { id: 'access', label: 'Access & Presence', icon: MapPin },
  { id: 'space', label: 'Space Distribution', icon: SquareM },
  { id: 'rooms', label: 'Room Reservations', icon: Grid2X2 },
  { id: 'hr', label: 'HR Analytics', icon: Users },
  { id: 'financial', label: 'Financial Details', icon: Wallet },
  { id: 'suivi', label: 'Financial Follow-up', icon: TrendingUp },
  { id: 'energy', label: 'Energy & Utilities', icon: Zap },
  { id: 'ecosystem', label: 'Ecosystem', icon: Network },
  { id: 'social', label: 'Social Media', icon: Share2 },
  { id: 'inventory', label: 'Inventory', icon: Package },
  { id: 'tickets', label: 'Tickets IT', icon: Ticket },
  { id: 'fnb', label: 'Catering F&B', icon: BarChart3 },
  { id: 'correlations', label: 'Cross-Correlations', icon: Settings },
];

interface SidebarProps {
  activeItem: string;
  onItemClick: (id: string) => void;
}

export default function Sidebar({ activeItem, onItemClick }: SidebarProps) {
  return (
    <aside className="fixed left-0 top-0 h-screen w-52 bg-white border-r border-slate-200 overflow-y-auto">
      {/* Logo */}
      <div className="p-6 border-b border-slate-200">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 bg-sky-600 rounded-lg flex items-center justify-center text-white font-bold">
            SV
          </div>
          <div>
            <h1 className="text-sm font-bold text-slate-900">Startup Village</h1>
            <p className="text-xs text-slate-500">Analytics Dashboard</p>
          </div>
        </div>
      </div>

      {/* Navigation Items */}
      <nav className="p-4 space-y-1">
        {sidebarItems.map((item) => {
          const Icon = item.icon;
          const isActive = activeItem === item.id;
          
          return (
            <button
              key={item.id}
              onClick={() => onItemClick(item.id)}
              className={cn(
                'w-full flex items-center gap-3 px-4 py-2.5 rounded-lg text-sm font-medium transition-all text-left',
                isActive
                  ? 'bg-sky-100 text-sky-700'
                  : 'text-slate-700 hover:bg-slate-100'
              )}
            >
              <Icon size={18} className="flex-shrink-0" />
              <span>{item.label}</span>
            </button>
          );
        })}
      </nav>

      {/* Data Period Footer */}
      <div className="absolute bottom-0 left-0 right-0 p-4 border-t border-slate-200 bg-slate-50">
        <p className="text-xs text-slate-600 font-medium">Data Period</p>
        <p className="text-xs text-slate-500">2022 - 2026</p>
      </div>
    </aside>
  );
}
