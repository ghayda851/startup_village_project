import { useState } from 'react';
import Sidebar from './components/Sidebar';
import SpaceExecutivePage from './pages/SpaceExecutivePage';
import TicketsExecutivePage from './pages/TicketsExecutivePage';
import ReservationExecutivePage from './pages/ReservationExecutivePage';

type Page = 'overview' | 'access' | 'space' | 'rooms' | 'hr' | 'financial' | 'suivi' | 'energy' | 'ecosystem' | 'social' | 'inventory' | 'tickets' | 'fnb' | 'correlations';

function App() {
  const [currentPage, setCurrentPage] = useState<Page>('space');

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Sidebar */}
      <Sidebar activeItem={String(currentPage)} onItemClick={(id) => setCurrentPage(id as Page)} />

      {/* Main Content */}
      <main className="ml-52 min-h-screen">
        <div className="p-8">
          {currentPage === 'space' && <SpaceExecutivePage />}
          {currentPage === 'rooms' && <ReservationExecutivePage />}
          {currentPage === 'tickets' && <TicketsExecutivePage />}
          
          {/* Placeholder for other pages */}
          {!['space', 'rooms', 'tickets'].includes(currentPage) && (
            <div className="rounded-lg bg-white p-8 text-center">
              <h1 className="text-2xl font-bold text-slate-900 capitalize">{currentPage.replace('-', ' ')} Page</h1>
              <p className="mt-2 text-slate-600">This page is under development. Coming soon!</p>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}

export default App;
