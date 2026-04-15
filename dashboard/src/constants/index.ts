// API Endpoints
export const API_ENDPOINTS = {
  SPACE: {
    GLOBAL: '/api/space/global',
    BY_SITE: '/api/space/by-site',
    BY_SPACE_TYPE: '/api/space/by-site-space-type',
    BY_ORG_TYPE: '/api/space/by-site-org-type',
    ROOMS: '/api/space/rooms',
  },
  TICKETS: {
    CARDS: '/api/tickets/cards',
    BY_MONTH: '/api/tickets/by-month',
    BY_YEAR: '/api/tickets/by-year',
    PRIORITY: '/api/tickets/priority',
    CATEGORY: '/api/tickets/category',
    LOCATION: '/api/tickets/location',
    BY_TECHNICIAN: '/api/tickets/by-technician',
    HEATMAP: '/api/tickets/heatmap',
    LIST: '/api/tickets/list',
  },
};

// Query Keys for React Query
export const QUERY_KEYS = {
  SPACE: {
    GLOBAL: ['space', 'global'],
    BY_SITE: ['space', 'by-site'],
    BY_SPACE_TYPE: ['space', 'by-site-space-type'],
    BY_ORG_TYPE: ['space', 'by-site-org-type'],
    ROOMS: ['space', 'rooms'],
  },
  TICKETS: {
    CARDS: ['tickets', 'cards'],
    BY_MONTH: ['tickets', 'by-month'],
    BY_YEAR: ['tickets', 'by-year'],
    PRIORITY: ['tickets', 'priority'],
    CATEGORY: ['tickets', 'category'],
    LOCATION: ['tickets', 'location'],
    BY_TECHNICIAN: ['tickets', 'by-technician'],
    HEATMAP: ['tickets', 'heatmap'],
    LIST: ['tickets', 'list'],
  },
};

// Period Enum for Tickets
export const PERIODS = {
  ALL: 'ALL',
} as const;

// Ticket Priority Levels
export const TICKET_PRIORITY = {
  1: 'Minimal',
  2: 'Low',
  3: 'Medium',
  4: 'High',
  5: 'Critical',
} as const;

// Ticket Status
export const TICKET_STATUS = {
  OPEN: 'OPEN',
  RESOLVED: 'RESOLVED',
  CLOSED: 'CLOSED',
} as const;
