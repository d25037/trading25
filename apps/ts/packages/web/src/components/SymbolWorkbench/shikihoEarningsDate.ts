export type ShikihoEarningsDateState = 'neutral' | 'yellow' | 'orange' | 'red' | 'past';

export interface ShikihoEarningsDatePresentation {
  state: ShikihoEarningsDateState;
  daysRemaining: number;
  remainingDayText: string;
}

const DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000;

function getJstCalendarDay(date: Date): number {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Tokyo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return Date.UTC(Number(values.year), Number(values.month) - 1, Number(values.day));
}

function parseCalendarDay(date: string): number {
  const year = Number(date.slice(0, 4));
  const month = Number(date.slice(5, 7));
  const day = Number(date.slice(8, 10));
  return Date.UTC(year, month - 1, day);
}

export function getShikihoEarningsDateState(date: string, now: Date = new Date()): ShikihoEarningsDatePresentation {
  const daysRemaining = Math.round((parseCalendarDay(date) - getJstCalendarDay(now)) / DAY_IN_MILLISECONDS);
  if (daysRemaining < 0) return { state: 'past', daysRemaining, remainingDayText: '予定日を過ぎています' };

  const remainingDayText = daysRemaining === 0 ? '本日' : `あと${daysRemaining}日`;
  if (daysRemaining <= 3) return { state: 'red', daysRemaining, remainingDayText };
  if (daysRemaining <= 7) return { state: 'orange', daysRemaining, remainingDayText };
  if (daysRemaining <= 14) return { state: 'yellow', daysRemaining, remainingDayText };
  return { state: 'neutral', daysRemaining, remainingDayText };
}
