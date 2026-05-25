export function getRSquaredColor(rSquared: number): string {
  const pct = rSquared * 100;
  if (pct >= 30) return 'text-green-500';
  if (pct >= 10) return 'text-yellow-500';
  return 'text-muted-foreground';
}

export function getBetaColor(beta: number): string {
  if (beta > 1.2) return 'text-red-500';
  if (beta > 0.8) return 'text-yellow-500';
  return 'text-green-500';
}

export function getBetaInterpretation(beta: number): string {
  if (beta > 1.2) return 'High sensitivity';
  if (beta > 0.8) return 'Moderate sensitivity';
  return 'Low sensitivity';
}
