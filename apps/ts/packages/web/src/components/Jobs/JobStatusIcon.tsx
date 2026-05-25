import { AlertCircle, Ban, CheckCircle2, Loader2, XCircle } from 'lucide-react';

type JobStatusIconSize = 'sm' | 'md';

interface JobStatusIconProps {
  status: string | null | undefined;
  size?: JobStatusIconSize;
  showUnknown?: boolean;
}

const SIZE_CLASS: Record<JobStatusIconSize, string> = {
  sm: 'h-4 w-4',
  md: 'h-5 w-5',
};

export function JobStatusIcon({ status, size = 'md', showUnknown = false }: JobStatusIconProps) {
  const className = SIZE_CLASS[size];

  switch (status) {
    case 'pending':
    case 'running':
      return <Loader2 className={`${className} animate-spin text-blue-500`} />;
    case 'completed':
      return <CheckCircle2 className={`${className} text-green-500`} />;
    case 'failed':
      return <XCircle className={`${className} text-red-500`} />;
    case 'cancelled':
      return <Ban className={`${className} text-orange-500`} />;
    default:
      return showUnknown ? <AlertCircle className={`${className} text-yellow-500`} /> : null;
  }
}
