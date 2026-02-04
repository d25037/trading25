import { cva, type VariantProps } from 'class-variance-authority';
import * as React from 'react';

import { cn } from '@/lib/utils';

const labelVariants = cva('text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70');

interface LabelProps extends React.ComponentPropsWithoutRef<'label'>, VariantProps<typeof labelVariants> {
  htmlFor?: string; // Optional for decorative labels
  children: React.ReactNode;
}

const Label = React.forwardRef<React.ElementRef<'label'>, LabelProps>(
  ({ className, htmlFor, children, ...props }, ref) => (
    <label ref={ref} className={cn(labelVariants(), className)} htmlFor={htmlFor} {...props}>
      {children}
    </label>
  )
);
Label.displayName = 'Label';

export { Label };
