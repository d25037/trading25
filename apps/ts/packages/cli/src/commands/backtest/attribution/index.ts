import { define } from 'gunshi';

export const attributionCommand = define({
  name: 'attribution',
  description: 'Signal attribution operations (LOO + Shapley)',
  run: (ctx) => {
    ctx.log('Use --help to see available attribution commands');
  },
});
