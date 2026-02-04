# ohlcv_accessors module¶

# ohlcv_accessors module[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors "Permanent link")

Custom pandas accessors for OHLC(V) data.

Methods can be accessed as follows:

  * [OHLCVDFAccessor](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor "vectorbt.ohlcv_accessors.OHLCVDFAccessor") -> `pd.DataFrame.vbt.ohlc.*`
  * [OHLCVDFAccessor](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor "vectorbt.ohlcv_accessors.OHLCVDFAccessor") -> `pd.DataFrame.vbt.ohlcv.*`

The accessors inherit [vectorbt.generic.accessors](../generic/accessors/index.html "vectorbt.generic.accessors").

Note

Accessors do not utilize caching.

## Column names[¶](index.html#column-names "Permanent link")

By default, vectorbt searches for columns with names 'open', 'high', 'low', 'close', and 'volume' (case doesn't matter). You can change the naming either using `ohlcv.column_names` in [settings](../_settings/index.html#vectorbt._settings.settings "vectorbt._settings.settings"), or by providing `column_names` directly to the accessor.

    >>> import pandas as pd
    >>> import vectorbt as vbt
    
    >>> df = pd.DataFrame({
    ...     'my_open1': [2, 3, 4, 3.5, 2.5],
    ...     'my_high2': [3, 4, 4.5, 4, 3],
    ...     'my_low3': [1.5, 2.5, 3.5, 2.5, 1.5],
    ...     'my_close4': [2.5, 3.5, 4, 3, 2],
    ...     'my_volume5': [10, 11, 10, 9, 10]
    ... })
    
    >>> # vectorbt can't find columns
    >>> df.vbt.ohlcv.get_column('open')
    None
    
    >>> my_column_names = dict(
    ...     open='my_open1',
    ...     high='my_high2',
    ...     low='my_low3',
    ...     close='my_close4',
    ...     volume='my_volume5',
    ... )
    >>> ohlcv_acc = df.vbt.ohlcv(freq='d', column_names=my_column_names)
    >>> ohlcv_acc.get_column('open')
    0    2.0
    1    3.0
    2    4.0
    3    3.5
    4    2.5
    Name: my_open1, dtype: float64

## Stats[¶](index.html#stats "Permanent link")

Hint

See [StatsBuilderMixin.stats()](../generic/stats_builder/index.html#vectorbt.generic.stats_builder.StatsBuilderMixin.stats "vectorbt.generic.stats_builder.StatsBuilderMixin.stats") and [OHLCVDFAccessor.metrics](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.metrics "vectorbt.ohlcv_accessors.OHLCVDFAccessor.metrics").

    >>> ohlcv_acc.stats()
    Start                           0
    End                             4
    Period            5 days 00:00:00
    First Price                   2.0
    Lowest Price                  1.5
    Highest Price                 4.5
    Last Price                    2.0
    First Volume                   10
    Lowest Volume                   9
    Highest Volume                 11
    Last Volume                    10
    Name: agg_func_mean, dtype: object

## Plots[¶](index.html#plots "Permanent link")

Hint

See [PlotsBuilderMixin.plots()](../generic/plots_builder/index.html#vectorbt.generic.plots_builder.PlotsBuilderMixin.plots "vectorbt.generic.plots_builder.PlotsBuilderMixin.plots") and [OHLCVDFAccessor.subplots](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.subplots "vectorbt.ohlcv_accessors.OHLCVDFAccessor.subplots").

[OHLCVDFAccessor](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor "vectorbt.ohlcv_accessors.OHLCVDFAccessor") class has a single subplot based on [OHLCVDFAccessor.plot()](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.plot "vectorbt.ohlcv_accessors.OHLCVDFAccessor.plot") (without volume):

    >>> ohlcv_acc.plots(settings=dict(plot_type='candlestick'))

![](../../assets/images/ohlcv_plots.svg)

* * *

## OHLCVDFAccessor class[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L104-L435 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor "Permanent link")

    OHLCVDFAccessor(
        obj,
        column_names=None,
        **kwargs
    )

Accessor on top of OHLCV data. For DataFrames only.

Accessible through `pd.DataFrame.vbt.ohlcv`.

**Superclasses**

  * [AttrResolver](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver "vectorbt.utils.attr_.AttrResolver")
  * [BaseAccessor](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor "vectorbt.base.accessors.BaseAccessor")
  * [BaseDFAccessor](../base/accessors/index.html#vectorbt.base.accessors.BaseDFAccessor "vectorbt.base.accessors.BaseDFAccessor")
  * [Configured](../utils/config/index.html#vectorbt.utils.config.Configured "vectorbt.utils.config.Configured")
  * [Documented](../utils/docs/index.html#vectorbt.utils.docs.Documented "vectorbt.utils.docs.Documented")
  * [GenericAccessor](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor "vectorbt.generic.accessors.GenericAccessor")
  * [GenericDFAccessor](../generic/accessors/index.html#vectorbt.generic.accessors.GenericDFAccessor "vectorbt.generic.accessors.GenericDFAccessor")
  * [IndexingBase](../base/indexing/index.html#vectorbt.base.indexing.IndexingBase "vectorbt.base.indexing.IndexingBase")
  * [PandasIndexer](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer "vectorbt.base.indexing.PandasIndexer")
  * [Pickleable](../utils/config/index.html#vectorbt.utils.config.Pickleable "vectorbt.utils.config.Pickleable")
  * [PlotsBuilderMixin](../generic/plots_builder/index.html#vectorbt.generic.plots_builder.PlotsBuilderMixin "vectorbt.generic.plots_builder.PlotsBuilderMixin")
  * [StatsBuilderMixin](../generic/stats_builder/index.html#vectorbt.generic.stats_builder.StatsBuilderMixin "vectorbt.generic.stats_builder.StatsBuilderMixin")
  * [Wrapping](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping "vectorbt.base.array_wrapper.Wrapping")

**Inherited members**

  * [AttrResolver.deep_getattr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.deep_getattr "vectorbt.generic.accessors.GenericDFAccessor.deep_getattr")
  * [AttrResolver.post_resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.post_resolve_attr "vectorbt.generic.accessors.GenericDFAccessor.post_resolve_attr")
  * [AttrResolver.pre_resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.pre_resolve_attr "vectorbt.generic.accessors.GenericDFAccessor.pre_resolve_attr")
  * [AttrResolver.resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.resolve_attr "vectorbt.generic.accessors.GenericDFAccessor.resolve_attr")
  * [BaseAccessor.align_to()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.align_to "vectorbt.generic.accessors.GenericDFAccessor.align_to")
  * [BaseAccessor.apply()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply "vectorbt.generic.accessors.GenericDFAccessor.apply")
  * [BaseAccessor.apply_and_concat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply_and_concat "vectorbt.generic.accessors.GenericDFAccessor.apply_and_concat")
  * [BaseAccessor.apply_on_index()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply_on_index "vectorbt.generic.accessors.GenericDFAccessor.apply_on_index")
  * [BaseAccessor.broadcast()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.broadcast "vectorbt.generic.accessors.GenericDFAccessor.broadcast")
  * [BaseAccessor.broadcast_to()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.broadcast_to "vectorbt.generic.accessors.GenericDFAccessor.broadcast_to")
  * [BaseAccessor.combine()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.combine "vectorbt.generic.accessors.GenericDFAccessor.combine")
  * [BaseAccessor.concat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.concat "vectorbt.generic.accessors.GenericDFAccessor.concat")
  * [BaseAccessor.drop_duplicate_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_duplicate_levels "vectorbt.generic.accessors.GenericDFAccessor.drop_duplicate_levels")
  * [BaseAccessor.drop_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_levels "vectorbt.generic.accessors.GenericDFAccessor.drop_levels")
  * [BaseAccessor.drop_redundant_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_redundant_levels "vectorbt.generic.accessors.GenericDFAccessor.drop_redundant_levels")
  * [BaseAccessor.empty()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.empty "vectorbt.generic.accessors.GenericDFAccessor.empty")
  * [BaseAccessor.empty_like()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.empty_like "vectorbt.generic.accessors.GenericDFAccessor.empty_like")
  * [BaseAccessor.indexing_func()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.indexing_func "vectorbt.generic.accessors.GenericDFAccessor.indexing_func")
  * [BaseAccessor.make_symmetric()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.make_symmetric "vectorbt.generic.accessors.GenericDFAccessor.make_symmetric")
  * [BaseAccessor.rename_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.rename_levels "vectorbt.generic.accessors.GenericDFAccessor.rename_levels")
  * [BaseAccessor.repeat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.repeat "vectorbt.generic.accessors.GenericDFAccessor.repeat")
  * [BaseAccessor.select_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.select_levels "vectorbt.generic.accessors.GenericDFAccessor.select_levels")
  * [BaseAccessor.stack_index()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.stack_index "vectorbt.generic.accessors.GenericDFAccessor.stack_index")
  * [BaseAccessor.tile()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.tile "vectorbt.generic.accessors.GenericDFAccessor.tile")
  * [BaseAccessor.to_1d_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_1d_array "vectorbt.generic.accessors.GenericDFAccessor.to_1d_array")
  * [BaseAccessor.to_2d_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_2d_array "vectorbt.generic.accessors.GenericDFAccessor.to_2d_array")
  * [BaseAccessor.to_dict()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_dict "vectorbt.generic.accessors.GenericDFAccessor.to_dict")
  * [BaseAccessor.unstack_to_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.unstack_to_array "vectorbt.generic.accessors.GenericDFAccessor.unstack_to_array")
  * [BaseAccessor.unstack_to_df()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.unstack_to_df "vectorbt.generic.accessors.GenericDFAccessor.unstack_to_df")
  * [Configured.copy()](../utils/config/index.html#vectorbt.utils.config.Configured.copy "vectorbt.generic.accessors.GenericDFAccessor.copy")
  * [Configured.dumps()](../utils/config/index.html#vectorbt.utils.config.Pickleable.dumps "vectorbt.generic.accessors.GenericDFAccessor.dumps")
  * [Configured.loads()](../utils/config/index.html#vectorbt.utils.config.Pickleable.loads "vectorbt.generic.accessors.GenericDFAccessor.loads")
  * [Configured.replace()](../utils/config/index.html#vectorbt.utils.config.Configured.replace "vectorbt.generic.accessors.GenericDFAccessor.replace")
  * [Configured.to_doc()](../utils/docs/index.html#vectorbt.utils.docs.Documented.to_doc "vectorbt.generic.accessors.GenericDFAccessor.to_doc")
  * [Configured.update_config()](../utils/config/index.html#vectorbt.utils.config.Configured.update_config "vectorbt.generic.accessors.GenericDFAccessor.update_config")
  * [GenericAccessor.apply_along_axis()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.apply_along_axis "vectorbt.generic.accessors.GenericDFAccessor.apply_along_axis")
  * [GenericAccessor.apply_and_reduce()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.apply_and_reduce "vectorbt.generic.accessors.GenericDFAccessor.apply_and_reduce")
  * [GenericAccessor.apply_mapping()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.apply_mapping "vectorbt.generic.accessors.GenericDFAccessor.apply_mapping")
  * [GenericAccessor.applymap()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.applymap "vectorbt.generic.accessors.GenericDFAccessor.applymap")
  * [GenericAccessor.barplot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.barplot "vectorbt.generic.accessors.GenericDFAccessor.barplot")
  * [GenericAccessor.bfill()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.bfill "vectorbt.generic.accessors.GenericDFAccessor.bfill")
  * [GenericAccessor.binarize()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.binarize "vectorbt.generic.accessors.GenericDFAccessor.binarize")
  * [GenericAccessor.boxplot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.boxplot "vectorbt.generic.accessors.GenericDFAccessor.boxplot")
  * [GenericAccessor.bshift()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.bshift "vectorbt.generic.accessors.GenericDFAccessor.bshift")
  * [GenericAccessor.count()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.count "vectorbt.generic.accessors.GenericDFAccessor.count")
  * [GenericAccessor.crossed_above()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.crossed_above "vectorbt.generic.accessors.GenericDFAccessor.crossed_above")
  * [GenericAccessor.crossed_below()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.crossed_below "vectorbt.generic.accessors.GenericDFAccessor.crossed_below")
  * [GenericAccessor.cumprod()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.cumprod "vectorbt.generic.accessors.GenericDFAccessor.cumprod")
  * [GenericAccessor.cumsum()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.cumsum "vectorbt.generic.accessors.GenericDFAccessor.cumsum")
  * [GenericAccessor.describe()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.describe "vectorbt.generic.accessors.GenericDFAccessor.describe")
  * [GenericAccessor.diff()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.diff "vectorbt.generic.accessors.GenericDFAccessor.diff")
  * [GenericAccessor.drawdown()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.drawdown "vectorbt.generic.accessors.GenericDFAccessor.drawdown")
  * [GenericAccessor.ewm_mean()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.ewm_mean "vectorbt.generic.accessors.GenericDFAccessor.ewm_mean")
  * [GenericAccessor.ewm_std()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.ewm_std "vectorbt.generic.accessors.GenericDFAccessor.ewm_std")
  * [GenericAccessor.expanding_apply()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_apply "vectorbt.generic.accessors.GenericDFAccessor.expanding_apply")
  * [GenericAccessor.expanding_max()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_max "vectorbt.generic.accessors.GenericDFAccessor.expanding_max")
  * [GenericAccessor.expanding_mean()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_mean "vectorbt.generic.accessors.GenericDFAccessor.expanding_mean")
  * [GenericAccessor.expanding_min()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_min "vectorbt.generic.accessors.GenericDFAccessor.expanding_min")
  * [GenericAccessor.expanding_split()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_split "vectorbt.generic.accessors.GenericDFAccessor.expanding_split")
  * [GenericAccessor.expanding_std()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_std "vectorbt.generic.accessors.GenericDFAccessor.expanding_std")
  * [GenericAccessor.ffill()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.ffill "vectorbt.generic.accessors.GenericDFAccessor.ffill")
  * [GenericAccessor.fillna()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.fillna "vectorbt.generic.accessors.GenericDFAccessor.fillna")
  * [GenericAccessor.filter()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.filter "vectorbt.generic.accessors.GenericDFAccessor.filter")
  * [GenericAccessor.fshift()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.fshift "vectorbt.generic.accessors.GenericDFAccessor.fshift")
  * [GenericAccessor.get_drawdowns()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.get_drawdowns "vectorbt.generic.accessors.GenericDFAccessor.get_drawdowns")
  * [GenericAccessor.get_ranges()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.get_ranges "vectorbt.generic.accessors.GenericDFAccessor.get_ranges")
  * [GenericAccessor.groupby_apply()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.groupby_apply "vectorbt.generic.accessors.GenericDFAccessor.groupby_apply")
  * [GenericAccessor.histplot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.histplot "vectorbt.generic.accessors.GenericDFAccessor.histplot")
  * [GenericAccessor.idxmax()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.idxmax "vectorbt.generic.accessors.GenericDFAccessor.idxmax")
  * [GenericAccessor.idxmin()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.idxmin "vectorbt.generic.accessors.GenericDFAccessor.idxmin")
  * [GenericAccessor.lineplot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.lineplot "vectorbt.generic.accessors.GenericDFAccessor.lineplot")
  * [GenericAccessor.max()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.max "vectorbt.generic.accessors.GenericDFAccessor.max")
  * [GenericAccessor.maxabs_scale()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.maxabs_scale "vectorbt.generic.accessors.GenericDFAccessor.maxabs_scale")
  * [GenericAccessor.mean()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.mean "vectorbt.generic.accessors.GenericDFAccessor.mean")
  * [GenericAccessor.median()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.median "vectorbt.generic.accessors.GenericDFAccessor.median")
  * [GenericAccessor.min()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.min "vectorbt.generic.accessors.GenericDFAccessor.min")
  * [GenericAccessor.minmax_scale()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.minmax_scale "vectorbt.generic.accessors.GenericDFAccessor.minmax_scale")
  * [GenericAccessor.normalize()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.normalize "vectorbt.generic.accessors.GenericDFAccessor.normalize")
  * [GenericAccessor.pct_change()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.pct_change "vectorbt.generic.accessors.GenericDFAccessor.pct_change")
  * [GenericAccessor.power_transform()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.power_transform "vectorbt.generic.accessors.GenericDFAccessor.power_transform")
  * [GenericAccessor.product()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.product "vectorbt.generic.accessors.GenericDFAccessor.product")
  * [GenericAccessor.quantile_transform()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.quantile_transform "vectorbt.generic.accessors.GenericDFAccessor.quantile_transform")
  * [GenericAccessor.range_split()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.range_split "vectorbt.generic.accessors.GenericDFAccessor.range_split")
  * [GenericAccessor.rebase()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rebase "vectorbt.generic.accessors.GenericDFAccessor.rebase")
  * [GenericAccessor.reduce()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.reduce "vectorbt.generic.accessors.GenericDFAccessor.reduce")
  * [GenericAccessor.resample_apply()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.resample_apply "vectorbt.generic.accessors.GenericDFAccessor.resample_apply")
  * [GenericAccessor.resolve_self()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.resolve_self "vectorbt.generic.accessors.GenericDFAccessor.resolve_self")
  * [GenericAccessor.robust_scale()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.robust_scale "vectorbt.generic.accessors.GenericDFAccessor.robust_scale")
  * [GenericAccessor.rolling_apply()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_apply "vectorbt.generic.accessors.GenericDFAccessor.rolling_apply")
  * [GenericAccessor.rolling_max()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_max "vectorbt.generic.accessors.GenericDFAccessor.rolling_max")
  * [GenericAccessor.rolling_mean()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_mean "vectorbt.generic.accessors.GenericDFAccessor.rolling_mean")
  * [GenericAccessor.rolling_min()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_min "vectorbt.generic.accessors.GenericDFAccessor.rolling_min")
  * [GenericAccessor.rolling_split()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_split "vectorbt.generic.accessors.GenericDFAccessor.rolling_split")
  * [GenericAccessor.rolling_std()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_std "vectorbt.generic.accessors.GenericDFAccessor.rolling_std")
  * [GenericAccessor.scale()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.scale "vectorbt.generic.accessors.GenericDFAccessor.scale")
  * [GenericAccessor.scatterplot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.scatterplot "vectorbt.generic.accessors.GenericDFAccessor.scatterplot")
  * [GenericAccessor.shuffle()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.shuffle "vectorbt.generic.accessors.GenericDFAccessor.shuffle")
  * [GenericAccessor.split()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.split "vectorbt.generic.accessors.GenericDFAccessor.split")
  * [GenericAccessor.std()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.std "vectorbt.generic.accessors.GenericDFAccessor.std")
  * [GenericAccessor.sum()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.sum "vectorbt.generic.accessors.GenericDFAccessor.sum")
  * [GenericAccessor.to_mapped()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.to_mapped "vectorbt.generic.accessors.GenericDFAccessor.to_mapped")
  * [GenericAccessor.to_returns()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.to_returns "vectorbt.generic.accessors.GenericDFAccessor.to_returns")
  * [GenericAccessor.transform()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.transform "vectorbt.generic.accessors.GenericDFAccessor.transform")
  * [GenericAccessor.value_counts()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.value_counts "vectorbt.generic.accessors.GenericDFAccessor.value_counts")
  * [GenericAccessor.zscore()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.zscore "vectorbt.generic.accessors.GenericDFAccessor.zscore")
  * [GenericDFAccessor.config](../utils/config/index.html#vectorbt.utils.config.Configured.config "vectorbt.generic.accessors.GenericDFAccessor.config")
  * [GenericDFAccessor.df_accessor_cls](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.df_accessor_cls "vectorbt.generic.accessors.GenericDFAccessor.df_accessor_cls")
  * [GenericDFAccessor.drawdowns](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.drawdowns "vectorbt.generic.accessors.GenericDFAccessor.drawdowns")
  * [GenericDFAccessor.flatten_grouped()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericDFAccessor.flatten_grouped "vectorbt.generic.accessors.GenericDFAccessor.flatten_grouped")
  * [GenericDFAccessor.heatmap()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericDFAccessor.heatmap "vectorbt.generic.accessors.GenericDFAccessor.heatmap")
  * [GenericDFAccessor.iloc](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.iloc "vectorbt.generic.accessors.GenericDFAccessor.iloc")
  * [GenericDFAccessor.indexing_kwargs](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.indexing_kwargs "vectorbt.generic.accessors.GenericDFAccessor.indexing_kwargs")
  * [GenericDFAccessor.loc](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.loc "vectorbt.generic.accessors.GenericDFAccessor.loc")
  * [GenericDFAccessor.mapping](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.mapping "vectorbt.generic.accessors.GenericDFAccessor.mapping")
  * [GenericDFAccessor.obj](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.obj "vectorbt.generic.accessors.GenericDFAccessor.obj")
  * [GenericDFAccessor.ranges](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.ranges "vectorbt.generic.accessors.GenericDFAccessor.ranges")
  * [GenericDFAccessor.self_aliases](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.self_aliases "vectorbt.generic.accessors.GenericDFAccessor.self_aliases")
  * [GenericDFAccessor.squeeze_grouped()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericDFAccessor.squeeze_grouped "vectorbt.generic.accessors.GenericDFAccessor.squeeze_grouped")
  * [GenericDFAccessor.sr_accessor_cls](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.sr_accessor_cls "vectorbt.generic.accessors.GenericDFAccessor.sr_accessor_cls")
  * [GenericDFAccessor.ts_heatmap()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericDFAccessor.ts_heatmap "vectorbt.generic.accessors.GenericDFAccessor.ts_heatmap")
  * [GenericDFAccessor.wrapper](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.wrapper "vectorbt.generic.accessors.GenericDFAccessor.wrapper")
  * [GenericDFAccessor.writeable_attrs](../utils/config/index.html#vectorbt.utils.config.Configured.writeable_attrs "vectorbt.generic.accessors.GenericDFAccessor.writeable_attrs")
  * [PandasIndexer.xs()](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.xs "vectorbt.generic.accessors.GenericDFAccessor.xs")
  * [Pickleable.load()](../utils/config/index.html#vectorbt.utils.config.Pickleable.load "vectorbt.generic.accessors.GenericDFAccessor.load")
  * [Pickleable.save()](../utils/config/index.html#vectorbt.utils.config.Pickleable.save "vectorbt.generic.accessors.GenericDFAccessor.save")
  * [PlotsBuilderMixin.build_subplots_doc()](../generic/plots_builder/index.html#vectorbt.generic.plots_builder.PlotsBuilderMixin.build_subplots_doc "vectorbt.generic.accessors.GenericDFAccessor.build_subplots_doc")
  * [PlotsBuilderMixin.override_subplots_doc()](../generic/plots_builder/index.html#vectorbt.generic.plots_builder.PlotsBuilderMixin.override_subplots_doc "vectorbt.generic.accessors.GenericDFAccessor.override_subplots_doc")
  * [PlotsBuilderMixin.plots()](../generic/plots_builder/index.html#vectorbt.generic.plots_builder.PlotsBuilderMixin.plots "vectorbt.generic.accessors.GenericDFAccessor.plots")
  * [StatsBuilderMixin.build_metrics_doc()](../generic/stats_builder/index.html#vectorbt.generic.stats_builder.StatsBuilderMixin.build_metrics_doc "vectorbt.generic.accessors.GenericDFAccessor.build_metrics_doc")
  * [StatsBuilderMixin.override_metrics_doc()](../generic/stats_builder/index.html#vectorbt.generic.stats_builder.StatsBuilderMixin.override_metrics_doc "vectorbt.generic.accessors.GenericDFAccessor.override_metrics_doc")
  * [StatsBuilderMixin.stats()](../generic/stats_builder/index.html#vectorbt.generic.stats_builder.StatsBuilderMixin.stats "vectorbt.generic.accessors.GenericDFAccessor.stats")
  * [Wrapping.regroup()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.regroup "vectorbt.generic.accessors.GenericDFAccessor.regroup")
  * [Wrapping.select_one()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.select_one "vectorbt.generic.accessors.GenericDFAccessor.select_one")
  * [Wrapping.select_one_from_obj()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.select_one_from_obj "vectorbt.generic.accessors.GenericDFAccessor.select_one_from_obj")

* * *

### close property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L147-L150 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.close "Permanent link")

Close series.

* * *

### column_names property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L116-L122 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.column_names "Permanent link")

Column names.

* * *

### get_column method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L124-L130 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.get_column "Permanent link")

    OHLCVDFAccessor.get_column(
        col_name
    )

Get column from [OHLCVDFAccessor.column_names](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.column_names "vectorbt.ohlcv_accessors.OHLCVDFAccessor.column_names").

* * *

### high property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L137-L140 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.high "Permanent link")

High series.

* * *

### low property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L142-L145 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.low "Permanent link")

Low series.

* * *

### metrics class variable[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.metrics "Permanent link")

Metrics supported by [OHLCVDFAccessor](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor "vectorbt.ohlcv_accessors.OHLCVDFAccessor").

    Config({
        "start": {
            "title": "Start",
            "calc_func": "<function OHLCVDFAccessor.<lambda> at 0x12ddbb380>",
            "agg_func": null,
            "tags": "wrapper"
        },
        "end": {
            "title": "End",
            "calc_func": "<function OHLCVDFAccessor.<lambda> at 0x12ddbb420>",
            "agg_func": null,
            "tags": "wrapper"
        },
        "period": {
            "title": "Period",
            "calc_func": "<function OHLCVDFAccessor.<lambda> at 0x12ddbb4c0>",
            "apply_to_timedelta": true,
            "agg_func": null,
            "tags": "wrapper"
        },
        "first_price": {
            "title": "First Price",
            "calc_func": "<function OHLCVDFAccessor.<lambda> at 0x12ddbb560>",
            "resolve_ohlc": true,
            "tags": [
                "ohlcv",
                "ohlc"
            ]
        },
        "lowest_price": {
            "title": "Lowest Price",
            "calc_func": "<function OHLCVDFAccessor.<lambda> at 0x12ddbb600>",
            "resolve_ohlc": true,
            "tags": [
                "ohlcv",
                "ohlc"
            ]
        },
        "highest_price": {
            "title": "Highest Price",
            "calc_func": "<function OHLCVDFAccessor.<lambda> at 0x12ddbb6a0>",
            "resolve_ohlc": true,
            "tags": [
                "ohlcv",
                "ohlc"
            ]
        },
        "last_price": {
            "title": "Last Price",
            "calc_func": "<function OHLCVDFAccessor.<lambda> at 0x12ddbb740>",
            "resolve_ohlc": true,
            "tags": [
                "ohlcv",
                "ohlc"
            ]
        },
        "first_volume": {
            "title": "First Volume",
            "calc_func": "<function OHLCVDFAccessor.<lambda> at 0x12ddbb7e0>",
            "resolve_volume": true,
            "tags": [
                "ohlcv",
                "volume"
            ]
        },
        "lowest_volume": {
            "title": "Lowest Volume",
            "calc_func": "<function OHLCVDFAccessor.<lambda> at 0x12ddbb880>",
            "resolve_volume": true,
            "tags": [
                "ohlcv",
                "volume"
            ]
        },
        "highest_volume": {
            "title": "Highest Volume",
            "calc_func": "<function OHLCVDFAccessor.<lambda> at 0x12ddbb920>",
            "resolve_volume": true,
            "tags": [
                "ohlcv",
                "volume"
            ]
        },
        "last_volume": {
            "title": "Last Volume",
            "calc_func": "<function OHLCVDFAccessor.<lambda> at 0x12ddbb9c0>",
            "resolve_volume": true,
            "tags": [
                "ohlcv",
                "volume"
            ]
        }
    })

Returns `OHLCVDFAccessor._metrics`, which gets (deep) copied upon creation of each instance. Thus, changing this config won't affect the class.

To change metrics, you can either change the config in-place, override this property, or overwrite the instance variable `OHLCVDFAccessor._metrics`.

* * *

### ohlc property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L152-L166 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.ohlc "Permanent link")

Open, high, low, and close series.

* * *

### open property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L132-L135 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.open "Permanent link")

Open series.

* * *

### plot method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L268-L397 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.plot "Permanent link")

    OHLCVDFAccessor.plot(
        plot_type=None,
        show_volume=None,
        ohlc_kwargs=None,
        volume_kwargs=None,
        ohlc_add_trace_kwargs=None,
        volume_add_trace_kwargs=None,
        fig=None,
        **layout_kwargs
    )

Plot OHLCV data.

**Args**

**`plot_type`**

Either 'OHLC', 'Candlestick' or Plotly trace.

Pass None to use the default.

**`show_volume`** : `bool`
    If True, shows volume as bar chart.
**`ohlc_kwargs`** : `dict`
    Keyword arguments passed to `plot_type`.
**`volume_kwargs`** : `dict`
    Keyword arguments passed to `plotly.graph_objects.Bar`.
**`ohlc_add_trace_kwargs`** : `dict`
    Keyword arguments passed to `add_trace` for OHLC.
**`volume_add_trace_kwargs`** : `dict`
    Keyword arguments passed to `add_trace` for volume.
**`fig`** : `Figure` or `FigureWidget`
    Figure to add traces to.
**`**layout_kwargs`**
    Keyword arguments for layout.

**Usage**

    >>> import vectorbt as vbt
    
    >>> vbt.YFData.download("BTC-USD").get().vbt.ohlcv.plot()

![](../../assets/images/ohlcv_plot.svg)

* * *

### plots_defaults property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L399-L411 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.plots_defaults "Permanent link")

Defaults for [PlotsBuilderMixin.plots()](../generic/plots_builder/index.html#vectorbt.generic.plots_builder.PlotsBuilderMixin.plots "vectorbt.ohlcv_accessors.OHLCVDFAccessor.plots").

Merges [GenericAccessor.plots_defaults](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.plots_defaults "vectorbt.generic.accessors.GenericAccessor.plots_defaults") and `ohlcv.plots` from [settings](../_settings/index.html#vectorbt._settings.settings "vectorbt._settings.settings").

* * *

### stats_defaults property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L175-L187 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.stats_defaults "Permanent link")

Defaults for [StatsBuilderMixin.stats()](../generic/stats_builder/index.html#vectorbt.generic.stats_builder.StatsBuilderMixin.stats "vectorbt.ohlcv_accessors.OHLCVDFAccessor.stats").

Merges [GenericAccessor.stats_defaults](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.stats_defaults "vectorbt.generic.accessors.GenericAccessor.stats_defaults") and `ohlcv.stats` from [settings](../_settings/index.html#vectorbt._settings.settings "vectorbt._settings.settings").

* * *

### subplots class variable[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.subplots "Permanent link")

Subplots supported by [OHLCVDFAccessor](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor "vectorbt.ohlcv_accessors.OHLCVDFAccessor").

    Config({
        "plot": {
            "title": "OHLC",
            "xaxis_kwargs": {
                "showgrid": true,
                "rangeslider_visible": false
            },
            "yaxis_kwargs": {
                "showgrid": true
            },
            "check_is_not_grouped": true,
            "plot_func": "plot",
            "show_volume": false,
            "tags": "ohlcv"
        }
    })

Returns `OHLCVDFAccessor._subplots`, which gets (deep) copied upon creation of each instance. Thus, changing this config won't affect the class.

To change subplots, you can either change the config in-place, override this property, or overwrite the instance variable `OHLCVDFAccessor._subplots`.

* * *

### volume property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/ohlcv_accessors.py#L168-L171 "Jump to source")[¶](index.html#vectorbt.ohlcv_accessors.OHLCVDFAccessor.volume "Permanent link")

Volume series.