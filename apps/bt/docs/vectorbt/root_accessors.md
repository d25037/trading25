# root_accessors module¶

# root_accessors module[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py "Jump to source")[¶](index.html#vectorbt.root_accessors "Permanent link")

Root pandas accessors.

An accessor adds additional “namespace” to pandas objects.

The [vectorbt.root_accessors](index.html "vectorbt.root_accessors") registers a custom `vbt` accessor on top of each `pd.Series` and `pd.DataFrame` object. It is the main entry point for all other accessors:

    vbt.base.accessors.BaseSR/DFAccessor           -> pd.Series/DataFrame.vbt.*
    vbt.generic.accessors.GenericSR/DFAccessor     -> pd.Series/DataFrame.vbt.*
    vbt.signals.accessors.SignalsSR/DFAccessor     -> pd.Series/DataFrame.vbt.signals.*
    vbt.returns.accessors.ReturnsSR/DFAccessor     -> pd.Series/DataFrame.vbt.returns.*
    vbt.ohlcv.accessors.OHLCVDFAccessor            -> pd.DataFrame.vbt.ohlc.* and pd.DataFrame.vbt.ohlcv.*
    vbt.px_accessors.PXAccessor                    -> pd.DataFrame.vbt.px.*

Additionally, some accessors subclass other accessors building the following inheritance hierarchy:

    vbt.base.accessors.BaseSR/DFAccessor
        -> vbt.generic.accessors.GenericSR/DFAccessor
            -> vbt.cat_accessors.CatSR/DFAccessor
            -> vbt.signals.accessors.SignalsSR/DFAccessor
            -> vbt.returns.accessors.ReturnsSR/DFAccessor
            -> vbt.ohlcv_accessors.OHLCVDFAccessor
        -> vbt.px_accessors.PXSR/DFAccessor

So, for example, the method `pd.Series.vbt.to_2d_array` is also available as `pd.Series.vbt.returns.to_2d_array`.

Note

Accessors in vectorbt are not cached, so querying `df.vbt` twice will also call [Vbt_DFAccessor](index.html#vectorbt.root_accessors.Vbt_DFAccessor "vectorbt.root_accessors.Vbt_DFAccessor") twice.

* * *

## register_accessor function[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py#L75-L93 "Jump to source")[¶](index.html#vectorbt.root_accessors.register_accessor "Permanent link")

    register_accessor(
        name,
        cls
    )

Register a custom accessor.

`cls` should subclass `pandas.core.accessor.DirNamesMixin`.

* * *

## register_dataframe_accessor function[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py#L101-L103 "Jump to source")[¶](index.html#vectorbt.root_accessors.register_dataframe_accessor "Permanent link")

    register_dataframe_accessor(
        name
    )

Decorator to register a custom `pd.DataFrame` accessor on top of the `pd.DataFrame`.

* * *

## register_dataframe_vbt_accessor function[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py#L134-L136 "Jump to source")[¶](index.html#vectorbt.root_accessors.register_dataframe_vbt_accessor "Permanent link")

    register_dataframe_vbt_accessor(
        name,
        parent=vectorbt.root_accessors.Vbt_DFAccessor
    )

Decorator to register a `pd.DataFrame` accessor on top of a parent accessor.

* * *

## register_series_accessor function[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py#L96-L98 "Jump to source")[¶](index.html#vectorbt.root_accessors.register_series_accessor "Permanent link")

    register_series_accessor(
        name
    )

Decorator to register a custom `pd.Series` accessor on top of the `pd.Series`.

* * *

## register_series_vbt_accessor function[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py#L129-L131 "Jump to source")[¶](index.html#vectorbt.root_accessors.register_series_vbt_accessor "Permanent link")

    register_series_vbt_accessor(
        name,
        parent=vectorbt.root_accessors.Vbt_SRAccessor
    )

Decorator to register a `pd.Series` accessor on top of a parent accessor.

* * *

## Accessor class[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py#L51-L72 "Jump to source")[¶](index.html#vectorbt.root_accessors.Accessor "Permanent link")

    Accessor(
        name,
        accessor
    )

Custom property-like object.

Note

In contrast to other pandas accessors, this accessor is not cached!

This prevents from using old data if the object has been changed in-place.

* * *

## Vbt_DFAccessor class[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py#L118-L126 "Jump to source")[¶](index.html#vectorbt.root_accessors.Vbt_DFAccessor "Permanent link")

    Vbt_DFAccessor(
        obj,
        **kwargs
    )

The main vectorbt accessor for `pd.DataFrame`.

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
  * `pandas.core.accessor.DirNamesMixin`

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
  * [GenericAccessor.plot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.plot "vectorbt.generic.accessors.GenericDFAccessor.plot")
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
  * [GenericDFAccessor.plots_defaults](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.plots_defaults "vectorbt.generic.accessors.GenericDFAccessor.plots_defaults")
  * [GenericDFAccessor.ranges](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.ranges "vectorbt.generic.accessors.GenericDFAccessor.ranges")
  * [GenericDFAccessor.self_aliases](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.self_aliases "vectorbt.generic.accessors.GenericDFAccessor.self_aliases")
  * [GenericDFAccessor.squeeze_grouped()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericDFAccessor.squeeze_grouped "vectorbt.generic.accessors.GenericDFAccessor.squeeze_grouped")
  * [GenericDFAccessor.sr_accessor_cls](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.sr_accessor_cls "vectorbt.generic.accessors.GenericDFAccessor.sr_accessor_cls")
  * [GenericDFAccessor.stats_defaults](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.stats_defaults "vectorbt.generic.accessors.GenericDFAccessor.stats_defaults")
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

### ohlc class variable[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py "Jump to source")[¶](index.html#vectorbt.root_accessors.Vbt_DFAccessor.ohlc "Permanent link")

Accessor on top of OHLCV data. For DataFrames only.

Accessible through `pd.DataFrame.vbt.ohlcv`.

* * *

### ohlcv class variable[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py "Jump to source")[¶](index.html#vectorbt.root_accessors.Vbt_DFAccessor.ohlcv "Permanent link")

Accessor on top of OHLCV data. For DataFrames only.

Accessible through `pd.DataFrame.vbt.ohlcv`.

* * *

### px class variable[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py "Jump to source")[¶](index.html#vectorbt.root_accessors.Vbt_DFAccessor.px "Permanent link")

Accessor for running Plotly Express functions. For DataFrames only.

Accessible through `pd.DataFrame.vbt.px`.

* * *

### returns class variable[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py "Jump to source")[¶](index.html#vectorbt.root_accessors.Vbt_DFAccessor.returns "Permanent link")

Accessor on top of return series. For DataFrames only.

Accessible through `pd.DataFrame.vbt.returns`.

* * *

### signals class variable[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py "Jump to source")[¶](index.html#vectorbt.root_accessors.Vbt_DFAccessor.signals "Permanent link")

Accessor on top of signal series. For DataFrames only.

Accessible through `pd.DataFrame.vbt.signals`.

* * *

## Vbt_SRAccessor class[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py#L107-L115 "Jump to source")[¶](index.html#vectorbt.root_accessors.Vbt_SRAccessor "Permanent link")

    Vbt_SRAccessor(
        obj,
        **kwargs
    )

The main vectorbt accessor for `pd.Series`.

**Superclasses**

  * [AttrResolver](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver "vectorbt.utils.attr_.AttrResolver")
  * [BaseAccessor](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor "vectorbt.base.accessors.BaseAccessor")
  * [BaseSRAccessor](../base/accessors/index.html#vectorbt.base.accessors.BaseSRAccessor "vectorbt.base.accessors.BaseSRAccessor")
  * [Configured](../utils/config/index.html#vectorbt.utils.config.Configured "vectorbt.utils.config.Configured")
  * [Documented](../utils/docs/index.html#vectorbt.utils.docs.Documented "vectorbt.utils.docs.Documented")
  * [GenericAccessor](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor "vectorbt.generic.accessors.GenericAccessor")
  * [GenericSRAccessor](../generic/accessors/index.html#vectorbt.generic.accessors.GenericSRAccessor "vectorbt.generic.accessors.GenericSRAccessor")
  * [IndexingBase](../base/indexing/index.html#vectorbt.base.indexing.IndexingBase "vectorbt.base.indexing.IndexingBase")
  * [PandasIndexer](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer "vectorbt.base.indexing.PandasIndexer")
  * [Pickleable](../utils/config/index.html#vectorbt.utils.config.Pickleable "vectorbt.utils.config.Pickleable")
  * [PlotsBuilderMixin](../generic/plots_builder/index.html#vectorbt.generic.plots_builder.PlotsBuilderMixin "vectorbt.generic.plots_builder.PlotsBuilderMixin")
  * [StatsBuilderMixin](../generic/stats_builder/index.html#vectorbt.generic.stats_builder.StatsBuilderMixin "vectorbt.generic.stats_builder.StatsBuilderMixin")
  * [Wrapping](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping "vectorbt.base.array_wrapper.Wrapping")
  * `pandas.core.accessor.DirNamesMixin`

**Inherited members**

  * [AttrResolver.deep_getattr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.deep_getattr "vectorbt.generic.accessors.GenericSRAccessor.deep_getattr")
  * [AttrResolver.post_resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.post_resolve_attr "vectorbt.generic.accessors.GenericSRAccessor.post_resolve_attr")
  * [AttrResolver.pre_resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.pre_resolve_attr "vectorbt.generic.accessors.GenericSRAccessor.pre_resolve_attr")
  * [AttrResolver.resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.resolve_attr "vectorbt.generic.accessors.GenericSRAccessor.resolve_attr")
  * [BaseAccessor.align_to()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.align_to "vectorbt.generic.accessors.GenericSRAccessor.align_to")
  * [BaseAccessor.apply()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply "vectorbt.generic.accessors.GenericSRAccessor.apply")
  * [BaseAccessor.apply_and_concat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply_and_concat "vectorbt.generic.accessors.GenericSRAccessor.apply_and_concat")
  * [BaseAccessor.apply_on_index()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply_on_index "vectorbt.generic.accessors.GenericSRAccessor.apply_on_index")
  * [BaseAccessor.broadcast()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.broadcast "vectorbt.generic.accessors.GenericSRAccessor.broadcast")
  * [BaseAccessor.broadcast_to()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.broadcast_to "vectorbt.generic.accessors.GenericSRAccessor.broadcast_to")
  * [BaseAccessor.combine()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.combine "vectorbt.generic.accessors.GenericSRAccessor.combine")
  * [BaseAccessor.concat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.concat "vectorbt.generic.accessors.GenericSRAccessor.concat")
  * [BaseAccessor.drop_duplicate_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_duplicate_levels "vectorbt.generic.accessors.GenericSRAccessor.drop_duplicate_levels")
  * [BaseAccessor.drop_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_levels "vectorbt.generic.accessors.GenericSRAccessor.drop_levels")
  * [BaseAccessor.drop_redundant_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_redundant_levels "vectorbt.generic.accessors.GenericSRAccessor.drop_redundant_levels")
  * [BaseAccessor.empty()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.empty "vectorbt.generic.accessors.GenericSRAccessor.empty")
  * [BaseAccessor.empty_like()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.empty_like "vectorbt.generic.accessors.GenericSRAccessor.empty_like")
  * [BaseAccessor.indexing_func()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.indexing_func "vectorbt.generic.accessors.GenericSRAccessor.indexing_func")
  * [BaseAccessor.make_symmetric()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.make_symmetric "vectorbt.generic.accessors.GenericSRAccessor.make_symmetric")
  * [BaseAccessor.rename_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.rename_levels "vectorbt.generic.accessors.GenericSRAccessor.rename_levels")
  * [BaseAccessor.repeat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.repeat "vectorbt.generic.accessors.GenericSRAccessor.repeat")
  * [BaseAccessor.select_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.select_levels "vectorbt.generic.accessors.GenericSRAccessor.select_levels")
  * [BaseAccessor.stack_index()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.stack_index "vectorbt.generic.accessors.GenericSRAccessor.stack_index")
  * [BaseAccessor.tile()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.tile "vectorbt.generic.accessors.GenericSRAccessor.tile")
  * [BaseAccessor.to_1d_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_1d_array "vectorbt.generic.accessors.GenericSRAccessor.to_1d_array")
  * [BaseAccessor.to_2d_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_2d_array "vectorbt.generic.accessors.GenericSRAccessor.to_2d_array")
  * [BaseAccessor.to_dict()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_dict "vectorbt.generic.accessors.GenericSRAccessor.to_dict")
  * [BaseAccessor.unstack_to_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.unstack_to_array "vectorbt.generic.accessors.GenericSRAccessor.unstack_to_array")
  * [BaseAccessor.unstack_to_df()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.unstack_to_df "vectorbt.generic.accessors.GenericSRAccessor.unstack_to_df")
  * [Configured.copy()](../utils/config/index.html#vectorbt.utils.config.Configured.copy "vectorbt.generic.accessors.GenericSRAccessor.copy")
  * [Configured.dumps()](../utils/config/index.html#vectorbt.utils.config.Pickleable.dumps "vectorbt.generic.accessors.GenericSRAccessor.dumps")
  * [Configured.loads()](../utils/config/index.html#vectorbt.utils.config.Pickleable.loads "vectorbt.generic.accessors.GenericSRAccessor.loads")
  * [Configured.replace()](../utils/config/index.html#vectorbt.utils.config.Configured.replace "vectorbt.generic.accessors.GenericSRAccessor.replace")
  * [Configured.to_doc()](../utils/docs/index.html#vectorbt.utils.docs.Documented.to_doc "vectorbt.generic.accessors.GenericSRAccessor.to_doc")
  * [Configured.update_config()](../utils/config/index.html#vectorbt.utils.config.Configured.update_config "vectorbt.generic.accessors.GenericSRAccessor.update_config")
  * [GenericAccessor.apply_along_axis()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.apply_along_axis "vectorbt.generic.accessors.GenericSRAccessor.apply_along_axis")
  * [GenericAccessor.apply_and_reduce()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.apply_and_reduce "vectorbt.generic.accessors.GenericSRAccessor.apply_and_reduce")
  * [GenericAccessor.apply_mapping()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.apply_mapping "vectorbt.generic.accessors.GenericSRAccessor.apply_mapping")
  * [GenericAccessor.applymap()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.applymap "vectorbt.generic.accessors.GenericSRAccessor.applymap")
  * [GenericAccessor.barplot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.barplot "vectorbt.generic.accessors.GenericSRAccessor.barplot")
  * [GenericAccessor.bfill()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.bfill "vectorbt.generic.accessors.GenericSRAccessor.bfill")
  * [GenericAccessor.binarize()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.binarize "vectorbt.generic.accessors.GenericSRAccessor.binarize")
  * [GenericAccessor.boxplot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.boxplot "vectorbt.generic.accessors.GenericSRAccessor.boxplot")
  * [GenericAccessor.bshift()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.bshift "vectorbt.generic.accessors.GenericSRAccessor.bshift")
  * [GenericAccessor.count()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.count "vectorbt.generic.accessors.GenericSRAccessor.count")
  * [GenericAccessor.crossed_above()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.crossed_above "vectorbt.generic.accessors.GenericSRAccessor.crossed_above")
  * [GenericAccessor.crossed_below()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.crossed_below "vectorbt.generic.accessors.GenericSRAccessor.crossed_below")
  * [GenericAccessor.cumprod()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.cumprod "vectorbt.generic.accessors.GenericSRAccessor.cumprod")
  * [GenericAccessor.cumsum()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.cumsum "vectorbt.generic.accessors.GenericSRAccessor.cumsum")
  * [GenericAccessor.describe()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.describe "vectorbt.generic.accessors.GenericSRAccessor.describe")
  * [GenericAccessor.diff()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.diff "vectorbt.generic.accessors.GenericSRAccessor.diff")
  * [GenericAccessor.drawdown()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.drawdown "vectorbt.generic.accessors.GenericSRAccessor.drawdown")
  * [GenericAccessor.ewm_mean()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.ewm_mean "vectorbt.generic.accessors.GenericSRAccessor.ewm_mean")
  * [GenericAccessor.ewm_std()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.ewm_std "vectorbt.generic.accessors.GenericSRAccessor.ewm_std")
  * [GenericAccessor.expanding_apply()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_apply "vectorbt.generic.accessors.GenericSRAccessor.expanding_apply")
  * [GenericAccessor.expanding_max()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_max "vectorbt.generic.accessors.GenericSRAccessor.expanding_max")
  * [GenericAccessor.expanding_mean()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_mean "vectorbt.generic.accessors.GenericSRAccessor.expanding_mean")
  * [GenericAccessor.expanding_min()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_min "vectorbt.generic.accessors.GenericSRAccessor.expanding_min")
  * [GenericAccessor.expanding_split()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_split "vectorbt.generic.accessors.GenericSRAccessor.expanding_split")
  * [GenericAccessor.expanding_std()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.expanding_std "vectorbt.generic.accessors.GenericSRAccessor.expanding_std")
  * [GenericAccessor.ffill()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.ffill "vectorbt.generic.accessors.GenericSRAccessor.ffill")
  * [GenericAccessor.fillna()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.fillna "vectorbt.generic.accessors.GenericSRAccessor.fillna")
  * [GenericAccessor.filter()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.filter "vectorbt.generic.accessors.GenericSRAccessor.filter")
  * [GenericAccessor.fshift()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.fshift "vectorbt.generic.accessors.GenericSRAccessor.fshift")
  * [GenericAccessor.get_drawdowns()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.get_drawdowns "vectorbt.generic.accessors.GenericSRAccessor.get_drawdowns")
  * [GenericAccessor.get_ranges()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.get_ranges "vectorbt.generic.accessors.GenericSRAccessor.get_ranges")
  * [GenericAccessor.groupby_apply()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.groupby_apply "vectorbt.generic.accessors.GenericSRAccessor.groupby_apply")
  * [GenericAccessor.histplot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.histplot "vectorbt.generic.accessors.GenericSRAccessor.histplot")
  * [GenericAccessor.idxmax()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.idxmax "vectorbt.generic.accessors.GenericSRAccessor.idxmax")
  * [GenericAccessor.idxmin()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.idxmin "vectorbt.generic.accessors.GenericSRAccessor.idxmin")
  * [GenericAccessor.lineplot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.lineplot "vectorbt.generic.accessors.GenericSRAccessor.lineplot")
  * [GenericAccessor.max()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.max "vectorbt.generic.accessors.GenericSRAccessor.max")
  * [GenericAccessor.maxabs_scale()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.maxabs_scale "vectorbt.generic.accessors.GenericSRAccessor.maxabs_scale")
  * [GenericAccessor.mean()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.mean "vectorbt.generic.accessors.GenericSRAccessor.mean")
  * [GenericAccessor.median()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.median "vectorbt.generic.accessors.GenericSRAccessor.median")
  * [GenericAccessor.min()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.min "vectorbt.generic.accessors.GenericSRAccessor.min")
  * [GenericAccessor.minmax_scale()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.minmax_scale "vectorbt.generic.accessors.GenericSRAccessor.minmax_scale")
  * [GenericAccessor.normalize()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.normalize "vectorbt.generic.accessors.GenericSRAccessor.normalize")
  * [GenericAccessor.pct_change()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.pct_change "vectorbt.generic.accessors.GenericSRAccessor.pct_change")
  * [GenericAccessor.plot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.plot "vectorbt.generic.accessors.GenericSRAccessor.plot")
  * [GenericAccessor.power_transform()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.power_transform "vectorbt.generic.accessors.GenericSRAccessor.power_transform")
  * [GenericAccessor.product()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.product "vectorbt.generic.accessors.GenericSRAccessor.product")
  * [GenericAccessor.quantile_transform()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.quantile_transform "vectorbt.generic.accessors.GenericSRAccessor.quantile_transform")
  * [GenericAccessor.range_split()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.range_split "vectorbt.generic.accessors.GenericSRAccessor.range_split")
  * [GenericAccessor.rebase()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rebase "vectorbt.generic.accessors.GenericSRAccessor.rebase")
  * [GenericAccessor.reduce()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.reduce "vectorbt.generic.accessors.GenericSRAccessor.reduce")
  * [GenericAccessor.resample_apply()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.resample_apply "vectorbt.generic.accessors.GenericSRAccessor.resample_apply")
  * [GenericAccessor.resolve_self()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.resolve_self "vectorbt.generic.accessors.GenericSRAccessor.resolve_self")
  * [GenericAccessor.robust_scale()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.robust_scale "vectorbt.generic.accessors.GenericSRAccessor.robust_scale")
  * [GenericAccessor.rolling_apply()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_apply "vectorbt.generic.accessors.GenericSRAccessor.rolling_apply")
  * [GenericAccessor.rolling_max()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_max "vectorbt.generic.accessors.GenericSRAccessor.rolling_max")
  * [GenericAccessor.rolling_mean()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_mean "vectorbt.generic.accessors.GenericSRAccessor.rolling_mean")
  * [GenericAccessor.rolling_min()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_min "vectorbt.generic.accessors.GenericSRAccessor.rolling_min")
  * [GenericAccessor.rolling_split()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_split "vectorbt.generic.accessors.GenericSRAccessor.rolling_split")
  * [GenericAccessor.rolling_std()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.rolling_std "vectorbt.generic.accessors.GenericSRAccessor.rolling_std")
  * [GenericAccessor.scale()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.scale "vectorbt.generic.accessors.GenericSRAccessor.scale")
  * [GenericAccessor.scatterplot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.scatterplot "vectorbt.generic.accessors.GenericSRAccessor.scatterplot")
  * [GenericAccessor.shuffle()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.shuffle "vectorbt.generic.accessors.GenericSRAccessor.shuffle")
  * [GenericAccessor.split()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.split "vectorbt.generic.accessors.GenericSRAccessor.split")
  * [GenericAccessor.std()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.std "vectorbt.generic.accessors.GenericSRAccessor.std")
  * [GenericAccessor.sum()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.sum "vectorbt.generic.accessors.GenericSRAccessor.sum")
  * [GenericAccessor.to_mapped()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.to_mapped "vectorbt.generic.accessors.GenericSRAccessor.to_mapped")
  * [GenericAccessor.to_returns()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.to_returns "vectorbt.generic.accessors.GenericSRAccessor.to_returns")
  * [GenericAccessor.transform()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.transform "vectorbt.generic.accessors.GenericSRAccessor.transform")
  * [GenericAccessor.value_counts()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.value_counts "vectorbt.generic.accessors.GenericSRAccessor.value_counts")
  * [GenericAccessor.zscore()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.zscore "vectorbt.generic.accessors.GenericSRAccessor.zscore")
  * [GenericSRAccessor.config](../utils/config/index.html#vectorbt.utils.config.Configured.config "vectorbt.generic.accessors.GenericSRAccessor.config")
  * [GenericSRAccessor.df_accessor_cls](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.df_accessor_cls "vectorbt.generic.accessors.GenericSRAccessor.df_accessor_cls")
  * [GenericSRAccessor.drawdowns](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.drawdowns "vectorbt.generic.accessors.GenericSRAccessor.drawdowns")
  * [GenericSRAccessor.flatten_grouped()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericSRAccessor.flatten_grouped "vectorbt.generic.accessors.GenericSRAccessor.flatten_grouped")
  * [GenericSRAccessor.heatmap()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericSRAccessor.heatmap "vectorbt.generic.accessors.GenericSRAccessor.heatmap")
  * [GenericSRAccessor.iloc](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.iloc "vectorbt.generic.accessors.GenericSRAccessor.iloc")
  * [GenericSRAccessor.indexing_kwargs](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.indexing_kwargs "vectorbt.generic.accessors.GenericSRAccessor.indexing_kwargs")
  * [GenericSRAccessor.loc](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.loc "vectorbt.generic.accessors.GenericSRAccessor.loc")
  * [GenericSRAccessor.mapping](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.mapping "vectorbt.generic.accessors.GenericSRAccessor.mapping")
  * [GenericSRAccessor.obj](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.obj "vectorbt.generic.accessors.GenericSRAccessor.obj")
  * [GenericSRAccessor.overlay_with_heatmap()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericSRAccessor.overlay_with_heatmap "vectorbt.generic.accessors.GenericSRAccessor.overlay_with_heatmap")
  * [GenericSRAccessor.plot_against()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericSRAccessor.plot_against "vectorbt.generic.accessors.GenericSRAccessor.plot_against")
  * [GenericSRAccessor.plots_defaults](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.plots_defaults "vectorbt.generic.accessors.GenericSRAccessor.plots_defaults")
  * [GenericSRAccessor.qqplot()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericSRAccessor.qqplot "vectorbt.generic.accessors.GenericSRAccessor.qqplot")
  * [GenericSRAccessor.ranges](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.ranges "vectorbt.generic.accessors.GenericSRAccessor.ranges")
  * [GenericSRAccessor.self_aliases](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.self_aliases "vectorbt.generic.accessors.GenericSRAccessor.self_aliases")
  * [GenericSRAccessor.squeeze_grouped()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericSRAccessor.squeeze_grouped "vectorbt.generic.accessors.GenericSRAccessor.squeeze_grouped")
  * [GenericSRAccessor.sr_accessor_cls](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.sr_accessor_cls "vectorbt.generic.accessors.GenericSRAccessor.sr_accessor_cls")
  * [GenericSRAccessor.stats_defaults](../generic/accessors/index.html#vectorbt.generic.accessors.GenericAccessor.stats_defaults "vectorbt.generic.accessors.GenericSRAccessor.stats_defaults")
  * [GenericSRAccessor.ts_heatmap()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericSRAccessor.ts_heatmap "vectorbt.generic.accessors.GenericSRAccessor.ts_heatmap")
  * [GenericSRAccessor.volume()](../generic/accessors/index.html#vectorbt.generic.accessors.GenericSRAccessor.volume "vectorbt.generic.accessors.GenericSRAccessor.volume")
  * [GenericSRAccessor.wrapper](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.wrapper "vectorbt.generic.accessors.GenericSRAccessor.wrapper")
  * [GenericSRAccessor.writeable_attrs](../utils/config/index.html#vectorbt.utils.config.Configured.writeable_attrs "vectorbt.generic.accessors.GenericSRAccessor.writeable_attrs")
  * [PandasIndexer.xs()](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.xs "vectorbt.generic.accessors.GenericSRAccessor.xs")
  * [Pickleable.load()](../utils/config/index.html#vectorbt.utils.config.Pickleable.load "vectorbt.generic.accessors.GenericSRAccessor.load")
  * [Pickleable.save()](../utils/config/index.html#vectorbt.utils.config.Pickleable.save "vectorbt.generic.accessors.GenericSRAccessor.save")
  * [PlotsBuilderMixin.build_subplots_doc()](../generic/plots_builder/index.html#vectorbt.generic.plots_builder.PlotsBuilderMixin.build_subplots_doc "vectorbt.generic.accessors.GenericSRAccessor.build_subplots_doc")
  * [PlotsBuilderMixin.override_subplots_doc()](../generic/plots_builder/index.html#vectorbt.generic.plots_builder.PlotsBuilderMixin.override_subplots_doc "vectorbt.generic.accessors.GenericSRAccessor.override_subplots_doc")
  * [PlotsBuilderMixin.plots()](../generic/plots_builder/index.html#vectorbt.generic.plots_builder.PlotsBuilderMixin.plots "vectorbt.generic.accessors.GenericSRAccessor.plots")
  * [StatsBuilderMixin.build_metrics_doc()](../generic/stats_builder/index.html#vectorbt.generic.stats_builder.StatsBuilderMixin.build_metrics_doc "vectorbt.generic.accessors.GenericSRAccessor.build_metrics_doc")
  * [StatsBuilderMixin.override_metrics_doc()](../generic/stats_builder/index.html#vectorbt.generic.stats_builder.StatsBuilderMixin.override_metrics_doc "vectorbt.generic.accessors.GenericSRAccessor.override_metrics_doc")
  * [StatsBuilderMixin.stats()](../generic/stats_builder/index.html#vectorbt.generic.stats_builder.StatsBuilderMixin.stats "vectorbt.generic.accessors.GenericSRAccessor.stats")
  * [Wrapping.regroup()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.regroup "vectorbt.generic.accessors.GenericSRAccessor.regroup")
  * [Wrapping.select_one()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.select_one "vectorbt.generic.accessors.GenericSRAccessor.select_one")
  * [Wrapping.select_one_from_obj()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.select_one_from_obj "vectorbt.generic.accessors.GenericSRAccessor.select_one_from_obj")

* * *

### px class variable[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py "Jump to source")[¶](index.html#vectorbt.root_accessors.Vbt_SRAccessor.px "Permanent link")

Accessor for running Plotly Express functions. For Series only.

Accessible through `pd.Series.vbt.px`.

* * *

### returns class variable[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py "Jump to source")[¶](index.html#vectorbt.root_accessors.Vbt_SRAccessor.returns "Permanent link")

Accessor on top of return series. For Series only.

Accessible through `pd.Series.vbt.returns`.

* * *

### signals class variable[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/root_accessors.py "Jump to source")[¶](index.html#vectorbt.root_accessors.Vbt_SRAccessor.signals "Permanent link")

Accessor on top of signal series. For Series only.

Accessible through `pd.Series.vbt.signals`.