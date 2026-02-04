# px_accessors module¶

# px_accessors module[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py "Jump to source")[¶](index.html#vectorbt.px_accessors "Permanent link")

Plotly Express pandas accessors.

Note

Accessors do not utilize caching.

* * *

## attach_px_methods function[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L24-L66 "Jump to source")[¶](index.html#vectorbt.px_accessors.attach_px_methods "Permanent link")

    attach_px_methods(
        cls
    )

Class decorator to attach Plotly Express methods.

* * *

## PXAccessor class[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L69-L89 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor "Permanent link")

    PXAccessor(
        obj,
        **kwargs
    )

Accessor for running Plotly Express functions.

Accessible through `pd.Series.vbt.px` and `pd.DataFrame.vbt.px`.

**Usage**

    >>> import pandas as pd
    >>> import vectorbt as vbt
    
    >>> vbt.settings.set_theme('seaborn')
    
    >>> pd.Series([1, 2, 3]).vbt.px.bar()

![](../../assets/images/px_bar.svg)

**Superclasses**

  * [AttrResolver](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver "vectorbt.utils.attr_.AttrResolver")
  * [BaseAccessor](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor "vectorbt.base.accessors.BaseAccessor")
  * [Configured](../utils/config/index.html#vectorbt.utils.config.Configured "vectorbt.utils.config.Configured")
  * [Documented](../utils/docs/index.html#vectorbt.utils.docs.Documented "vectorbt.utils.docs.Documented")
  * [IndexingBase](../base/indexing/index.html#vectorbt.base.indexing.IndexingBase "vectorbt.base.indexing.IndexingBase")
  * [PandasIndexer](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer "vectorbt.base.indexing.PandasIndexer")
  * [Pickleable](../utils/config/index.html#vectorbt.utils.config.Pickleable "vectorbt.utils.config.Pickleable")
  * [Wrapping](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping "vectorbt.base.array_wrapper.Wrapping")

**Inherited members**

  * [AttrResolver.deep_getattr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.deep_getattr "vectorbt.base.accessors.BaseAccessor.deep_getattr")
  * [AttrResolver.post_resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.post_resolve_attr "vectorbt.base.accessors.BaseAccessor.post_resolve_attr")
  * [AttrResolver.pre_resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.pre_resolve_attr "vectorbt.base.accessors.BaseAccessor.pre_resolve_attr")
  * [AttrResolver.resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.resolve_attr "vectorbt.base.accessors.BaseAccessor.resolve_attr")
  * [BaseAccessor.align_to()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.align_to "vectorbt.base.accessors.BaseAccessor.align_to")
  * [BaseAccessor.apply()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply "vectorbt.base.accessors.BaseAccessor.apply")
  * [BaseAccessor.apply_and_concat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply_and_concat "vectorbt.base.accessors.BaseAccessor.apply_and_concat")
  * [BaseAccessor.apply_on_index()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply_on_index "vectorbt.base.accessors.BaseAccessor.apply_on_index")
  * [BaseAccessor.broadcast()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.broadcast "vectorbt.base.accessors.BaseAccessor.broadcast")
  * [BaseAccessor.broadcast_to()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.broadcast_to "vectorbt.base.accessors.BaseAccessor.broadcast_to")
  * [BaseAccessor.combine()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.combine "vectorbt.base.accessors.BaseAccessor.combine")
  * [BaseAccessor.concat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.concat "vectorbt.base.accessors.BaseAccessor.concat")
  * [BaseAccessor.config](../utils/config/index.html#vectorbt.utils.config.Configured.config "vectorbt.base.accessors.BaseAccessor.config")
  * [BaseAccessor.df_accessor_cls](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.df_accessor_cls "vectorbt.base.accessors.BaseAccessor.df_accessor_cls")
  * [BaseAccessor.drop_duplicate_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_duplicate_levels "vectorbt.base.accessors.BaseAccessor.drop_duplicate_levels")
  * [BaseAccessor.drop_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_levels "vectorbt.base.accessors.BaseAccessor.drop_levels")
  * [BaseAccessor.drop_redundant_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_redundant_levels "vectorbt.base.accessors.BaseAccessor.drop_redundant_levels")
  * [BaseAccessor.empty()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.empty "vectorbt.base.accessors.BaseAccessor.empty")
  * [BaseAccessor.empty_like()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.empty_like "vectorbt.base.accessors.BaseAccessor.empty_like")
  * [BaseAccessor.iloc](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.iloc "vectorbt.base.accessors.BaseAccessor.iloc")
  * [BaseAccessor.indexing_func()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.indexing_func "vectorbt.base.accessors.BaseAccessor.indexing_func")
  * [BaseAccessor.indexing_kwargs](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.indexing_kwargs "vectorbt.base.accessors.BaseAccessor.indexing_kwargs")
  * [BaseAccessor.loc](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.loc "vectorbt.base.accessors.BaseAccessor.loc")
  * [BaseAccessor.make_symmetric()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.make_symmetric "vectorbt.base.accessors.BaseAccessor.make_symmetric")
  * [BaseAccessor.obj](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.obj "vectorbt.base.accessors.BaseAccessor.obj")
  * [BaseAccessor.rename_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.rename_levels "vectorbt.base.accessors.BaseAccessor.rename_levels")
  * [BaseAccessor.repeat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.repeat "vectorbt.base.accessors.BaseAccessor.repeat")
  * [BaseAccessor.select_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.select_levels "vectorbt.base.accessors.BaseAccessor.select_levels")
  * [BaseAccessor.self_aliases](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.self_aliases "vectorbt.base.accessors.BaseAccessor.self_aliases")
  * [BaseAccessor.sr_accessor_cls](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.sr_accessor_cls "vectorbt.base.accessors.BaseAccessor.sr_accessor_cls")
  * [BaseAccessor.stack_index()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.stack_index "vectorbt.base.accessors.BaseAccessor.stack_index")
  * [BaseAccessor.tile()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.tile "vectorbt.base.accessors.BaseAccessor.tile")
  * [BaseAccessor.to_1d_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_1d_array "vectorbt.base.accessors.BaseAccessor.to_1d_array")
  * [BaseAccessor.to_2d_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_2d_array "vectorbt.base.accessors.BaseAccessor.to_2d_array")
  * [BaseAccessor.to_dict()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_dict "vectorbt.base.accessors.BaseAccessor.to_dict")
  * [BaseAccessor.unstack_to_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.unstack_to_array "vectorbt.base.accessors.BaseAccessor.unstack_to_array")
  * [BaseAccessor.unstack_to_df()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.unstack_to_df "vectorbt.base.accessors.BaseAccessor.unstack_to_df")
  * [BaseAccessor.wrapper](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.wrapper "vectorbt.base.accessors.BaseAccessor.wrapper")
  * [BaseAccessor.writeable_attrs](../utils/config/index.html#vectorbt.utils.config.Configured.writeable_attrs "vectorbt.base.accessors.BaseAccessor.writeable_attrs")
  * [Configured.copy()](../utils/config/index.html#vectorbt.utils.config.Configured.copy "vectorbt.base.accessors.BaseAccessor.copy")
  * [Configured.dumps()](../utils/config/index.html#vectorbt.utils.config.Pickleable.dumps "vectorbt.base.accessors.BaseAccessor.dumps")
  * [Configured.loads()](../utils/config/index.html#vectorbt.utils.config.Pickleable.loads "vectorbt.base.accessors.BaseAccessor.loads")
  * [Configured.replace()](../utils/config/index.html#vectorbt.utils.config.Configured.replace "vectorbt.base.accessors.BaseAccessor.replace")
  * [Configured.to_doc()](../utils/docs/index.html#vectorbt.utils.docs.Documented.to_doc "vectorbt.base.accessors.BaseAccessor.to_doc")
  * [Configured.update_config()](../utils/config/index.html#vectorbt.utils.config.Configured.update_config "vectorbt.base.accessors.BaseAccessor.update_config")
  * [PandasIndexer.xs()](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.xs "vectorbt.base.accessors.BaseAccessor.xs")
  * [Pickleable.load()](../utils/config/index.html#vectorbt.utils.config.Pickleable.load "vectorbt.base.accessors.BaseAccessor.load")
  * [Pickleable.save()](../utils/config/index.html#vectorbt.utils.config.Pickleable.save "vectorbt.base.accessors.BaseAccessor.save")
  * [Wrapping.regroup()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.regroup "vectorbt.base.accessors.BaseAccessor.regroup")
  * [Wrapping.resolve_self()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.resolve_self "vectorbt.base.accessors.BaseAccessor.resolve_self")
  * [Wrapping.select_one()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.select_one "vectorbt.base.accessors.BaseAccessor.select_one")
  * [Wrapping.select_one_from_obj()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.select_one_from_obj "vectorbt.base.accessors.BaseAccessor.select_one_from_obj")

**Subclasses**

  * [PXDFAccessor](index.html#vectorbt.px_accessors.PXDFAccessor "vectorbt.px_accessors.PXDFAccessor")
  * [PXSRAccessor](index.html#vectorbt.px_accessors.PXSRAccessor "vectorbt.px_accessors.PXSRAccessor")

* * *

### area method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.area "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### bar method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.bar "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### bar_polar method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.bar_polar "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### box method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.box "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### choropleth method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.choropleth "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### choropleth_map method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.choropleth_map "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### choropleth_mapbox method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.choropleth_mapbox "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### density_contour method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.density_contour "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### density_heatmap method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.density_heatmap "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### density_map method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.density_map "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### density_mapbox method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.density_mapbox "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### ecdf method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.ecdf "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### funnel method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.funnel "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### funnel_area method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.funnel_area "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### histogram method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.histogram "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### icicle method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.icicle "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### imshow method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.imshow "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### line method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.line "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### line_3d method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.line_3d "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### line_geo method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.line_geo "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### line_map method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.line_map "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### line_mapbox method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.line_mapbox "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### line_polar method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.line_polar "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### line_ternary method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.line_ternary "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### parallel_categories method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.parallel_categories "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### parallel_coordinates method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.parallel_coordinates "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### pie method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.pie "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### scatter method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.scatter "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### scatter_3d method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.scatter_3d "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### scatter_geo method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.scatter_geo "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### scatter_map method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.scatter_map "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### scatter_mapbox method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.scatter_mapbox "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### scatter_matrix method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.scatter_matrix "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### scatter_polar method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.scatter_polar "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### scatter_ternary method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.scatter_ternary "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### strip method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.strip "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### sunburst method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.sunburst "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### timeline method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.timeline "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### treemap method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.treemap "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

### violin method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L29-L63 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXAccessor.violin "Permanent link")

    attach_px_methods.<locals>.plot_func(
        *args,
        **kwargs
    )

* * *

## PXDFAccessor class[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L103-L111 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXDFAccessor "Permanent link")

    PXDFAccessor(
        obj,
        **kwargs
    )

Accessor for running Plotly Express functions. For DataFrames only.

Accessible through `pd.DataFrame.vbt.px`.

**Superclasses**

  * [AttrResolver](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver "vectorbt.utils.attr_.AttrResolver")
  * [BaseAccessor](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor "vectorbt.base.accessors.BaseAccessor")
  * [BaseDFAccessor](../base/accessors/index.html#vectorbt.base.accessors.BaseDFAccessor "vectorbt.base.accessors.BaseDFAccessor")
  * [Configured](../utils/config/index.html#vectorbt.utils.config.Configured "vectorbt.utils.config.Configured")
  * [Documented](../utils/docs/index.html#vectorbt.utils.docs.Documented "vectorbt.utils.docs.Documented")
  * [IndexingBase](../base/indexing/index.html#vectorbt.base.indexing.IndexingBase "vectorbt.base.indexing.IndexingBase")
  * [PXAccessor](index.html#vectorbt.px_accessors.PXAccessor "vectorbt.px_accessors.PXAccessor")
  * [PandasIndexer](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer "vectorbt.base.indexing.PandasIndexer")
  * [Pickleable](../utils/config/index.html#vectorbt.utils.config.Pickleable "vectorbt.utils.config.Pickleable")
  * [Wrapping](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping "vectorbt.base.array_wrapper.Wrapping")

**Inherited members**

  * [AttrResolver.deep_getattr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.deep_getattr "vectorbt.px_accessors.PXAccessor.deep_getattr")
  * [AttrResolver.post_resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.post_resolve_attr "vectorbt.px_accessors.PXAccessor.post_resolve_attr")
  * [AttrResolver.pre_resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.pre_resolve_attr "vectorbt.px_accessors.PXAccessor.pre_resolve_attr")
  * [AttrResolver.resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.resolve_attr "vectorbt.px_accessors.PXAccessor.resolve_attr")
  * [BaseAccessor.align_to()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.align_to "vectorbt.px_accessors.PXAccessor.align_to")
  * [BaseAccessor.apply()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply "vectorbt.px_accessors.PXAccessor.apply")
  * [BaseAccessor.apply_and_concat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply_and_concat "vectorbt.px_accessors.PXAccessor.apply_and_concat")
  * [BaseAccessor.apply_on_index()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply_on_index "vectorbt.px_accessors.PXAccessor.apply_on_index")
  * [BaseAccessor.broadcast()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.broadcast "vectorbt.px_accessors.PXAccessor.broadcast")
  * [BaseAccessor.broadcast_to()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.broadcast_to "vectorbt.px_accessors.PXAccessor.broadcast_to")
  * [BaseAccessor.combine()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.combine "vectorbt.px_accessors.PXAccessor.combine")
  * [BaseAccessor.concat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.concat "vectorbt.px_accessors.PXAccessor.concat")
  * [BaseAccessor.drop_duplicate_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_duplicate_levels "vectorbt.px_accessors.PXAccessor.drop_duplicate_levels")
  * [BaseAccessor.drop_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_levels "vectorbt.px_accessors.PXAccessor.drop_levels")
  * [BaseAccessor.drop_redundant_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_redundant_levels "vectorbt.px_accessors.PXAccessor.drop_redundant_levels")
  * [BaseAccessor.empty()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.empty "vectorbt.px_accessors.PXAccessor.empty")
  * [BaseAccessor.empty_like()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.empty_like "vectorbt.px_accessors.PXAccessor.empty_like")
  * [BaseAccessor.indexing_func()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.indexing_func "vectorbt.px_accessors.PXAccessor.indexing_func")
  * [BaseAccessor.make_symmetric()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.make_symmetric "vectorbt.px_accessors.PXAccessor.make_symmetric")
  * [BaseAccessor.rename_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.rename_levels "vectorbt.px_accessors.PXAccessor.rename_levels")
  * [BaseAccessor.repeat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.repeat "vectorbt.px_accessors.PXAccessor.repeat")
  * [BaseAccessor.select_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.select_levels "vectorbt.px_accessors.PXAccessor.select_levels")
  * [BaseAccessor.stack_index()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.stack_index "vectorbt.px_accessors.PXAccessor.stack_index")
  * [BaseAccessor.tile()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.tile "vectorbt.px_accessors.PXAccessor.tile")
  * [BaseAccessor.to_1d_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_1d_array "vectorbt.px_accessors.PXAccessor.to_1d_array")
  * [BaseAccessor.to_2d_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_2d_array "vectorbt.px_accessors.PXAccessor.to_2d_array")
  * [BaseAccessor.to_dict()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_dict "vectorbt.px_accessors.PXAccessor.to_dict")
  * [BaseAccessor.unstack_to_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.unstack_to_array "vectorbt.px_accessors.PXAccessor.unstack_to_array")
  * [BaseAccessor.unstack_to_df()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.unstack_to_df "vectorbt.px_accessors.PXAccessor.unstack_to_df")
  * [Configured.copy()](../utils/config/index.html#vectorbt.utils.config.Configured.copy "vectorbt.px_accessors.PXAccessor.copy")
  * [Configured.dumps()](../utils/config/index.html#vectorbt.utils.config.Pickleable.dumps "vectorbt.px_accessors.PXAccessor.dumps")
  * [Configured.loads()](../utils/config/index.html#vectorbt.utils.config.Pickleable.loads "vectorbt.px_accessors.PXAccessor.loads")
  * [Configured.replace()](../utils/config/index.html#vectorbt.utils.config.Configured.replace "vectorbt.px_accessors.PXAccessor.replace")
  * [Configured.to_doc()](../utils/docs/index.html#vectorbt.utils.docs.Documented.to_doc "vectorbt.px_accessors.PXAccessor.to_doc")
  * [Configured.update_config()](../utils/config/index.html#vectorbt.utils.config.Configured.update_config "vectorbt.px_accessors.PXAccessor.update_config")
  * [PXAccessor.config](../utils/config/index.html#vectorbt.utils.config.Configured.config "vectorbt.px_accessors.PXAccessor.config")
  * [PXAccessor.df_accessor_cls](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.df_accessor_cls "vectorbt.px_accessors.PXAccessor.df_accessor_cls")
  * [PXAccessor.iloc](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.iloc "vectorbt.px_accessors.PXAccessor.iloc")
  * [PXAccessor.indexing_kwargs](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.indexing_kwargs "vectorbt.px_accessors.PXAccessor.indexing_kwargs")
  * [PXAccessor.loc](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.loc "vectorbt.px_accessors.PXAccessor.loc")
  * [PXAccessor.obj](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.obj "vectorbt.px_accessors.PXAccessor.obj")
  * [PXAccessor.self_aliases](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.self_aliases "vectorbt.px_accessors.PXAccessor.self_aliases")
  * [PXAccessor.sr_accessor_cls](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.sr_accessor_cls "vectorbt.px_accessors.PXAccessor.sr_accessor_cls")
  * [PXAccessor.wrapper](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.wrapper "vectorbt.px_accessors.PXAccessor.wrapper")
  * [PXAccessor.writeable_attrs](../utils/config/index.html#vectorbt.utils.config.Configured.writeable_attrs "vectorbt.px_accessors.PXAccessor.writeable_attrs")
  * [PandasIndexer.xs()](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.xs "vectorbt.px_accessors.PXAccessor.xs")
  * [Pickleable.load()](../utils/config/index.html#vectorbt.utils.config.Pickleable.load "vectorbt.px_accessors.PXAccessor.load")
  * [Pickleable.save()](../utils/config/index.html#vectorbt.utils.config.Pickleable.save "vectorbt.px_accessors.PXAccessor.save")
  * [Wrapping.regroup()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.regroup "vectorbt.px_accessors.PXAccessor.regroup")
  * [Wrapping.resolve_self()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.resolve_self "vectorbt.px_accessors.PXAccessor.resolve_self")
  * [Wrapping.select_one()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.select_one "vectorbt.px_accessors.PXAccessor.select_one")
  * [Wrapping.select_one_from_obj()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.select_one_from_obj "vectorbt.px_accessors.PXAccessor.select_one_from_obj")

* * *

## PXSRAccessor class[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/px_accessors.py#L92-L100 "Jump to source")[¶](index.html#vectorbt.px_accessors.PXSRAccessor "Permanent link")

    PXSRAccessor(
        obj,
        **kwargs
    )

Accessor for running Plotly Express functions. For Series only.

Accessible through `pd.Series.vbt.px`.

**Superclasses**

  * [AttrResolver](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver "vectorbt.utils.attr_.AttrResolver")
  * [BaseAccessor](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor "vectorbt.base.accessors.BaseAccessor")
  * [BaseSRAccessor](../base/accessors/index.html#vectorbt.base.accessors.BaseSRAccessor "vectorbt.base.accessors.BaseSRAccessor")
  * [Configured](../utils/config/index.html#vectorbt.utils.config.Configured "vectorbt.utils.config.Configured")
  * [Documented](../utils/docs/index.html#vectorbt.utils.docs.Documented "vectorbt.utils.docs.Documented")
  * [IndexingBase](../base/indexing/index.html#vectorbt.base.indexing.IndexingBase "vectorbt.base.indexing.IndexingBase")
  * [PXAccessor](index.html#vectorbt.px_accessors.PXAccessor "vectorbt.px_accessors.PXAccessor")
  * [PandasIndexer](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer "vectorbt.base.indexing.PandasIndexer")
  * [Pickleable](../utils/config/index.html#vectorbt.utils.config.Pickleable "vectorbt.utils.config.Pickleable")
  * [Wrapping](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping "vectorbt.base.array_wrapper.Wrapping")

**Inherited members**

  * [AttrResolver.deep_getattr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.deep_getattr "vectorbt.px_accessors.PXAccessor.deep_getattr")
  * [AttrResolver.post_resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.post_resolve_attr "vectorbt.px_accessors.PXAccessor.post_resolve_attr")
  * [AttrResolver.pre_resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.pre_resolve_attr "vectorbt.px_accessors.PXAccessor.pre_resolve_attr")
  * [AttrResolver.resolve_attr()](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.resolve_attr "vectorbt.px_accessors.PXAccessor.resolve_attr")
  * [BaseAccessor.align_to()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.align_to "vectorbt.px_accessors.PXAccessor.align_to")
  * [BaseAccessor.apply()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply "vectorbt.px_accessors.PXAccessor.apply")
  * [BaseAccessor.apply_and_concat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply_and_concat "vectorbt.px_accessors.PXAccessor.apply_and_concat")
  * [BaseAccessor.apply_on_index()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.apply_on_index "vectorbt.px_accessors.PXAccessor.apply_on_index")
  * [BaseAccessor.broadcast()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.broadcast "vectorbt.px_accessors.PXAccessor.broadcast")
  * [BaseAccessor.broadcast_to()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.broadcast_to "vectorbt.px_accessors.PXAccessor.broadcast_to")
  * [BaseAccessor.combine()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.combine "vectorbt.px_accessors.PXAccessor.combine")
  * [BaseAccessor.concat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.concat "vectorbt.px_accessors.PXAccessor.concat")
  * [BaseAccessor.drop_duplicate_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_duplicate_levels "vectorbt.px_accessors.PXAccessor.drop_duplicate_levels")
  * [BaseAccessor.drop_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_levels "vectorbt.px_accessors.PXAccessor.drop_levels")
  * [BaseAccessor.drop_redundant_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.drop_redundant_levels "vectorbt.px_accessors.PXAccessor.drop_redundant_levels")
  * [BaseAccessor.empty()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.empty "vectorbt.px_accessors.PXAccessor.empty")
  * [BaseAccessor.empty_like()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.empty_like "vectorbt.px_accessors.PXAccessor.empty_like")
  * [BaseAccessor.indexing_func()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.indexing_func "vectorbt.px_accessors.PXAccessor.indexing_func")
  * [BaseAccessor.make_symmetric()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.make_symmetric "vectorbt.px_accessors.PXAccessor.make_symmetric")
  * [BaseAccessor.rename_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.rename_levels "vectorbt.px_accessors.PXAccessor.rename_levels")
  * [BaseAccessor.repeat()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.repeat "vectorbt.px_accessors.PXAccessor.repeat")
  * [BaseAccessor.select_levels()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.select_levels "vectorbt.px_accessors.PXAccessor.select_levels")
  * [BaseAccessor.stack_index()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.stack_index "vectorbt.px_accessors.PXAccessor.stack_index")
  * [BaseAccessor.tile()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.tile "vectorbt.px_accessors.PXAccessor.tile")
  * [BaseAccessor.to_1d_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_1d_array "vectorbt.px_accessors.PXAccessor.to_1d_array")
  * [BaseAccessor.to_2d_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_2d_array "vectorbt.px_accessors.PXAccessor.to_2d_array")
  * [BaseAccessor.to_dict()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.to_dict "vectorbt.px_accessors.PXAccessor.to_dict")
  * [BaseAccessor.unstack_to_array()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.unstack_to_array "vectorbt.px_accessors.PXAccessor.unstack_to_array")
  * [BaseAccessor.unstack_to_df()](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.unstack_to_df "vectorbt.px_accessors.PXAccessor.unstack_to_df")
  * [Configured.copy()](../utils/config/index.html#vectorbt.utils.config.Configured.copy "vectorbt.px_accessors.PXAccessor.copy")
  * [Configured.dumps()](../utils/config/index.html#vectorbt.utils.config.Pickleable.dumps "vectorbt.px_accessors.PXAccessor.dumps")
  * [Configured.loads()](../utils/config/index.html#vectorbt.utils.config.Pickleable.loads "vectorbt.px_accessors.PXAccessor.loads")
  * [Configured.replace()](../utils/config/index.html#vectorbt.utils.config.Configured.replace "vectorbt.px_accessors.PXAccessor.replace")
  * [Configured.to_doc()](../utils/docs/index.html#vectorbt.utils.docs.Documented.to_doc "vectorbt.px_accessors.PXAccessor.to_doc")
  * [Configured.update_config()](../utils/config/index.html#vectorbt.utils.config.Configured.update_config "vectorbt.px_accessors.PXAccessor.update_config")
  * [PXAccessor.config](../utils/config/index.html#vectorbt.utils.config.Configured.config "vectorbt.px_accessors.PXAccessor.config")
  * [PXAccessor.df_accessor_cls](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.df_accessor_cls "vectorbt.px_accessors.PXAccessor.df_accessor_cls")
  * [PXAccessor.iloc](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.iloc "vectorbt.px_accessors.PXAccessor.iloc")
  * [PXAccessor.indexing_kwargs](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.indexing_kwargs "vectorbt.px_accessors.PXAccessor.indexing_kwargs")
  * [PXAccessor.loc](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.loc "vectorbt.px_accessors.PXAccessor.loc")
  * [PXAccessor.obj](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.obj "vectorbt.px_accessors.PXAccessor.obj")
  * [PXAccessor.self_aliases](../utils/attr_/index.html#vectorbt.utils.attr_.AttrResolver.self_aliases "vectorbt.px_accessors.PXAccessor.self_aliases")
  * [PXAccessor.sr_accessor_cls](../base/accessors/index.html#vectorbt.base.accessors.BaseAccessor.sr_accessor_cls "vectorbt.px_accessors.PXAccessor.sr_accessor_cls")
  * [PXAccessor.wrapper](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.wrapper "vectorbt.px_accessors.PXAccessor.wrapper")
  * [PXAccessor.writeable_attrs](../utils/config/index.html#vectorbt.utils.config.Configured.writeable_attrs "vectorbt.px_accessors.PXAccessor.writeable_attrs")
  * [PandasIndexer.xs()](../base/indexing/index.html#vectorbt.base.indexing.PandasIndexer.xs "vectorbt.px_accessors.PXAccessor.xs")
  * [Pickleable.load()](../utils/config/index.html#vectorbt.utils.config.Pickleable.load "vectorbt.px_accessors.PXAccessor.load")
  * [Pickleable.save()](../utils/config/index.html#vectorbt.utils.config.Pickleable.save "vectorbt.px_accessors.PXAccessor.save")
  * [Wrapping.regroup()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.regroup "vectorbt.px_accessors.PXAccessor.regroup")
  * [Wrapping.resolve_self()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.resolve_self "vectorbt.px_accessors.PXAccessor.resolve_self")
  * [Wrapping.select_one()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.select_one "vectorbt.px_accessors.PXAccessor.select_one")
  * [Wrapping.select_one_from_obj()](../base/array_wrapper/index.html#vectorbt.base.array_wrapper.Wrapping.select_one_from_obj "vectorbt.px_accessors.PXAccessor.select_one_from_obj")