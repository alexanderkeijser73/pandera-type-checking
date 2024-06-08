"""Pandera mypy plugin."""

from typing import Callable, Optional, Union, cast

from mypy.errorcodes import ATTR_DEFINED
from mypy.nodes import FuncBase, SymbolNode, TypeInfo
from mypy.plugin import (
    ClassDefContext,
    FunctionSigContext,
    MethodSigContext,
    Plugin, AttributeContext, MethodContext,
)
from mypy.types import CallableType, Instance, UnionType, Type

DATAFRAMEMODEL_FULLNAME = "pandera.api.pandas.model.DataFrameModel"
PANDERA_PANDAS_DATAFRAME_FULLNAME = "pandera.typing.pandas.DataFrame"
PANDERA_PANDAS_SERIES_FULLNAME = "pandera.typing.pandas.Series"
PANDERA_PANDAS_INDEX_FULLNAME = "pandera.typing.pandas.Index"
PANDERA_MODIN_SERIES_FULLNAME = "pandera.typing.modin.Series"
PANDERA_MODIN_INDEX_FULLNAME = "pandera.typing.modin.Index"
PANDERA_DASK_SERIES_FULLNAME = "pandera.typing.dask.Series"
PANDERA_DASK_INDEX_FULLNAME = "pandera.typing.dask.Index"
PANDERA_PYSPARK_SERIES_FULLNAME = "pandera.typing.pyspark.Series"
PANDERA_PYSPARK_INDEX_FULLNAME = "pandera.typing.pyspark.Index"
PANDERA_GEOPANDAS_SERIES_FULLNAME = "pandera.typing.geopandas.GeoSeries"
PANDAS_CONCAT = "pandas.core.reshape.concat.concat"
PANDAS_DATAFRAME_FULLNAME = 'pandas.core.frame.DataFrame'

FIELD_GENERICS_FULLNAMES = {
    PANDERA_PANDAS_SERIES_FULLNAME,
    PANDERA_PANDAS_INDEX_FULLNAME,
    PANDERA_MODIN_SERIES_FULLNAME,
    PANDERA_MODIN_INDEX_FULLNAME,
    PANDERA_DASK_SERIES_FULLNAME,
    PANDERA_DASK_INDEX_FULLNAME,
    PANDERA_PYSPARK_SERIES_FULLNAME,
    PANDERA_PYSPARK_INDEX_FULLNAME,
    PANDERA_GEOPANDAS_SERIES_FULLNAME,
}


# pylint: disable=unused-argument
def plugin(version: str):
    """Mypy plugin entrypoint."""
    return PanderaPlugin


def is_pandas_module(fullname: str) -> bool:
    """Check if a fully qualified name is from the pandas module"""
    return fullname.startswith("pandas.")


class PanderaPlugin(Plugin):
    """Pandera mypy plugin.

    Since pandera uses the pandas-stubs library:
    https://github.com/pandas-dev/pandas-stubs

    We need to patch all of the function/method signatures in the library
    which turn out to yield many false positives with respect to regular
    pandas usage. Currently this is what this plugin does, though the
    future plan for this plugin is to improve and enable users to customize
    the static typing experience for both pandas and pandera.
    """

    def __init__(self, options) -> None:
        self.plugin_config = PanderaPluginConfig(options)
        super().__init__(options)

    def get_function_signature_hook(self, fullname: str):
        """Adjust the function signatures of pandas functions."""
        if fullname == PANDAS_CONCAT:
            return self.pandas_concat_callback

    def get_base_class_hook(
        self, fullname: str
    ) -> "Optional[Callable[[ClassDefContext], None]]":
        sym = self.lookup_fully_qualified(fullname)
        if sym and isinstance(sym.node, TypeInfo):  # pragma: no branch
            if any(
                get_fullname(base) == DATAFRAMEMODEL_FULLNAME
                for base in sym.node.mro
            ):
                return self._pandera_model_class_maker_callback
        return None

    def get_attribute_hook(self, fullname: str) -> Callable[[AttributeContext], Type] | None:
        if PANDAS_DATAFRAME_FULLNAME in fullname:
            return pandera_attribute_callback

    def get_method_hook(self, fullname: str) -> Callable[[MethodContext], Type] | None:
        if PANDERA_PANDAS_DATAFRAME_FULLNAME in fullname:
            return pandera_method_callback

    def _pandera_model_class_maker_callback(
        self, ctx: ClassDefContext
    ) -> None:
        transformer = DataFrameModelTransformer(ctx, self.plugin_config)
        transformer.transform()

    def pandas_concat_callback(
        self, ctx: Union[FunctionSigContext, MethodSigContext]
    ) -> CallableType:
        """Adjusts the signature pandas.concat to allow generator inputs."""
        iterable = self.lookup_fully_qualified("typing.Iterable")
        if iterable is not None:
            iterable_node = cast(TypeInfo, iterable.node)
        else:
            raise ValueError("typing.Iterable node not found")

        union_type = cast(UnionType, ctx.default_signature.arg_types[0])

        pandas_data_type = ctx.default_signature.ret_type
        arg_types = [
            UnionType(
                [
                    Instance(iterable_node, [pandas_data_type]),
                    *union_type.items,
                ]
            ),
            *ctx.default_signature.arg_types[1:],
        ]
        return ctx.default_signature.copy_modified(arg_types=arg_types)


def _check_column_defined(ctx: AttributeContext | MethodContext, colname: str):
    schema = ctx.type.args[0]
    schema_cols = {k for s in schema.type.mro for k in s.defn.info.names.keys()}
    if colname not in schema_cols:
        full_message = f"Column '{colname}' not defined for Pandera DataFrameModel '{schema}'"
        ctx.api.fail(full_message, ctx.context, code=ATTR_DEFINED)


def pandera_method_callback(ctx: MethodContext) -> Type:
    if hasattr(ctx.type, "type") and ctx.type.type.fullname == PANDERA_PANDAS_DATAFRAME_FULLNAME:
        colname = ctx.args[0][0]
        if hasattr(colname, 'value'):
            colname = colname.value
            _check_column_defined(ctx, colname)
    return ctx.default_return_type


def pandera_attribute_callback(ctx: AttributeContext) -> Type:
    if hasattr(ctx.type, "type") and ctx.type.type.fullname == PANDERA_PANDAS_DATAFRAME_FULLNAME:
        colname = ctx.context.name
        _check_column_defined(ctx, colname)
    return ctx.default_attr_type


class DataFrameModelTransformer:
    def __init__(self, ctx: ClassDefContext, plugin_config):
        self.ctx = ctx

    def transform(self) -> None:
        self.erase_field_type_arg()

    def erase_field_type_arg(self):
        """Erase type information of DataFrameModel fields.

        This allows for overriding types when subclassing DataFrameModels. For
        example:

        class BaseSchema(pa.DataFrameModel):
            x: pa.typing.Series[int]

        class Schema(BaseSchema):
            x: pa.typing.Series[str]  # mypy assignment error, cannot override types
        """
        for def_ in self.ctx.cls.defs.body:
            if (
                not hasattr(def_, "type")
                or def_.type is None
                # e.g. UnionType does not have module_name or name
                or not hasattr(def_.type, "module_name")
                or not hasattr(def_.type, "name")
            ):
                continue
            type_ = def_.type
            if str(def_.type) in FIELD_GENERICS_FULLNAMES:
                type_.args = ()  # erase generic type arg


# pylint: disable=too-few-public-methods
class PanderaPluginConfig:
    """Pandera mypy plugin config"""

    def __init__(self, options):
        """Configuration options (config options are still TBD)."""
        self.options = options


def get_fullname(x: Union[FuncBase, SymbolNode]) -> str:
    fn = x.fullname
    if callable(fn):  # pragma: no cover
        return fn()
    return fn