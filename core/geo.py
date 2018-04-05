from sqlalchemy import Integer, String, Boolean, Column
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.sql.base import ColumnCollection
from sqlalchemy.sql.expression import column
from geoalchemy2 import Geometry
from sqlalchemy_utils.types.pg_composite import CompositeElement
import sqlalchemy_utils


class CompositeType(sqlalchemy_utils.CompositeType):

    class comparator_factory(sqlalchemy_utils.CompositeType.comparator_factory):
        def __getattr__(self, key):
            try:
                type_ = self.type.typemap[key]
            except KeyError:
                raise AttributeError(key)
            return CompositeElement(self.expr, key, type_)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.typemap = {c.name: c.type for c in self.columns}


NormAddy = CompositeType(
    "norm_addy",
    [
        Column("address", Integer),
        Column("predirAbbrev", String),
        Column("streetName", String),
        Column("streetTypeAbbrev", String),
        Column("postdirAbbrev", String),
        Column("internal", String),
        Column("location", String),
        Column("stateAbbrev", String),
        Column("zip", String),
        Column("parsed", Boolean),
    ],
)


class geocode(GenericFunction):
    columns = ColumnCollection(
        Column("rating", Integer),
        column("geomout"),  # lowercase column because we don't have the `geometry` type
        Column("addy", NormAddy),
    )