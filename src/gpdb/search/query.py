"""
Search query types and DSL support.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Generic, List, Optional, TypeVar, Union

from pydantic import BaseModel, Field, field_validator


class Op(str, Enum):
    EQ = "eq"
    GT = "gt"
    LT = "lt"
    GTE = "gte"
    LTE = "lte"
    NE = "ne"
    CONTAINS = "contains"
    IN = "in"


class Logic(str, Enum):
    AND = "and"
    OR = "or"


class Filter(BaseModel):
    field: str
    op: Op = Op.EQ
    value: Any

    def to_dsl(self) -> str:
        """Convert Filter to DSL string."""
        from gpdb.search.parser import _value_to_dsl

        val_str = _value_to_dsl(self.value)
        return f"{self.field} {self.op.value} {val_str}"


class FilterGroup(BaseModel):
    logic: Logic = Logic.AND
    filters: List[Union[Filter, "FilterGroup"]]

    def to_dsl(self) -> str:
        """Convert FilterGroup to DSL string."""
        parts = []
        for f in self.filters:
            if isinstance(f, Filter):
                parts.append(f.to_dsl())
            else:
                parts.append(f.to_dsl())
        inner = f" {self.logic.value} ".join(parts)
        if len(parts) > 1:
            return f"({inner})"
        return inner

    @classmethod
    def from_dsl(cls, text: str) -> Union[Filter, "FilterGroup"]:
        """Parse DSL string into Filter or FilterGroup."""
        from gpdb.search.parser import _tokenize, _parse_expr

        tokens = _tokenize(text)
        result, _ = _parse_expr(tokens, 0)
        return result


# Allow recursive nesting
FilterGroup.model_rebuild()


class Sort(BaseModel):
    field: str
    desc: bool = True


class SearchQuery(BaseModel):
    filter: Optional[Union[FilterGroup, Filter, str]] = None
    sort: List[Sort] = Field(default_factory=list)
    limit: int = 50
    offset: int = 0
    select: Optional[List[str]] = None

    @field_validator("filter", mode="before")
    @classmethod
    def parse_filter(cls, v: Any) -> Any:
        if isinstance(v, str):
            if not v.strip():
                return None
            return FilterGroup.from_dsl(v)
        return v


T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: List[T]
    total: int
    limit: int
    offset: int
