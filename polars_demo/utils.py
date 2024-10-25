from enum import Enum
import polars as pl
import polars.selectors as pl_s


def polars_hhi(share_cols: list[str]) -> pl.Expr:
    # Fold expressions operate horizontally across columns but are
    # optimized so you don't get a performance hit.
    fold_expr: pl.Expr = pl.fold(
        # Starting value
        acc=pl.lit(0),
        # How to combine subsequent values
        function=lambda acc, x: acc + x**2,
        # Which columns to use, I use selectors here
        exprs=pl_s.by_name(*share_cols, require_all=True)
        )
    return fold_expr


class LodesProd(Enum):
    WAC = "wac"
    RAC = "rac"
