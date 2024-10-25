import polars as pl
from polars_demo.utils import polars_hhi, LodesProd

 
jobs_by_naics_reg: str = "^CNS[0-9][0-9]$"
jobs_by_naics_list: list[str] = (
    [f"CNS0{num}" for num in range(1,10)] + 
    [f"CNS{num}" for num in range(10,21)]
    )


def employment_hhi(prod: LodesProd) -> pl.LazyFrame:
    match prod:
        case LodesProd.WAC:
            geo_code: str = "w"
        case LodesProd.RAC:
            geo_code: str = "h"
    # Read multiple files with same schema using wildcards
    lodes_data: pl.LazyFrame = (
        pl.scan_csv(f"data/{prod.value}/*_{prod.value}_S000_JT01_2020.csv")
        .select(
            pl.col(f"{geo_code}_geocode").cast(pl.String).alias("block_code"),
            pl.col("C000").alias(f"{geo_code}_total_jobs"),
            # Select columns based on regex match
            pl.col(jobs_by_naics_reg)
        )
        .group_by(pl.col("block_code").str.slice(0, 5).alias("county_code"))
        # Apply the same operation to all columns based on wildcards.
        # Works for regex, types, or conditions
        .agg(pl.col("*").sum())
        .select(
            "county_code",
            f"{geo_code}_total_jobs",
            # Apply same operation for multiple columns with one reference column using regex
            pl.col(jobs_by_naics_reg) / pl.col(f"{geo_code}_total_jobs")
        )
        .select(
            "county_code",
            f"{geo_code}_total_jobs",
            # Use a fold that gets vectorized
            polars_hhi(jobs_by_naics_list).alias(f"{prod.value}_hhi")
        )
    )
    return lodes_data


wac_df: pl.LazyFrame = employment_hhi(LodesProd.WAC)
rac_df: pl.LazyFrame = employment_hhi(LodesProd.RAC)

acs_tracts: pl.LazyFrame = (
    pl.scan_csv(
        "data/acs_2020_S2001_tracts.csv",
        skip_rows=1
        )
    .select(
        pl.col("Geography").cast(pl.String).str.slice(9).alias("tract_code"),
        pl.col("Estimate!!Total!!Population 16 years and over with earnings!!Median earnings (dollars)").alias("median_inc")
        )
    .with_columns(
        pl.col("median_inc")
        .str.replace(r"-|N|\(\w\)|\*{2,5}", "")
        .str.replace(r",|\+|-", "")
        .alias("median_inc_tmp")
    )
    .with_columns(
        pl.when(pl.col("median_inc_tmp") == "").then(None).otherwise(pl.col("median_inc_tmp"))
        .cast(pl.Int64)
        .alias("median_inc_clean")
    )
    .drop("median_inc", "median_inc_tmp")
)

joined_df: pl.LazyFrame = (
    acs_tracts
    .join(
        wac_df,
        left_on=pl.col("tract_code").str.slice(0, 5),
        right_on=pl.col("county_code"),
        how="left"
    )
)
