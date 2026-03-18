"""Script that executes a data processing workflow."""

import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl


pl.Config.set_engine_affinity("streaming")

DATA_DIR: Path = Path(__file__).parent.absolute().resolve() / "artifacts" / "data"
OFFSET_DATE: date = date(2026, 1, 1)


def process_data(look_back_period: int) -> None:
    """Loads raw hourly energy demand data from `./artifacts/data/raw.parquet`,
    processes it, and writes the processed data to `./artifacts/data/processed.parquet`.

    Args:
        look_back_period (int): The number of previous data points (days) to use for
        filtering the raw data with respect to the current hour. 
    """
    try:
        assert look_back_period in range(1, 32), f"{look_back_period} is invalid. Try an integer \
between 1 and 31, inclusive."
        end: datetime = datetime.now(UTC).replace(second=0, microsecond=0, tzinfo=None)
        start: datetime = end - timedelta(days=look_back_period)
        path: Path = DATA_DIR / "raw.parquet"
        plan: pl.LazyFrame = pl.scan_parquet(path)
        data: pl.DataFrame = (
            plan
            .join(
                other=(
                    plan
                    .group_by("company_id")
                    .agg(pl.min("timestamp_utc").alias("min_timestamp"))
                    .select(
                        "company_id",
                        (pl.lit(OFFSET_DATE) - pl.col("min_timestamp")).alias("offset")
                    )
                ),
                how="left",
                on="company_id",
                maintain_order="left"
            )
            .with_columns(pl.col("timestamp_utc") + pl.col("offset"))
            .filter(pl.col("timestamp_utc").is_between(start, end))
            .drop("offset")
            .collect()
            .upsample(
                time_column="timestamp_utc",
                every="1h",
                group_by="company_id",
                maintain_order=True
            )
            .fill_null(strategy="forward")
            .unique(subset=["company_id", "timestamp_utc"], keep="first", maintain_order=True)
        )

        assert data.null_count().sum_horizontal().sum() == 0, "The data contains NULLs."
        assert data.is_duplicated().sum() == 0, "The data contains duplicates."

        path = DATA_DIR / "processed.parquet"
        if path.exists():
            (
                pl.union((pl.scan_parquet(path), data.lazy()))
                .unique(subset=["company_id", "timestamp_utc"], keep="first")
                .sort("company_id", "timestamp_utc")
                .sink_parquet(path)
            )
        else:
            data.lazy().sink_parquet(path)
    except Exception as e:
        raise e


if __name__ == "__main__":
    process_data(look_back_period=int(sys.argv[1]))
    print(pl.scan_parquet(DATA_DIR / "processed.parquet").collect())
