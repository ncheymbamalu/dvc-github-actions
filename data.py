import random
from pathlib import Path

import polars as pl


pl.Config.set_engine_affinity("streaming")

DATA_DIR: Path = Path.cwd() / "artifacts" / "data"


if __name__ == "__main__":
    path: Path = DATA_DIR / "raw.parquet"
    plans: list[pl.LazyFrame] = [
        pl.scan_parquet(path)
        .filter(pl.col("company_id").eq(company_id))
        .limit(random.choice(range(1_000)))
        for company_id in set(pl.scan_parquet(path).select("company_id").collect().to_series())
    ]
    path = DATA_DIR / f"raw_{random.choice(range(10))}.parquet"
    if path.exists():
        path.unlink()
    pl.union(plans).sort("company_id", "timestamp_utc").sink_parquet(path)
