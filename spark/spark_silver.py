"""
Reads bronze parquet files, applies business logic
Writes conformed star schema (fact + 3 dimenions)    
"""

import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StracutField,
    StringType, IntegerType,
    BooleanType, DecimalType
)

import argparse

#-- CLI

def parse_args():
    p = argparse.ArgumentParser(
        description="Einkaufpark DE Silver Layer v2")
    p.add_argument("--month", type=str, default=None,
                   help="Process single month (YYYY-MM). Default: all momths.")
    p.add_argument("--master-dir", type=str, default= None)
    p.add_argument("--bronze-dir", type=str, default=None)
    p.add_argument("--silver-dir", type=str, default=None)
    return p.parse_args()

#-- Paths

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MASTER_DIR = PROJECT_ROOT / "master"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "transactions"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


#-- Helper Functions

def load_json(path: Path, label: str) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found at {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)
    
    
def create_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("EinkaufPark Silver Layer")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
            .getOrCreate())
    

#-- Logic

#-- Step 1: Deduplication

def deduplicate(df: DataFrame) -> tuple:
    """ 
    Deduplicate on record_hash.
    """
    before_count = df.count()
    df_deduped = df.dropDuplicates(["record_hash"])
    after_count = df_deduped.count()
    return df_deduped, before_count - after_count

#-- Step 2: Gender normalisation

def normalise_gender(df: DataFrame) -> DataFrame:
    """
    Bronze: M, Male, F, Female, Drivers, null
    Silver: M, F, D, null
    """
    
    return df.withColumn(
        "gender",
        F.when(F.col("gender").isin("M", "Male"), F.lit("M"))
        .when(F.col("gender").isin("F", "Female"). F.lit("F"))
        .when(F.col("gender") == "Drivers", F.lit("D"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    
#-- Step 3:Age clamping

def clamp_age(df: DataFrame) -> DataFrame:
    """
    Null out impossible ages. No imputations - null is honest.
    """
    return df.withColumn(
        "customer_age",
        F.when(
            (F.col("customer_age").isNOtNull()) &
            (F.col("customer_age") >= 0) &
            (F.col("customer_age") <= 120),
            F.col("customer_age")
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    

#-- Step 4: Discount capping

def cap_discount(df: DataFrame) -> DataFrame:
    """
    Discounts > 100% are garbage from legacy POS. Nullify them.
    """
    
    return df.withColumn(
        "discount_pct",
        F.when(
            F.col("discount_pct") > 100,
            F.lit(None).cast(DecimalType(5, 2))
        ).otherwise(F.col("discount_pct"))
    )
    

#-- Step 5: Revenue recalculation 

def recalculate_revenue(df: DataFrame) -> DataFrame:
    
    """
    Recompute net_revenue_eur from  clean components.
    
    Order-status dependent Logic:
    - Voided: revenue = 0.00 always (transaction cancelled)
    - Returned: revenue = price * qty (qty  is negative -> negative revenue)
    - Partially_Returned: normal formula (some lines negative, some positive)
    - Completed: normal formula with full validation
    
    Normal formula: price * qty * (1-discount/100)
    Requires price > 0, qty not null, discount not null
    """    
    
    # Voided orders : revenue is always 0 regardless of price/qty
    voided_case = F.when(
        F.col("order_status") == "Voided",
        F.lit(0.00).cast(DecimalType(12, 2))
    )
    
    # Normal recalculation for all other statuses
    normal_case = F.when(
        (F.col("unit_price_eur").isNotNull()) &
        (F.col("quantity").isNotNull()) &
        (F.col("discount_pct").isNotNull()) &
        (F.col("unit_price_eur") > 0),
        F.round(
            F.col("unit_price_eur")
            * F.col("quantity")
            * (1 - F.col("discount_pct") / 100),
            2
        )
    ).otherwise(F.lit(None).cast(DecimalType(12, 2)))
    
    return df.withColumn(
        "net_revenue_eur",
        voided_case.otherwise(normal_case)
    )
    

#-- Step 6: Ship date    healing

def heal_ship_date(df: DataFrame) -> DataFrame:
    """
    For IN_STORE transactions, ship_date = order_date.
    There is no scenario where an in-store purchase ships on a different day.
    """
    
    return df.withColumn(
        "ship_date",
        F.when(
            F.col("sales_channel") == "IN_STORE",
            F.col("order_date")
        ).otherwise(F.col("ship_date"))
    )