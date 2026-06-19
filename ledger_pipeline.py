import dlt
from pyspark.sql.functions import col, current_timestamp, when, lit, sum, count, max
from pyspark.sql.types import DoubleType, TimestampType

# ============================================================
# BRONZE — raw streaming table
# ============================================================

@dlt.table(
    name="transactions_stream",
    comment="Bronze — raw events streamed from transactions_raw",
    table_properties={"quality": "bronze"}
)
def transactions_stream():
    return (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .table("`ledger-catalog`.raw.transactions_raw")
    )


# ============================================================
# SILVER — Dead Letter Queue
# ============================================================

@dlt.table(
    name="dlq_transactions",
    comment="Silver — malformed and invalid events quarantined here",
    table_properties={"quality": "silver"}
)
def dlq_transactions():
    return (
        dlt.read_stream("transactions_stream")
        .filter(
            col("transaction_id").isNull() |
            col("user_id").isNull() |
            col("amount").isNull() |
            col("amount").isNaN() |
            (col("amount") <= 0) |
            ~col("type").isin("deposit", "withdrawal", "transfer")
        )
        .withColumn("failed_at", current_timestamp())
        .withColumn(
            "error_reason",
            when(col("transaction_id").isNull(), lit("missing_transaction_id"))
            .when(col("user_id").isNull(), lit("missing_user_id"))
            .when(col("amount").isNull() | col("amount").isNaN(), lit("missing_amount"))
            .when(col("amount") <= 0, lit("invalid_amount"))
            .when(
                ~col("type").isin("deposit", "withdrawal", "transfer"),
                lit("invalid_type")
            )
            .otherwise(lit("unknown_error"))
        )
        .select(
            "transaction_id",
            "user_id",
            "user_name",
            "bank",
            "type",
            "amount",
            "currency",
            "channel",
            "description",
            "raw_payload",
            "failed_at",
            "error_reason"
        )
    )


# ============================================================
# SILVER — clean validated deduplicated events
# ============================================================

@dlt.table(
    name="transactions_clean",
    comment="Silver — validated, deduplicated clean transactions",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount IS NOT NULL AND amount > 0 AND NOT isnan(amount)")
@dlt.expect_or_drop("valid_type", "type IN ('deposit', 'withdrawal', 'transfer')")
def transactions_clean():
    return (
        dlt.read_stream("transactions_stream")
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn(
            "event_timestamp",
            col("event_timestamp").cast(TimestampType())
        )
        .withColumn("processed_at", current_timestamp())
        .dropDuplicates(["transaction_id"])
        .select(
            "transaction_id",
            "user_id",
            "user_name",
            "account_number",
            "bank",
            "type",
            "amount",
            "currency",
            "event_timestamp",
            "channel",
            "description",
            "processed_at"
        )
    )


# ============================================================
# SILVER — fraud alerts
# ============================================================

@dlt.table(
    name="fraud_alerts",
    comment="Silver — suspicious transactions flagged for review",
    table_properties={"quality": "silver"}
)
def fraud_alerts():
    return (
        dlt.read_stream("transactions_clean")
        .filter(
            (col("amount") > 100000) |
            (col("type") == "withdrawal")
        )
        .withColumn("flagged_at", current_timestamp())
        .withColumn(
            "fraud_reason",
            when(col("amount") > 100000, lit("large_withdrawal"))
            .otherwise(lit("high_value_transaction"))
        )
        .select(
            "transaction_id",
            "user_id",
            "user_name",
            "bank",
            "type",
            "amount",
            "currency",
            "channel",
            "description",
            "fraud_reason",
            "flagged_at"
        )
    )


# ============================================================
# GOLD — user balances
# ============================================================

@dlt.table(
    name="user_balances",
    comment="Gold — running balance per user",
    table_properties={"quality": "gold"}
)
def user_balances():
    return (
        dlt.read("transactions_clean")
        .groupBy(
            "user_id",
            "user_name",
            "bank",
            "account_number"
        )
        .agg(
            sum(
                when(col("type") == "deposit", col("amount"))
                .when(col("type") == "transfer", col("amount") * -1)
                .otherwise(col("amount") * -1)
            ).alias("balance"),
            sum(
                when(col("type") == "deposit", col("amount"))
                .otherwise(lit(0))
            ).alias("total_deposits"),
            sum(
                when(col("type").isin("withdrawal", "transfer"), col("amount"))
                .otherwise(lit(0))
            ).alias("total_withdrawals"),
            count("transaction_id").alias("transaction_count"),
            max("event_timestamp").alias("last_updated")
        )
    )
