"""
Test script for SparkSession Factory
Run from project root with: pipenv run python scripts/test_spark.py
"""
from jobs.spark_session_factory import create_spark_session, get_or_create_session, stop_session

def test_create_spark_session():
    print("\n--- Test 1: create_spark_session() ---")
    spark = create_spark_session()
    df = spark.range(0, 5).toDF("id")
    df.show()
    print("✓ create_spark_session() works")
    stop_session(spark)
    print("✓ stop_session() works")

def test_get_or_create_session():
    print("\n--- Test 2: get_or_create_session() ---")
    spark = get_or_create_session()
    df = spark.range(0, 5).toDF("id")
    df.show()
    print("✓ get_or_create_session() created a new session (no active session)")

    spark2 = get_or_create_session()
    assert spark2 is spark, "Should return the same active session"
    print("✓ get_or_create_session() returned existing active session (no duplicate)")
    stop_session(spark)

def test_config_overrides():
    print("\n--- Test 3: config_overrides ---")
    spark = create_spark_session(config_overrides={"spark.sql.shuffle.partitions": "4"})
    val = spark.conf.get("spark.sql.shuffle.partitions")
    assert val == "4", f"Expected '4', got '{val}'"
    print(f"✓ config_override applied: spark.sql.shuffle.partitions = {val}")
    stop_session(spark)

if __name__ == "__main__":
    test_create_spark_session()
    test_get_or_create_session()
    test_config_overrides()
    print("\n✓ All tests passed!")