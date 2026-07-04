"""
smoke.py — Phase 0: confirm Spark runs, and see lazy evaluation + a shuffle.

Run:  python phases/phase00-setup/smoke.py
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceLakehouse")
    .master("local[*]")                          # executors = threads, 1 per core
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.range(0, 1000)               # transformation -> builds a plan, runs nothing
result = df.filter("id % 2 = 0").groupBy().count()   # still lazy
print("EVEN COUNT =", result.collect()[0][0])        # collect() = ACTION -> runs the job

print("\n=== PHYSICAL PLAN (look for the Exchange = shuffle) ===")
result.explain("formatted")

spark.stop()
