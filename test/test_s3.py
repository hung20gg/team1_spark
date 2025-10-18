# inside your test
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.read.parquet("s3a://team1spark/silver/2025-01-01_2025-01-31/posts").show()
