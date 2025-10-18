$CONDA_PREFIX/bin/spark-submit - <<'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestSpark").getOrCreate()
print("Spark version:", spark.version)
spark.stop()
EOF
