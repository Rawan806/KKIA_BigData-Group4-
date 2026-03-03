from pyspark.sql.functions import col, to_timestamp
from pyspark.ml.feature import StringIndexer

# Data Transformation: Type conversion and Categorical Encoding
# -----------------------------------------------------------

# 1. Standardizing scheduled time format for analysis
df_transformed = df.withColumn("scheduled_time", 
                               to_timestamp(col("movement.scheduledTime.local"), "yyyy-MM-dd HH:mm:ss"))

# 2. Encoding categorical attributes for processing
# Attributes: Airline, Flight Type, and Terminal
categorical_cols = ["airline.name", "flight_type", "movement.terminal"]
output_cols = ["airline_index", "flight_type_index", "terminal_index"]

indexer = StringIndexer(inputCols=categorical_cols, outputCols=output_cols)
df_transformed = indexer.fit(df_transformed).transform(df_transformed)

# 3. Final structural validation
df_transformed.printSchema()
df_transformed.show(5)
