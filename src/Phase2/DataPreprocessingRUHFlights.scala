import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataPreprocessingRUHFlights {

  // --- Safe column accessor for names that contain dots like "movement.scheduledTime.utc"
  def c(name: String): Column = col(s"$name")

  // --- 1) Missing summary (SAFE with dot column names)
  def missingSummary(df: DataFrame, label: String): DataFrame = {
    val cols = df.columns.toSeq

    val result =
      cols
        .map { name =>
          df.select(
            lit(name).alias("column"),
            sum(when(c(name).isNull, 1).otherwise(0)).cast("long").alias("missing_count")
          )
        }
        .reduce(_ union _)
        .filter(col("missing_count") > 0)
        .orderBy(desc("missing_count"))

    println(s"========== Missing Summary: $label ==========")
    result.show(200, truncate = false)

    result
  }

  // --- 2) Trim all string columns
  def withTrimmedStrings(df: DataFrame): DataFrame = {
    df.schema.fields.foldLeft(df) { (acc, f) =>
      f.dataType match {
        case StringType => acc.withColumn(f.name, trim(c(f.name)))
        case _          => acc
      }
    }
  }

  // --- 3) Normalize null-like strings to null (for string columns only)
  // IMPORTANT: we do NOT treat "Unknown" as null because it is a valid category in this dataset (e.g., status=Unknown).
  def normalizeNullLikeStrings(df: DataFrame): DataFrame = {
    // removed "unknown"
    val nullLike = Seq("null", "none", "na", "n/a", "nan", "")

    df.schema.fields.foldLeft(df) { (acc, f) =>
      f.dataType match {
        case StringType =>
          acc.withColumn(
            f.name,
            when(lower(c(f.name)).isin(nullLike: _*), lit(null)).otherwise(c(f.name))
          )
        case _ => acc
      }
    }
  }

  // --- 4) Uppercase codes safely (keeps nulls)
  def safeUpper(df: DataFrame, columnName: String): DataFrame = {
    if (df.columns.contains(columnName))
      df.withColumn(columnName, upper(c(columnName)))
    else df
  }

  // --- 5) Set invalid codes to null based on regex pattern
  def safeRegexNullify(df: DataFrame, columnName: String, pattern: String): (DataFrame, Long) = {
    if (!df.columns.contains(columnName)) return (df, 0L)

    val invalidCount =
      df.filter(c(columnName).isNotNull && !c(columnName).rlike(pattern)).count()

    val updated =
      df.withColumn(
        columnName,
        when(c(columnName).isNotNull && !c(columnName).rlike(pattern), lit(null)).otherwise(c(columnName))
      )

    (updated, invalidCount)
  }

  // --- 6) Robust timestamp parsing helpers
  private def parseTsMulti(colStr: Column): Column = {
    val normalized = regexp_replace(colStr, "Z$", "+00:00")

    coalesce(
      to_timestamp(normalized, "yyyy-MM-dd HH:mm:ssXXX"),
      to_timestamp(normalized, "yyyy-MM-dd HH:mmXXX"),
      to_timestamp(normalized, "yyyy-MM-dd HH:mm:ssX"),
      to_timestamp(normalized, "yyyy-MM-dd HH:mmX"),
      to_timestamp(normalized) // fallback
    )
  }

  // Create new parsed timestamp columns (do NOT overwrite originals)
  def addParsedTimestamps(df: DataFrame): DataFrame = {
    var out = df
    val localCol = "movement.scheduledTime.local"
    val utcCol   = "movement.scheduledTime.utc"

    if (df.columns.contains(localCol))
      out = out.withColumn("scheduled_local_ts", parseTsMulti(c(localCol)))

    if (df.columns.contains(utcCol))
      out = out.withColumn("scheduled_utc_ts", parseTsMulti(c(utcCol)))

    out
  }

  // Build peak label using hour_of_day counts (top percentile)
  def addPeakLabelByPercentile(df: DataFrame, percentile: Double = 0.90): (DataFrame, Double) = {
    val hourly =
      df.groupBy("hour_of_day")
        .agg(count(lit(1)).alias("flights_in_hour"))

    val q = hourly.stat.approxQuantile("flights_in_hour", Array(percentile), 0.0)
    val threshold = if (q.nonEmpty) q(0) else Double.PositiveInfinity

    val labeledHourly =
      hourly.withColumn("peak_traffic_label",
        when(col("flights_in_hour") >= lit(threshold), lit(1)).otherwise(lit(0))
      )

    val out =
      df.join(labeledHourly.select("hour_of_day", "peak_traffic_label"), Seq("hour_of_day"), "left")
        .withColumn("peak_traffic_label", when(col("peak_traffic_label").isNull, 0).otherwise(col("peak_traffic_label")))

    (out, threshold)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RUH Flights Preprocessing")
      .master("local[*]")
      .getOrCreate()

    // Important for correct local feature extraction
    spark.conf.set("spark.sql.session.timeZone", "Asia/Riyadh")

    import spark.implicits._

    // ---------- PATHS ----------
    val base = "C:/Users/amusa/KKIA_BigDataGroup4/data"

    val inputPath = s"$base/raw/flights_RUH.parquet"

    // outputs from preprocessing
    val outputParquetPath = s"$base/processed/preprocessed_dataset.parquet"
    val outputCsvPath     = s"$base/processed/preprocessed_dataset_csv"

    // stats
    val resultsPath       = s"$base/results/phase2_preprocessing_stats"

    // ---------- LOAD ----------
    val raw = spark.read.parquet(inputPath)

    println("========== RAW DATASET ==========")
    raw.printSchema()

    val beforeRows = raw.count()
    val beforeCols = raw.columns.length
    println(s"Before rows: $beforeRows")
    println(s"Before cols: $beforeCols")

    val missingBefore = missingSummary(raw, "BEFORE")

    // ---------- CLEANING ----------
    var clean = raw.transform(withTrimmedStrings).transform(normalizeNullLikeStrings)

    // Uppercase important code columns
    val codeCols = Seq(
      "aircraft.reg",
      "aircraft.modeS",
      "airline.iata",
      "airline.icao",
      "origin_airport_iata",
      "origin_airport_icao",
      "destination_airport_iata",
      "destination_airport_icao"
    )
    codeCols.foreach(colName => clean = safeUpper(clean, colName))

    // Fix movement.terminal formatting (optional)
    if (clean.columns.contains("movement.terminal")) {
      clean = clean.withColumn("movement.terminal", upper(regexp_replace(c("movement.terminal"), "\\s+", "")))
    }

    // Parse timestamps into new columns
    clean = addParsedTimestamps(clean)

    // Filter rows missing local timestamp (your target derived from local time)
    if (clean.columns.contains("scheduled_local_ts")) {
      val beforeFilter = clean.count()
      clean = clean.filter(col("scheduled_local_ts").isNotNull)
      val afterFilter = clean.count()
      println(s"Filtered rows where scheduled_local_ts is null: removed ${beforeFilter - afterFilter}")
    }

    // Validate code formats (optional strict rules)
    var invalidStats = Seq.empty[(String, Long)]
    val formatRules = Seq(
      ("airline.iata", "^[A-Z0-9]{2}$"),
      ("airline.icao", "^[A-Z0-9]{3}$"),
      ("origin_airport_iata", "^[A-Z0-9]{3}$"),
      ("origin_airport_icao", "^[A-Z0-9]{4}$"),
      ("destination_airport_iata", "^[A-Z0-9]{3}$"),
      ("destination_airport_icao", "^[A-Z0-9]{4}$")
    )

    formatRules.foreach { case (colName, pattern) =>
      val (updated, invalidCount) = safeRegexNullify(clean, colName, pattern)
      clean = updated
      invalidStats = invalidStats :+ (colName, invalidCount)
    }

    println("========== Invalid Format Counts (set to null) ==========")
    invalidStats.foreach { case (colName, cnt) => println(s"$colName -> $cnt") }

    // ---------- FEATURE ENGINEERING (LOCAL time) ----------
    if (clean.columns.contains("scheduled_local_ts")) {
      clean = clean
        .withColumn("scheduled_date_local", to_date(col("scheduled_local_ts")))
        .withColumn("hour_of_day", hour(col("scheduled_local_ts")))
        .withColumn("day_of_week", dayofweek(col("scheduled_local_ts")))
        .withColumn("is_weekend", when(col("day_of_week").isin(6, 7), 1).otherwise(0)) // Fri+Sat
    }

    if (clean.columns.contains("isCargo")) {
      clean = clean.withColumn("isCargo_int", when(c("isCargo") === true, 1).otherwise(0))
    }

    // Fill some key text columns with UNKNOWN (optional)
    val fillUnknown = Seq("airline.icao", "airline.iata", "destination_airport_icao", "destination_airport_iata")
      .filter(clean.columns.contains)
    fillUnknown.foreach { colName =>
      clean = clean.withColumn(colName, when(c(colName).isNull, lit("UNKNOWN")).otherwise(c(colName)))
    }

    // ---------- DUPLICATES CHECK ----------
    val beforeDup = clean.count()

    // If you want to check duplicates based on ALL columns:
    val deduped = clean.dropDuplicates()

    val afterDup = deduped.count()
    val dupRemoved = beforeDup - afterDup

    println("========== DUPLICATE CHECK ==========")
    println(s"Rows before removing duplicates: $beforeDup")
    println(s"Rows after removing duplicates:  $afterDup")
    println(s"Duplicate rows removed:          $dupRemoved")

    // Replace clean with deduplicated version
    clean = deduped

    // ---------- OUTLIER CHECK (Hourly Flight Volume) ----------
    // We detect extreme hourly volumes using IQR on flights_in_hour.
    // This is meaningful for traffic/peak modeling.

    if (clean.columns.contains("hour_of_day")) {

      val hourly =
        clean.groupBy("hour_of_day")
          .agg(count(lit(1)).alias("flights_in_hour"))

      val qs = hourly.stat.approxQuantile("flights_in_hour", Array(0.25, 0.75), 0.01)
      val q1 = qs(0)
      val q3 = qs(1)
      val iqr = q3 - q1

      val lower = q1 - 1.5 * iqr
      val upper = q3 + 1.5 * iqr

      println("========== OUTLIER CHECK (Hourly Volume using IQR) ==========")
      println(s"Q1=$q1, Q3=$q3, IQR=$iqr")
      println(s"Lower bound=$lower, Upper bound=$upper")

      val outlierHours =
        hourly.filter(col("flights_in_hour") < lower || col("flights_in_hour") > upper)
          .orderBy(desc("flights_in_hour"))

      val outlierCount = outlierHours.count()
      println(s"Outlier hours detected: $outlierCount")

      if (outlierCount > 0) {
        println("Top outlier hours:")
        outlierHours.show(50, truncate = false)
      }

      // Decision: KEEP outlier hours (do NOT remove/cap),
      // because peak traffic is a real-world behavior we want to model.
      println("Decision: Outlier hours were kept (they represent genuine peak traffic).")
    }

    // ---------- PEAK LABEL ----------
    // Target built from LOCAL hour_of_day distribution (top 90% threshold)
    if (clean.columns.contains("hour_of_day")) {
      val (labeled, thr) = addPeakLabelByPercentile(clean, percentile = 0.90)
      clean = labeled
      println(s"========== Peak Label Threshold (Percentile 0.9) ==========")
      println(s"Hourly flights threshold = $thr")
    }

    // ---------- REPORT ----------
    val afterRows = clean.count()
    val afterCols = clean.columns.length

    println("========== AFTER CLEANING / PREPROCESSING ==========")
    println(s"After rows: $afterRows")
    println(s"After cols: $afterCols")
    println(s"Rows removed total: ${beforeRows - afterRows}")

    val missingAfter = missingSummary(clean, "AFTER")

    val missingCompare =
      missingBefore.withColumnRenamed("missing_count", "before_missing")
        .join(missingAfter.withColumnRenamed("missing_count", "after_missing"), Seq("column"), "full")
        .na.fill(0, Seq("before_missing", "after_missing"))
        .withColumn("before_missing", col("before_missing").cast("long"))
        .withColumn("after_missing", col("after_missing").cast("long"))
        .withColumn("missing_change", col("after_missing") - col("before_missing"))
        .orderBy(desc("before_missing"))

    println("========== Missing Values Before / After ==========")
    missingCompare.show(200, truncate = false)

    // Hourly counts + label distribution (for sanity)
    if (clean.columns.contains("hour_of_day") && clean.columns.contains("peak_traffic_label")) {
      println("========== Hourly Volume + Peak Label ==========")
      clean.groupBy("hour_of_day", "peak_traffic_label").count()
        .orderBy(col("hour_of_day"), col("peak_traffic_label"))
        .show(200, truncate = false)

      println("========== Peak Label Distribution ==========")
      clean.groupBy("peak_traffic_label").count().show(false)
    }

    // ---------- SAVE (TWO DATASETS: Parquet + CSV) ----------

    // 1) Save FULL dataset as Parquet (keeps array columns like movement.quality)
    clean.write.mode("overwrite").parquet(outputParquetPath)
    println(s"Saved FULL Parquet dataset to: $outputParquetPath")

    // 2) Prepare CSV-safe version (convert array columns to string)
    val cleanForCsv =
      if (clean.columns.contains("movement.quality"))
        clean.withColumn("movement.quality", concat_ws("|", c("movement.quality")))
      else clean

    // Save FULL dataset as CSV (Report Version)
    cleanForCsv
      .coalesce(1) // optional: single CSV file
      .write.mode("overwrite")
      .option("header", "true")
      .csv(outputCsvPath)

    println(s"Saved FULL CSV dataset to: $outputCsvPath")

    // ---------- EXTRA CSV REPORTS ----------
    // Snapshot 20 rows (CSV-safe)
    val snapshot = cleanForCsv.limit(20)
    snapshot.show(20, truncate = false)

    snapshot.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$resultsPath/final_snapshot_20_rows_csv")
    println(s"Saved 20-row snapshot CSV to: $resultsPath/final_snapshot_20_rows_csv")

    // Missing compare CSV
    missingCompare.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$resultsPath/missing_before_after_csv")
    println(s"Saved missing summary to: $resultsPath/missing_before_after_csv")

    // Basic stats CSV
    val basicStats = Seq(
      ("before_rows", beforeRows.toString),
      ("after_rows", afterRows.toString),
      ("rows_removed", (beforeRows - afterRows).toString),
      ("before_cols", beforeCols.toString),
      ("after_cols", afterCols.toString)
    ).toDF("metric", "value")

    basicStats.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$resultsPath/basic_stats_csv")
    println(s"Saved basic stats to: $resultsPath/basic_stats_csv")

    // Optional: Hourly counts CSV (great for report)
    if (clean.columns.contains("hour_of_day")) {
      val hourlyCounts =
        clean.groupBy("hour_of_day")
          .agg(count(lit(1)).alias("flights_in_hour"))
          .orderBy("hour_of_day")

      hourlyCounts.coalesce(1).write.mode("overwrite").option("header", "true")
        .csv(s"$resultsPath/hourly_counts_csv")
      println(s"Saved hourly counts to: $resultsPath/hourly_counts_csv")
    }

    // spark.stop()
  }
}