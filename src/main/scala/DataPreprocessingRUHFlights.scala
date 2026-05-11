import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.PrintWriter
import java.nio.file.{Files, Paths}

object DataPreprocessingRUHFlights {
  def escapeCsv(value: Any): String = {
    val text = Option(value).getOrElse("").toString
    "\"" + text.replace("\"", "\"\"") + "\""
  }

  def saveDataFrameAsCsvNoHadoop(df: DataFrame, outputPath: String): Unit = {
    Files.createDirectories(Paths.get("results"))

    val writer = new PrintWriter(outputPath)

    writer.println(df.columns.map(escapeCsv).mkString(","))

    val rows = df.toLocalIterator()
    while (rows.hasNext) {
      val row = rows.next()
      writer.println(row.toSeq.map(escapeCsv).mkString(","))
    }

    writer.close()
  }

  // Safe column accessor for dotted column names
  def c(name: String): Column = col(s"`$name`")

  // 1) Missing summary
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

  // 2) Trim all string columns
  def withTrimmedStrings(df: DataFrame): DataFrame = {
    df.schema.fields.foldLeft(df) { (acc, f) =>
      f.dataType match {
        case StringType => acc.withColumn(f.name, trim(c(f.name)))
        case _          => acc
      }
    }
  }

  // 3) Normalize null-like strings
  def normalizeNullLikeStrings(df: DataFrame): DataFrame = {
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

  // 4) Uppercase safely
  def safeUpper(df: DataFrame, columnName: String): DataFrame = {
    if (df.columns.contains(columnName))
      df.withColumn(columnName, upper(c(columnName)))
    else df
  }

  // 5) Regex validation
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

  // 6) Timestamp parsing
  private def parseTsMulti(colStr: Column): Column = {
    val normalized = regexp_replace(colStr, "Z$", "+00:00")

    coalesce(
      to_timestamp(normalized, "yyyy-MM-dd HH:mm:ssXXX"),
      to_timestamp(normalized, "yyyy-MM-dd HH:mmXXX"),
      to_timestamp(normalized, "yyyy-MM-dd HH:mm:ssX"),
      to_timestamp(normalized, "yyyy-MM-dd HH:mmX"),
      to_timestamp(normalized)
    )
  }

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

  def addPeakLabelByTimeWindow(df: DataFrame, percentile: Double = 0.75): (DataFrame, Double) = {
    var base = df

    if (!base.columns.contains("scheduled_date_local") || !base.columns.contains("hour_of_day")) {
      return (base.withColumn("peak_traffic_label", lit(0)), Double.PositiveInfinity)
    }

    // Terminal is part of operational congestion.
    // If terminal is missing, use ALL so the code still works.
    base =
      if (base.columns.contains("movement.terminal")) {
        base.withColumn(
          "terminal_group",
          when(c("movement.terminal").isNull || length(trim(c("movement.terminal"))) === 0, lit("UNKNOWN"))
            .otherwise(c("movement.terminal"))
        )
      } else {
        base.withColumn("terminal_group", lit("ALL"))
      }

    val windowCols = Seq(
      "scheduled_date_local",
      "hour_of_day",
      "day_of_week",
      "terminal_group"
    )

    val trafficWindows =
      base.groupBy(windowCols.map(col): _*)
        .agg(count(lit(1)).alias("flights_in_window"))

    val q = trafficWindows.stat.approxQuantile("flights_in_window", Array(percentile), 0.0)
    val threshold = if (q.nonEmpty) q(0) else Double.PositiveInfinity

    val labeledWindows =
      trafficWindows.withColumn(
        "peak_traffic_label",
        when(col("flights_in_window") >= lit(threshold), lit(1)).otherwise(lit(0))
      )

    val out =
      base.join(
          labeledWindows.select((windowCols.map(col) :+ col("peak_traffic_label") :+ col("flights_in_window")): _*),
          windowCols,
          "left"
        )
        .withColumn("peak_traffic_label", when(col("peak_traffic_label").isNull, 0).otherwise(col("peak_traffic_label")))

    (out, threshold)
  }

  def buildPreprocessedData(spark: SparkSession): DataFrame = {
    spark.conf.set("spark.sql.session.timeZone", "Asia/Riyadh")
    import spark.implicits._

    val inputPath = "data/raw/flights_RUH.parquet"
    val raw = spark.read.parquet(inputPath)

    println("========== RAW DATASET ==========")
    raw.printSchema()

    val beforeRows = raw.count()
    val beforeCols = raw.columns.length
    println(s"Before rows: $beforeRows")
    println(s"Before cols: $beforeCols")

    val missingBefore = missingSummary(raw, "BEFORE")

    var clean = raw.transform(withTrimmedStrings).transform(normalizeNullLikeStrings)

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

    if (clean.columns.contains("movement.terminal")) {
      clean = clean.withColumn("movement.terminal", upper(regexp_replace(c("movement.terminal"), "\\s+", "")))
    }

    clean = addParsedTimestamps(clean)

    if (clean.columns.contains("scheduled_local_ts")) {
      val beforeFilter = clean.count()
      clean = clean.filter(col("scheduled_local_ts").isNotNull)
      val afterFilter = clean.count()
      println(s"Filtered rows where scheduled_local_ts is null: removed ${beforeFilter - afterFilter}")
    }

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

    if (clean.columns.contains("scheduled_local_ts")) {
      clean = clean
        .withColumn("scheduled_date_local", to_date(col("scheduled_local_ts")))
        .withColumn("hour_of_day", hour(col("scheduled_local_ts")))
        .withColumn("day_of_week", dayofweek(col("scheduled_local_ts")))
        .withColumn("is_weekend", when(col("day_of_week").isin(6, 7), 1).otherwise(0))
    }

    if (clean.columns.contains("isCargo")) {
      clean = clean.withColumn("isCargo_int", when(c("isCargo") === true, 1).otherwise(0))
    }

    val fillUnknown = Seq("airline.icao", "airline.iata", "destination_airport_icao", "destination_airport_iata")
      .filter(clean.columns.contains)

    fillUnknown.foreach { colName =>
      clean = clean.withColumn(colName, when(c(colName).isNull, lit("UNKNOWN")).otherwise(c(colName)))
    }

    val beforeDup = clean.count()
    clean = clean.dropDuplicates()
    val afterDup = clean.count()
    val dupRemoved = beforeDup - afterDup

    println("========== DUPLICATE CHECK ==========")
    println(s"Rows before removing duplicates: $beforeDup")
    println(s"Rows after removing duplicates:  $afterDup")
    println(s"Duplicate rows removed:          $dupRemoved")

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

      println("Decision: Outlier hours were kept (they represent genuine peak traffic).")
    }

    if (clean.columns.contains("hour_of_day")) {
      val (labeled, thr) = addPeakLabelByTimeWindow(clean, percentile = 0.75)
      clean = labeled
      println("========== Peak Label Threshold (Time Window Percentile 0.75) ==========")
      println(s"Flights per date-hour-terminal threshold = $thr")
    }

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

    if (clean.columns.contains("hour_of_day") && clean.columns.contains("peak_traffic_label")) {
      println("========== Hourly Volume + Peak Label ==========")
      clean.groupBy("hour_of_day", "peak_traffic_label").count()
        .orderBy(col("hour_of_day"), col("peak_traffic_label"))
        .show(200, truncate = false)

      println("========== Peak Label Distribution ==========")
      clean.groupBy("peak_traffic_label").count().show(false)

      println("========== Sample Traffic Windows ==========")
      clean.select(
          "scheduled_date_local",
          "hour_of_day",
          "day_of_week",
          "terminal_group",
          "flights_in_window",
          "peak_traffic_label"
        ).distinct()
        .orderBy(desc("flights_in_window"))
        .show(30, truncate = false)
    }

    clean
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RUH Flights Preprocessing")
      .master("local[*]")
      .getOrCreate()

    val clean = buildPreprocessedData(spark)

    println("========== FINAL SNAPSHOT ==========")
    clean.show(20, truncate = false)

    println("Saving preprocessed dataset without Spark CSV writer...")

    val csvReady = clean
      .withColumn("movement_quality", concat_ws(",", col("`movement.quality`")))
      .drop("movement.quality")

    saveDataFrameAsCsvNoHadoop(csvReady, "results/preprocessed_dataset.csv")

    println("Preprocessed dataset saved to results/preprocessed_dataset.csv")

    spark.stop()
  }
}