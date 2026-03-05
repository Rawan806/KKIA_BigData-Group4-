import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Paths}

object DataReductionRUHFlights {

  def c(name: String): Column = col(s"`$name`")

  private def ensureDir(path: String): Unit = {
    Files.createDirectories(Paths.get(path))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RUH Flights Data Reduction")
      .master("local[*]")
      .getOrCreate()

    // -------- PATHS (REPO-RELATIVE) --------
    val inputPath      = "data/processed/preprocessed_dataset.parquet"
    val outParquetPath = "data/processed/reduced_dataset.parquet"
    val outCsvPath     = "data/processed/reduced_dataset_csv"

    ensureDir("data/processed")

    // -------- LOAD --------
    val df = spark.read.parquet(inputPath)

    val beforeRows = df.count()
    val beforeCols = df.columns.length

    println("========== BEFORE REDUCTION ==========")
    println(s"Rows: $beforeRows")
    println(s"Columns: $beforeCols")
    df.printSchema()

    // -------- KEEP COLUMNS --------
    val keepColsPreferred = Seq(
      "hour_of_day", "day_of_week", "is_weekend",
      "airline.name", "status",
      "flight_type", "isCargo_int",
      "movement.terminal", "movement.airport.timeZone",
      "aircraft.model",
      "destination_airport_iata", "destination_airport_icao", "destination_airport_name",
      "peak_traffic_label"
    )

    val keepCols = keepColsPreferred.filter(df.columns.contains)

    // -------- REDUCE --------
    val reduced = df.select(keepCols.map(c): _*)

    val afterRows = reduced.count()
    val afterCols = reduced.columns.length

    val droppedCols = df.columns.toSet.diff(keepCols.toSet).toSeq.sorted

    println("\n========== AFTER REDUCTION ==========")
    println(s"Rows: $afterRows")
    println(s"Columns: $afterCols")
    println(s"Rows removed: ${beforeRows - afterRows}")
    println(s"Columns removed: ${beforeCols - afterCols}")

    println("\n========== KEPT COLUMNS ==========")
    keepCols.foreach(println)

    println("\n========== DROPPED COLUMNS ==========")
    droppedCols.foreach(println)

    reduced.printSchema()

    // -------- SAVE (PARQUET) --------
    reduced.write.mode("overwrite").parquet(outParquetPath)
    println(s"\nSaved reduced Parquet dataset to: $outParquetPath")

    // -------- SAVE (CSV) --------
    reduced.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(outCsvPath)

    println(s"Saved reduced CSV dataset to: $outCsvPath")

    // spark.stop()
  }
}