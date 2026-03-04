import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._

object DataReductionRUHFlights {

  // Safe accessor for dot-names like "movement.terminal"
  def c(name: String): Column = col(s"`$name`")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RUH Flights Data Reduction")
      .master("local[*]")
      .getOrCreate()

    // -------- PATHS --------
    val inputPath      = "C:/Users/Hessa/data/preprocessed_dataset.parquet"
    val outParquetPath = "C:/Users/Hessa/data/reduced_dataset.parquet"
    val outCsvPath     = "C:/Users/Hessa/data/reduced_dataset_csv"

    // -------- LOAD --------
    val df = spark.read.parquet(inputPath)

    val beforeRows = df.count()
    val beforeCols = df.columns.length

    println("========== BEFORE REDUCTION ==========")
    println(s"Rows: $beforeRows")
    println(s"Columns: $beforeCols")
    df.printSchema()

    // -------- KEEP COLUMNS (your final choice) --------
    val keepColsPreferred = Seq(
      // time features
      "hour_of_day", "day_of_week", "is_weekend",

      // airline + operational
      "airline.name", "status",

      // flight type + cargo
      "flight_type", "isCargo_int",

      // terminal + timezone
      "movement.terminal", "movement.airport.timeZone",

      // aircraft
      "aircraft.model",

      // destination (keep all destination-related)
      "destination_airport_iata", "destination_airport_icao", "destination_airport_name",

      // target
      "peak_traffic_label"
    )

    // keep only columns that actually exist (safe)
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
    // CSV doesn't support complex types like arrays/structs.
    // Our reduced DF should be only primitive/string columns, so it's safe.
    reduced.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(outCsvPath)

    println(s"Saved reduced CSV dataset to: $outCsvPath")

    spark.stop()
  }
}