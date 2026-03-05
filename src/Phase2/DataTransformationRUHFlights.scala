import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature._
import org.apache.spark.sql.types._

import java.nio.file.{Files, Paths}

object DataTransformationRUHFlights {

  def q(name: String): Column = col(s"`$name`")

  private def ensureDir(path: String): Unit = {
    Files.createDirectories(Paths.get(path))
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RUH Flights Data Transformation")
      .master("local[*]")
      .getOrCreate()

    // -------- PATHS (REPO-RELATIVE) --------
    val inputPath      = "data/processed/reduced_dataset.parquet"
    val outParquetPath = "data/processed/transformed_dataset.parquet"
    val outCsvPath     = "data/processed/transformed_dataset_csv"
    val pipelinePath   = "models/transformation_pipeline"

    ensureDir("data/processed")
    ensureDir("models")

    // -------- LOAD --------
    var df = spark.read.parquet(inputPath)

    println("========== INPUT SCHEMA (BEFORE TRANSFORMATION) ==========")
    df.printSchema()

    // -------- 0) RENAME dot-columns -> underscore columns --------
    val renameMap: Seq[(String, String)] = Seq(
      "airline.name" -> "airline_name",
      "movement.terminal" -> "movement_terminal",
      "movement.airport.timeZone" -> "movement_airport_timeZone",
      "aircraft.model" -> "aircraft_model"
    )

    renameMap.foreach { case (oldName, newName) =>
      if (df.columns.contains(oldName)) {
        df = df.withColumn(newName, q(oldName)).drop(oldName)
      }
    }

    println("\n========== SCHEMA (AFTER RENAMING DOT COLUMNS) ==========")
    df.printSchema()

    // -------- 1) Ensure data types --------
    def castIfExists(df: DataFrame, colName: String, dt: DataType): DataFrame = {
      if (df.columns.contains(colName)) df.withColumn(colName, col(colName).cast(dt)) else df
    }

    df = castIfExists(df, "hour_of_day", IntegerType)
    df = castIfExists(df, "day_of_week", IntegerType)
    df = castIfExists(df, "is_weekend", IntegerType)
    df = castIfExists(df, "isCargo_int", IntegerType)
    df = castIfExists(df, "peak_traffic_label", IntegerType)

    // label for Spark ML
    df = df.withColumn("label", col("peak_traffic_label").cast(DoubleType))

    // -------- 2) Clean categorical columns --------
    val categoricalColsPreferred = Seq(
      "airline_name",
      "status",
      "flight_type",
      "movement_terminal",
      "movement_airport_timeZone",
      "aircraft_model",
      "destination_airport_iata",
      "destination_airport_icao",
      "destination_airport_name"
    )

    val categoricalCols = categoricalColsPreferred.filter(df.columns.contains)

    categoricalCols.foreach { cn =>
      df = df.withColumn(
        cn,
        when(col(cn).isNull || length(trim(col(cn))) === 0, lit("UNKNOWN"))
          .otherwise(trim(col(cn)))
      )
    }

    val upperCols = Seq("destination_airport_iata", "destination_airport_icao", "movement_terminal")
      .filter(df.columns.contains)

    upperCols.foreach { cn =>
      df = df.withColumn(cn, upper(col(cn)))
    }

    // -------- 3) Encoding --------
    val indexers: Array[StringIndexer] = categoricalCols.map { cn =>
      new StringIndexer()
        .setInputCol(cn)
        .setOutputCol(s"${cn}_idx")
        .setHandleInvalid("keep")
    }.toArray

    val encoder = new OneHotEncoder()
      .setInputCols(categoricalCols.map(cn => s"${cn}_idx").toArray)
      .setOutputCols(categoricalCols.map(cn => s"${cn}_ohe").toArray)
      .setHandleInvalid("keep")

    // -------- 4) Assemble features --------
    val numericCols = Seq("hour_of_day", "day_of_week", "is_weekend", "isCargo_int")
      .filter(df.columns.contains)

    val featureCols = numericCols ++ categoricalCols.map(cn => s"${cn}_ohe")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol("features")
      .setHandleInvalid("keep")

    val pipeline = new Pipeline().setStages(indexers ++ Array(encoder, assembler))

    val model: PipelineModel = pipeline.fit(df)
    val transformed = model.transform(df)

    // -------- 5) Validate schema --------
    println("\n========== FINAL SCHEMA (AFTER TRANSFORMATION) ==========")
    transformed.select("label", "features").printSchema()

    println("\n========== SAMPLE (label + features) ==========")
    transformed.select("label", "features").show(5, truncate = false)

    val mlReady = transformed.select("label", "features")

    println("\n========== ML-READY COUNTS ==========")
    println(s"Rows = ${mlReady.count()}")

    // -------- SAVE --------
    mlReady.write.mode("overwrite").parquet(outParquetPath)
    println(s"\nSaved transformed Parquet dataset to: $outParquetPath")

    // CSV: save flat (numeric + idx + label) because features is vector
    val idxCols = categoricalCols.map(cn => s"${cn}_idx")
    val flatCols = (numericCols ++ idxCols :+ "label").filter(transformed.columns.contains)

    val flatForCsv = transformed.select(flatCols.map(col): _*)
    flatForCsv.coalesce(1).write.mode("overwrite").option("header", "true").csv(outCsvPath)
    println(s"Saved transformed CSV (flat indexed) to: $outCsvPath")

    model.write.overwrite().save(pipelinePath)
    println(s"Saved transformation pipeline to: $pipelinePath")

    // spark.stop()
  }
}