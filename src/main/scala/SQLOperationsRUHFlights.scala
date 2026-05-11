import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.io.PrintWriter

object SQLOperationsRUHFlights {

    def appendQueryToCsv(writer: PrintWriter, queryName: String, df: org.apache.spark.sql.DataFrame): Unit = {
        df.collect().foreach { row =>
            val rowText = df.columns.zip(row.toSeq).map { case (colName, value) =>
                s"$colName=${Option(value).getOrElse("").toString}"
            }.mkString(" | ")

            writer.println(s""""$queryName","$rowText"""")
        }
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .appName("RUH Flights SQL Operations")
          .master("local[*]")
          .getOrCreate()

        // Build the preprocessed DataFrame from Phase 2/3
        val df = DataPreprocessingRUHFlights.buildPreprocessedData(spark)

        // Register temporary SQL view
        df.createOrReplaceTempView("ruh_flights")

        // =========================================================
        // QUERY 1: Hourly Traffic with 3-Hour Rolling Average
        //          and Intensity Quartile (NTILE)
        // Detects sustained peak periods, not just single-hour spikes,
        // and tiers each hour by traffic intensity.
        // SQL features: window frame (ROWS BETWEEN), NTILE,
        //               windowed AVG over an ordered partition.
        // =========================================================
        println("\n========== QUERY 1: ROLLING 3-HR AVG + INTENSITY QUARTILE ==========")
        val q1 = spark.sql("""
      WITH hourly_counts AS (
        SELECT hour_of_day, COUNT(*) AS flights_in_hour
        FROM ruh_flights
        WHERE hour_of_day IS NOT NULL
        GROUP BY hour_of_day
      )
      SELECT
        hour_of_day,
        flights_in_hour,
        ROUND(AVG(flights_in_hour) OVER (
          ORDER BY hour_of_day
          ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ), 2) AS rolling_3hr_avg,
        NTILE(4) OVER (ORDER BY flights_in_hour DESC) AS intensity_quartile
      FROM hourly_counts
      ORDER BY hour_of_day
    """)
        q1.show(24, false)

        // =========================================================
        // QUERY 2: Hour-over-Hour Surge Detection (LAG)
        // Identifies hours where traffic ramps up significantly
        // compared to the previous hour - useful for early-warning
        // and proactive resource scheduling before peak onset.
        // SQL features: LAG window function, derived percentage change.
        // =========================================================
        println("\n========== QUERY 2: HOUR-OVER-HOUR SURGE (LAG) ==========")
        val q2 = spark.sql("""
      WITH hourly_counts AS (
        SELECT hour_of_day, COUNT(*) AS flights_in_hour
        FROM ruh_flights
        WHERE hour_of_day IS NOT NULL
        GROUP BY hour_of_day
      )
      SELECT
        hour_of_day,
        flights_in_hour,
        LAG(flights_in_hour) OVER (ORDER BY hour_of_day) AS prev_hour_flights,
        flights_in_hour - LAG(flights_in_hour) OVER (ORDER BY hour_of_day) AS delta,
        ROUND(
          100.0 * (flights_in_hour - LAG(flights_in_hour) OVER (ORDER BY hour_of_day))
          / LAG(flights_in_hour) OVER (ORDER BY hour_of_day), 2
        ) AS pct_change
      FROM hourly_counts
      ORDER BY pct_change DESC NULLS LAST
    """)
          q2.show(24, false)

        // =========================================================
        // QUERY 3: Peak Concentration Ratio per Airline
        // For each airline, what fraction of its flights fall in
        // peak hours? Reveals carriers that disproportionately
        // contribute to congestion (not just the largest carriers).
        // SQL features: conditional aggregation (SUM CASE WHEN),
        //               HAVING for volume threshold.
        // =========================================================
        println("\n========== QUERY 3: AIRLINE PEAK CONCENTRATION RATIO ==========")
        val q3 = spark.sql("""
      SELECT
        COALESCE(`airline.name`, 'UNKNOWN') AS airline_name,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN peak_traffic_label = 1 THEN 1 ELSE 0 END) AS peak_flights,
        ROUND(
          100.0 * SUM(CASE WHEN peak_traffic_label = 1 THEN 1 ELSE 0 END) / COUNT(*),
          2
        ) AS peak_share_pct
      FROM ruh_flights
      WHERE peak_traffic_label IS NOT NULL
      GROUP BY `airline.name`
      HAVING COUNT(*) >= 1000
      ORDER BY peak_share_pct DESC
      LIMIT 10
    """)
          q3.show(false)

        // =========================================================
        // QUERY 4: Top Peak Destination per Airline (Partitioned Rank)
        // For each airline, which destination is its #1 peak-hour
        // route? Much richer than overall top destinations - gives a
        // per-carrier hotspot map for peak congestion modeling.
        // SQL features: ROW_NUMBER() OVER (PARTITION BY ...),
        //               multi-CTE chained.
        // =========================================================
        println("\n========== QUERY 4: TOP PEAK DESTINATION PER AIRLINE ==========")
        val q4 = spark.sql("""
      WITH airline_dest_peak AS (
        SELECT
          COALESCE(`airline.name`, 'UNKNOWN') AS airline_name,
          COALESCE(destination_airport_iata, 'UNKNOWN') AS destination,
          COUNT(*) AS peak_flight_count
        FROM ruh_flights
        WHERE peak_traffic_label = 1
        GROUP BY `airline.name`, destination_airport_iata
      ),
      ranked AS (
        SELECT
          airline_name,
          destination,
          peak_flight_count,
          ROW_NUMBER() OVER (
            PARTITION BY airline_name
            ORDER BY peak_flight_count DESC
          ) AS rn
        FROM airline_dest_peak
      )
      SELECT airline_name, destination, peak_flight_count
      FROM ranked
      WHERE rn = 1
      ORDER BY peak_flight_count DESC
      LIMIT 10
    """)
          q4.show(false)

        // =========================================================
        // QUERY 5: Multi-Dimensional Summary with GROUPING SETS
        // Single query that produces:
        //   - flights per (hour_of_day, is_weekend)
        //   - flights per hour_of_day (across both day types)
        //   - flights per is_weekend (across all hours)
        //   - grand total
        // Demonstrates a SQL feature with no clean RDD equivalent.
        // =========================================================
        println("\n========== QUERY 5: MULTI-DIM AGGREGATION (GROUPING SETS) ==========")
        val q5 = spark.sql("""
      SELECT
        hour_of_day,
        CASE
          WHEN is_weekend = 1 THEN 'Weekend'
          WHEN is_weekend = 0 THEN 'Weekday'
          ELSE 'ALL_DAYS'
        END AS day_type,
        COUNT(*) AS flight_count
      FROM ruh_flights
      WHERE hour_of_day IS NOT NULL AND is_weekend IS NOT NULL
      GROUP BY GROUPING SETS (
        (hour_of_day, is_weekend),
        (hour_of_day),
        (is_weekend),
        ()
      )
      ORDER BY hour_of_day NULLS LAST, day_type
    """)
          q5.show(60, false)

        // =========================================================
        // QUERY 6: Percentile-based Peak Hour Classification
        // Tiers each hour by its statistical position in the
        // hourly-traffic distribution. Provides an alternative
        // peak definition that validates the preprocessing label.
        // SQL features: PERCENT_RANK window function, CASE bucketing.
        // =========================================================
        println("\n========== QUERY 6: PERCENTILE-BASED PEAK CLASSIFICATION ==========")
        val q6 = spark.sql("""
      WITH hourly_counts AS (
        SELECT hour_of_day, COUNT(*) AS flights_in_hour
        FROM ruh_flights
        WHERE hour_of_day IS NOT NULL
        GROUP BY hour_of_day
      )
      SELECT
        hour_of_day,
        flights_in_hour,
        ROUND(PERCENT_RANK() OVER (ORDER BY flights_in_hour), 3) AS percentile_rank,
        CASE
          WHEN PERCENT_RANK() OVER (ORDER BY flights_in_hour) >= 0.75 THEN 'Peak'
          WHEN PERCENT_RANK() OVER (ORDER BY flights_in_hour) >= 0.50 THEN 'High'
          WHEN PERCENT_RANK() OVER (ORDER BY flights_in_hour) >= 0.25 THEN 'Moderate'
          ELSE 'Low'
        END AS intensity_tier
      FROM hourly_counts
      ORDER BY flights_in_hour DESC
    """)
          q6.show(24, false)

        // =========================================================
        // QUERY 7: Average Hourly Traffic (Weekday vs Weekend)
        // Kept from Phase 4 v1 - uses CTE + aggregation to compare
        // mean hourly load across day types. Confirms day-type as a
        // strong predictive feature for peak modeling.
        // =========================================================
        println("\n========== QUERY 7: AVG FLIGHTS PER HOUR (WEEKDAY VS WEEKEND) ==========")
        val q7 = spark.sql("""
      WITH hourly_counts AS (
        SELECT
          is_weekend,
          hour_of_day,
          COUNT(*) AS flights_in_hour
        FROM ruh_flights
        WHERE is_weekend IS NOT NULL
          AND hour_of_day IS NOT NULL
        GROUP BY is_weekend, hour_of_day
      )
      SELECT
        CASE
          WHEN is_weekend = 1 THEN 'Weekend'
          WHEN is_weekend = 0 THEN 'Weekday'
        END AS day_type,
        ROUND(AVG(flights_in_hour), 2) AS avg_hourly_traffic
      FROM hourly_counts
      GROUP BY is_weekend
      ORDER BY avg_hourly_traffic DESC
    """)
          q7.show(false)

        // =========================================================
        // QUERY 8: Top 5 Destinations During Peak Hours (DENSE_RANK)
        // Kept from Phase 4 v1 - uses window function to rank
        // destinations by peak-hour flight volume. Pinpoints routes
        // most associated with operational pressure.
        // =========================================================
        println("\n========== QUERY 8: TOP DESTINATIONS DURING PEAK HOURS (DENSE_RANK) ==========")
        val q8 = spark.sql("""
      WITH destination_counts AS (
        SELECT
          COALESCE(destination_airport_iata, 'UNKNOWN') AS destination,
          COUNT(*) AS flight_count
        FROM ruh_flights
        WHERE peak_traffic_label = 1
        GROUP BY destination_airport_iata
      ),
      ranked_destinations AS (
        SELECT
          destination,
          flight_count,
          DENSE_RANK() OVER (ORDER BY flight_count DESC) AS rank_num
        FROM destination_counts
      )
      SELECT destination, flight_count, rank_num
      FROM ranked_destinations
      WHERE rank_num <= 5
      ORDER BY rank_num, destination
    """)
          q8.show(false)

        val writer = new PrintWriter("results/sql_results.csv")

        writer.println("query_name,result")

        appendQueryToCsv(writer, "Query 1: Rolling 3-Hour Average + Intensity Quartile", q1)
        appendQueryToCsv(writer, "Query 2: Hour-over-Hour Surge", q2)
        appendQueryToCsv(writer, "Query 3: Airline Peak Concentration Ratio", q3)
        appendQueryToCsv(writer, "Query 4: Top Peak Destination per Airline", q4)
        appendQueryToCsv(writer, "Query 5: Multi-Dimensional Aggregation", q5)
        appendQueryToCsv(writer, "Query 6: Percentile-Based Peak Classification", q6)
        appendQueryToCsv(writer, "Query 7: Average Flights Per Hour", q7)
        appendQueryToCsv(writer, "Query 8: Top Peak Destinations During Peak Hours", q8)

        writer.close()

        println("Saved SQL results to results/sql_results.csv")
        spark.stop()
    }
}
