import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EventDataQualityAnalysis {

  def main(args: Array[String]): Unit = {

    // ---------------------------------------------------------
    // 1. Create SparkSession
    // ---------------------------------------------------------

    val spark = SparkSession.builder()
      .appName("Event Data Quality and Analysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n========== EVENT DATA QUALITY & ANALYSIS ==========\n")

    // ---------------------------------------------------------
    // 2. Define suitable schema
    // ---------------------------------------------------------

    val eventSchema = StructType(Seq(
      StructField("event_id", IntegerType, nullable = false),
      StructField("user_id", StringType, nullable = false),
      StructField("event_type", StringType, nullable = true),
      StructField("event_date", DateType, nullable = true),
      StructField("duration_seconds", IntegerType, nullable = true),
      StructField("device", StringType, nullable = true)
    ))

    // ---------------------------------------------------------
    // 3. Read CSV
    // ---------------------------------------------------------

    val inputPath = "data/events.csv"

    val events = spark.read
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .schema(eventSchema)
      .csv(inputPath)

    println("========== SCHEMA ==========")
    events.printSchema()

    println("\n========== SAMPLE ROWS ==========")
    events.show(10, truncate = false)

    println("\n========== RECORD COUNT ==========")
    println(s"Total records: ${events.count()}")

    // ---------------------------------------------------------
    // 4. Null-count report
    // ---------------------------------------------------------

    println("\n========== NULL COUNT REPORT ==========")

    val nullReport = events.select(
      events.columns.map(
        c => sum(when(col(c).isNull, 1).otherwise(0)).alias(c)
      ): _*
    )

    nullReport.show(false)

    // ---------------------------------------------------------
    // 5. Find duplicate event_id values
    // ---------------------------------------------------------

    println("\n========== DUPLICATE EVENT IDs ==========")

    val duplicateEventIds = events
      .groupBy("event_id")
      .count()
      .filter(col("count") > 1)

    duplicateEventIds.show(false)

    // ---------------------------------------------------------
    // 6. Remove duplicate records
    // ---------------------------------------------------------

    val deduplicatedEvents = events.dropDuplicates("event_id")

    println("\n========== AFTER DUPLICATE REMOVAL ==========")
    println(s"Records before deduplication: ${events.count()}")
    println(s"Records after deduplication: ${deduplicatedEvents.count()}")

    // ---------------------------------------------------------
    // 7. Standardize event_type and device
    // ---------------------------------------------------------

    val standardizedEvents = deduplicatedEvents
      .withColumn(
        "event_type",
        lower(trim(col("event_type")))
      )
      .withColumn(
        "device",
        lower(trim(col("device")))
      )

    println("\n========== STANDARDIZED DATA ==========")
    standardizedEvents.show(20, truncate = false)

    // ---------------------------------------------------------
    // 8. Handle missing duration_seconds
    // ---------------------------------------------------------

    val medianDuration = standardizedEvents
      .stat
      .approxQuantile(
        "duration_seconds",
        Array(0.5),
        0.0
      )(0)

    println(s"\nMedian duration used for missing values: $medianDuration seconds")

    val cleanedEvents = standardizedEvents
      .withColumn(
        "duration_seconds",
        when(
          col("duration_seconds").isNull,
          lit(medianDuration.toInt)
        ).otherwise(col("duration_seconds"))
      )

    println("\n========== CLEANED DATA ==========")
    cleanedEvents.show(20, truncate = false)

    // ---------------------------------------------------------
    // 9. Verify remaining nulls
    // ---------------------------------------------------------

    println("\n========== NULL COUNT AFTER CLEANING ==========")

    val finalNullReport = cleanedEvents.select(
      cleanedEvents.columns.map(
        c => sum(when(col(c).isNull, 1).otherwise(0)).alias(c)
      ): _*
    )

    finalNullReport.show(false)

    // ---------------------------------------------------------
    // 10. Event count and average duration by event_type
    // ---------------------------------------------------------

    println("\n========== EVENT COUNT & AVERAGE DURATION BY EVENT TYPE ==========")

    val eventTypeSummary = cleanedEvents
      .groupBy("event_type")
      .agg(
        count("*").alias("event_count"),
        round(avg("duration_seconds"), 2).alias("average_duration_seconds")
      )
      .orderBy("event_type")

    eventTypeSummary.show(false)

    // ---------------------------------------------------------
    // 11. Event count by event_date
    // ---------------------------------------------------------

    println("\n========== EVENT COUNT BY DATE ==========")

    val eventDateSummary = cleanedEvents
      .groupBy("event_date")
      .count()
      .withColumnRenamed("count", "event_count")
      .orderBy("event_date")

    eventDateSummary.show(false)

    // ---------------------------------------------------------
    // 12. Explain Spark execution plan
    // ---------------------------------------------------------

    println("\n========== EXPLAIN PLAN ==========")

    eventTypeSummary.explain(true)

    println("\n========== PROJECT COMPLETED ==========\n")

    spark.stop()
  }
}
