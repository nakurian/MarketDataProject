import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object NyseMarketDataApp {
  def main(args: Array[String]): Unit = {
    val yesterday = "2024-05-15"
    val today = "2024-05-16"
    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local")
      .config("spark.driver.memory","2g")
      .master("local[4]")
      .getOrCreate()

    val nyse_summary_raw = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/MarketData")
      .option("dbtable", "nyse.summarystock")
      .option("user", "postgres")
      .load()

    import spark.implicits._
    val nyse_summary = nyse_summary_raw.withColumn("date1", to_date(col("date"), "yyyyMMdd")).drop("date")
    val ma10WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-9, 0)
    val ma20WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-19, 0)
    val ma50WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-49, 0)
    val monthWindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-29, 0)
    val twoMonthWindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-59, 0)
    val threeMonthWindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-89, 0)
    val sixMonthWindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-179, 0)
    val range20WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-19, 0)
    val nyse_summary_df =
      nyse_summary
        .withColumn("ma10", avg($"close".cast("Double")).over(ma10WindowSpec))
        .withColumn("ma10", round($"ma10", 2))
        .withColumn("ma20", avg($"close".cast("Double")).over(ma20WindowSpec))
        .withColumn("ma20", round($"ma20", 2))
        .withColumn("ma50", avg($"close".cast("Double")).over(ma50WindowSpec))
        .withColumn("ma50", round($"ma50", 2))
        .withColumn("volumeMa10", avg($"volume".cast("Double")).over(ma10WindowSpec))
        .withColumn("volumeMa10", round($"volumeMa10", 2))
        .orderBy($"date1".desc)
        .withColumn("dollarVolume", $"volumeMa10" * $"close")
        .withColumn("range", ($"high" / $"low"))
        .withColumn("avgRange", sum($"range").over(range20WindowSpec))
        .withColumn("adr", (($"avgRange".cast("Double") / 20) - 1) * 100)
        .withColumn("min10", min($"close").over(ma10WindowSpec))
        .withColumn("max10", max($"close").over(ma10WindowSpec))
        .withColumn("pct10d", (($"max10" - $"min10") / $"min10") * 100)
        .withColumn("min20", min($"close").over(ma20WindowSpec))
        .withColumn("max20", max($"close").over(ma20WindowSpec))
        .withColumn("pct20d", (($"max20" - $"min20") / $"min20") * 100)
        .withColumn("min30", min($"close").over(monthWindowSpec))
        .withColumn("max30", max($"close").over(monthWindowSpec))
        .withColumn("pct30d", (($"max30" - $"min30") / $"min30") * 100)
        .withColumn("min60", min($"close").over(twoMonthWindowSpec))
        .withColumn("max60", max($"close").over(twoMonthWindowSpec))
        .withColumn("pct60d", (($"max60" - $"min60") / $"min60") * 100)
        .withColumn("min90", min($"close").over(threeMonthWindowSpec))
        .withColumn("max90", max($"close").over(threeMonthWindowSpec))
        .withColumn("pct90d", (($"max90" - $"min90") / $"min90") * 100)
        .withColumn("min180", min($"close").over(sixMonthWindowSpec))
        .withColumn("max180", max($"close").over(sixMonthWindowSpec))
        .withColumn("pct180d", (($"max180" - $"min180") / $"min180") * 100)
        .withColumn("symbolToCopy", concat(lit("nyse:"), $"symbol", lit(",")))
        .filter($"dollarVolume" > 500000 and $"date1" === today)
        .cache()

    val move10Df = nyse_summary_df
      .filter(($"close" > ($"min10" + ($"min10" * 0.10))) && ($"pct10d" > 20) && ($"adr" >=4) && ($"close" >= 5))
    val universeDf_Narrow = nyse_summary_df
      .filter(($"stockpctchange" > -1 && $"stockpctchange" < 1) && ($"adr" >=4) && ($"close" >= 5))
    val move20Df = nyse_summary_df
      .filter(($"close" > ($"min20" + ($"min20" * 0.10))) && ($"pct20d" > 20) && ($"adr" >=4) && ($"close" >= 5))
    val move30Df = nyse_summary_df
      .filter(($"close" > ($"min30" + ($"min30" * 0.30))) && ($"pct30d" > 30) && ($"adr" >=4) && ($"close" >= 5))
    val move60Df = nyse_summary_df
      .filter(($"close" > ($"min60" + ($"min60" * 0.20))) && ($"pct60d" > 30) && ($"adr" >=4) && ($"close" >= 5))
    val move90Df = nyse_summary_df
      .filter(($"close" > ($"min90" + ($"min90" * 0.40))) && ($"pct90d" > 40) && ($"adr" >=4) && ($"close" >= 5))
    val move180Df = nyse_summary_df
      .filter(($"close" > ($"min180" + ($"min180" * 0.50))) && ($"pct180d" > 50) && ($"adr" >=4) && ($"close" >= 5))


    move10Df
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("nyse_ma10_move")

    move30Df
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("nyse_ma30_move")

    move90Df
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("nyse_ma90_move")

    move180Df
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("nyse_ma180_move")

    val universeDf = move30Df.select("symbolToCopy")
      .union(move90Df.select("symbolToCopy"))
      .union(move180Df.select("symbolToCopy"))
      .distinct()

    universeDf
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("nyse_combine")

    //Screener for narrow candles
    universeDf_Narrow.as("n")
      .join(universeDf.as("u"), Seq("symbolToCopy"), "inner").select($"u.symbolToCopy")
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("nyse_narrow_candles")

    //Screener with candle pattern
    val nyse_today_df =
      nyse_summary
        .withColumn("volumeMa10", avg($"volume".cast("Double")).over(ma10WindowSpec))
        .withColumn("volumeMa10", round($"volumeMa10", 2))
        .orderBy($"date1".desc)
        .withColumn("dollarVolume", $"volumeMa10" * $"close")
        .withColumn("range", ($"high" / $"low"))
        .withColumn("avgRange", sum($"range").over(range20WindowSpec))
        .withColumn("adr", (($"avgRange".cast("Double") / 20) - 1) * 100)
        .withColumn("symbolToCopy", concat(lit("nyse:"), $"symbol", lit(",")))
        .filter($"dollarVolume" > 500000 and $"date1" === today)
        .cache()

    val nyse_yesterday_df =
      nyse_summary
        .withColumn("volumeMa10", avg($"volume".cast("Double")).over(ma10WindowSpec))
        .withColumn("volumeMa10", round($"volumeMa10", 2))
        .orderBy($"date1".desc)
        .withColumn("dollarVolume", $"volumeMa10" * $"close")
        .withColumn("range", ($"high" / $"low"))
        .withColumn("avgRange", sum($"range").over(range20WindowSpec))
        .withColumn("adr", (($"avgRange".cast("Double") / 20) - 1) * 100)
        .withColumn("symbolToCopy", concat(lit("nyse:"), $"symbol", lit(",")))
        .filter($"dollarVolume" > 500000 and $"date1" === yesterday)
        .cache()

    nyse_today_df.as("t")
      .join(nyse_yesterday_df.as("y"), Seq("symbol"))
      .filter($"y.close" > $"t.open" && $"y.open" - $"y.close" > 0
        && $"t.close" >= ($"y.close" + ($"y.open" - $"y.close") / 2))
      .filter($"t.close" > 5 && $"t.adr" > 3)
      .select($"t.symbolToCopy")
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("nyse_pattern_screener")

    spark.stop()
  }
}

//    val filtered_nyse_summary_df = nyse_summary_df
//      .filter($"close" >= $"ma10" && $"close" >= $"ma20" && $"close" >= $"ma50")
//      .filter($"close" >= 5)
//    println("Total stocks close greater than all moving averages ", filtered_nyse_summary_df.count())
//
//    filtered_nyse_summary_df.coalesce(1)
//      .write.format("com.databricks.spark.csv")
//      .mode("overwrite")
//      .option("header", "true")
//      .save("nyse_gt_ma")
//
//    filtered_nyse_summary_df.filter($"adr" > 4)
//      .coalesce(1)
//      .write.format("com.databricks.spark.csv")
//      .mode("overwrite")
//      .option("header", "true")
//      .save("nyse_adr_gt_four")

//val ma10MinMaxWindowSpec = Window.partitionBy($"symbol", $"close").orderBy($"date1".asc).rowsBetween(-9, 0)