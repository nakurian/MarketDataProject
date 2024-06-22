import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, concat, lit, max, min, regexp_replace, round, sum, when}

object IndiaNseDataApp {
  def runNse(spark: SparkSession, bse_move15Df: DataFrame): Unit = {
    val yesterday = "2024-06-13"
    val today = "2024-06-14"

    val nse_summary_raw = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/MarketData")
      .option("dbtable", "nse_bse.nse_summary")
      .option("user", "postgres")
      .load()

    import spark.implicits._
    val nse_summary = nse_summary_raw
    val ma10WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-9, 0)
    val ma15WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-14, 0)
    val ma20WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-19, 0)
    val ma50WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-49, 0)
    val ma150WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-149, 0)
    val monthWindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-29, 0)
    val twoMonthWindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-59, 0)
    val threeMonthWindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-89, 0)
    val sixMonthWindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-179, 0)
    val range20WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-19, 0)
    val nse_summary_df =
      nse_summary
        .withColumn("ma10", avg($"close").over(ma10WindowSpec))
        .withColumn("ma10", round($"ma10", 2))
        .withColumn("ma20", avg($"close").over(ma20WindowSpec))
        .withColumn("ma20", round($"ma20", 2))
        .withColumn("ma50", avg($"close").over(ma50WindowSpec))
        .withColumn("ma50", round($"ma50", 2))
        .withColumn("ma150", avg($"close".cast("Double")).over(ma150WindowSpec))
        .withColumn("ma150", round($"ma150", 2))
        .withColumn("volumeMa10", avg($"volume").over(ma10WindowSpec))
        .withColumn("volumeMa10", round($"volumeMa10", 2))
        .withColumn("ma20lacs", avg($"in_lacs").over(ma20WindowSpec))
        .orderBy($"date1".desc)
        .withColumn("range", ($"high" / $"low"))
        .withColumn("avgRange", sum($"range").over(range20WindowSpec))
        .withColumn("adr", (($"avgRange".cast("Double") / 20) - 1) * 100)
        .withColumn("min10", min($"close").over(ma10WindowSpec))
        .withColumn("max10", max($"close").over(ma10WindowSpec))
        .withColumn("pct10d", (($"max10" - $"min10") / $"min10") * 100)
        .withColumn("min15", min($"close").over(ma15WindowSpec))
        .withColumn("max15", max($"close").over(ma15WindowSpec))
        .withColumn("pct15d", (($"max15" - $"min15") / $"min15") * 100)
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
        .withColumn("above10", when($"close" > $"ma10", 1).otherwise(0))
        .withColumn("above20", when($"close" > $"ma20", 1).otherwise(0))
        .withColumn("above50", when($"close" > $"ma50", 1).otherwise(0))
        .withColumn("symbolToCopy", concat(lit("NSE:"), $"symbol", lit(",")))
        .withColumn("symbolToCopy", regexp_replace($"symbolToCopy", lit("&"), lit("_")))
        .withColumn("symbolToCopy", regexp_replace($"symbolToCopy", lit("-"), lit("_")))
        .filter($"ma20lacs" > 10 and $"date1" === today)
        .cache()

   // val move10Df = nse_summary_df
      //.filter(($"close" > ($"min10" + ($"min10" * 0.10))) && ($"pct10d" > 20) && ($"adr" >= 4) && ($"close" >= 9))
    val move15Df = nse_summary_df
      .filter(($"close" > ($"min15" + ($"min15" * 0.10))) && ($"pct15d" > 20) && ($"close" >= 9))
    val universeDf_Narrow = nse_summary_df
      .filter(($"pctchange" > -1 && $"pctchange" < 1)  && ($"close" >= 5)) //&& ($"adr" >=3)

    val move30Df = nse_summary_df
      .filter(($"close" > ($"min30" + ($"min30" * 0.30))) && ($"pct30d" > 30) &&  $"close" >= $"ma150" && ($"adr" >=2) && ($"close" >= 9))
    val move90Df = nse_summary_df
      .filter(($"close" > ($"min90" + ($"min90" * 0.40))) && ($"pct90d" > 70)  &&  $"close" >= $"ma150" && ($"adr" >=2) && ($"close" >= 9))
    val move180Df = nse_summary_df
      .filter(($"close" > ($"min180" + ($"min180" * 0.50))) && ($"pct180d" > 90) &&  $"close" >= $"ma150" && ($"adr" >=2) && ($"close" >= 9))

    val valueDf = nse_summary_df
      .filter(($"close" <= $"ma10" && $"close" >= $"ma150") && ($"adr" >= 3) && ($"close" > 9))

    bse_move15Df.as("bse").join(move15Df.as("nse"), $"nse.isin" === $"bse.isin", "leftanti").select($"bse.symbolToCopy")
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("india_bse_ma10_move_filtered")

    val union15df = bse_move15Df.select("symbol", "symbolToCopy")
      .union(move15Df.select("symbol", "symbolToCopy")).distinct()

    union15df
        .coalesce(1)
        .write.format("csv")
        .mode("overwrite")
        .save("india_union_15")

    move15Df.select("symbolToCopy")
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("india_nse_ma10_move")

    //    move30Df
    //      .coalesce(1)
    //      .write.format("text")
    //      .mode("overwrite")
    //      .option("header", "true")
    //      .save("india_nse_ma30_move")

    //    move90Df
    //      .coalesce(1)
    //      .write.format("text")
    //      .mode("overwrite")
    //      .option("header", "true")
    //      .save("india_nse_ma90_move")

    //    move180Df
    //      .coalesce(1)
    //      .write.format("text")
    //      .mode("overwrite")
    //      .option("header", "true")
    //      .save("india_nse_ma180_move")

    //    val universeDf = move30Df.select("symbolToCopy")
    //      .union(move90Df.select("symbolToCopy"))
    //      .union(move180Df.select("symbolToCopy"))
    //      .distinct()

    //    universeDf
    //      .coalesce(1)
    //      .write.format("text")
    //      .mode("overwrite")
    //      .option("header", "true")
    //      .save("india_nse_combine")

    //Screener for narrow candles
    //    universeDf_Narrow.as("n")
    //      .join(universeDf.as("u"), Seq("symbolToCopy"), "inner").select($"u.symbolToCopy")
    //      .coalesce(1)
    //      .write
    //      .format("text")
    //      .mode("overwrite")
    //      .option("header", "true")
    //      .save("india_nse_narrow_candles")

    //Screener with candle pattern
    val nse_today_df =
      nse_summary_raw
        .withColumn("volumeMa10", avg($"volume".cast("Double")).over(ma10WindowSpec))
        .withColumn("volumeMa10", round($"volumeMa10", 2))
        .withColumn("ma20lacs", avg($"in_lacs").over(ma20WindowSpec))
        .orderBy($"date1".desc)
        //.withColumn("dollarVolume", $"volumeMa10" * $"close")
        .withColumn("range", ($"high" / $"low"))
        .withColumn("avgRange", sum($"range").over(range20WindowSpec))
        .withColumn("adr", (($"avgRange".cast("Double") / 20) - 1) * 100)
        .withColumn("symbolToCopy", concat(lit("NSE:"), $"symbol", lit(",")))
        .withColumn("symbolToCopy", regexp_replace($"symbolToCopy", lit("&"), lit("_")))
        .withColumn("symbolToCopy", regexp_replace($"symbolToCopy", lit("-"), lit("_")))
        .filter($"ma20lacs" > 10 and $"date1" === today)
        .cache()

    val nse_yesterday_df =
      nse_summary_raw
        .withColumn("volumeMa10", avg($"volume".cast("Double")).over(ma10WindowSpec))
        .withColumn("volumeMa10", round($"volumeMa10", 2))
        .withColumn("ma20lacs", avg($"in_lacs").over(ma20WindowSpec))
        .orderBy($"date1".desc)
        //.withColumn("dollarVolume", $"volumeMa10" * $"close")
        .withColumn("range", ($"high" / $"low"))
        .withColumn("avgRange", sum($"range").over(range20WindowSpec))
        .withColumn("adr", (($"avgRange".cast("Double") / 20) - 1) * 100)
        .withColumn("symbolToCopy", concat(lit("NSE:"), $"symbol", lit(",")))
        .withColumn("symbolToCopy", regexp_replace($"symbolToCopy", lit("&"), lit("_")))
        .withColumn("symbolToCopy", regexp_replace($"symbolToCopy", lit("-"), lit("_")))
        .filter($"ma20lacs" > 10 and $"date1" === yesterday)
        .cache()

    val patternDf = nse_today_df.as("t")
      .join(nse_yesterday_df.as("y"), $"t.symbol" === $"y.symbol")
      .filter($"y.close" > $"t.open" && $"y.open" - $"y.close" > 0
        && $"t.close" >= ($"y.close" + ($"y.open" - $"y.close") / 2))
      .filter($"t.close" > 5 && $"t.adr" > 3)
      .select($"t.symbolToCopy")

    patternDf.select("symbolToCopy")
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("india_nse_pattern_screener")

    //    val totalStocks = nse_summary_df.filter($"close" > 5).count()
    //    val above10 = nse_summary_df.filter($"above10" === 1).count()
    //    val above20 = nse_summary_df.filter($"above20" === 1).count()
    //    val above50 = nse_summary_df.filter($"above50" === 1).count()
    //    println("Total: " + totalStocks + " Above 10: " + above10 + " Above 20: " + above20 + " Above 50: " + above50)
  }
}
