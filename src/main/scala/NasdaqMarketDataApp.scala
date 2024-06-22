import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.Console.println

object NasdaqMarketDataApp {
  def main(args: Array[String]): Unit = {
    val yesterday = "2024-06-20"
    val today = "2024-06-21"
    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local")
      .config("spark.driver.memory", "2g")
      .master("local[4]")
      .getOrCreate()

    val nasdaq_summary_raw = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/MarketData")
      .option("dbtable", "nasdaq.summarystock")
      .option("user", "postgres")
      .load()

    val outstandingDf = spark.read.format("csv").option("header", "true").load("outstandingShares.csv")

    import spark.implicits._
    val nasdaq_summary = nasdaq_summary_raw.withColumn("date1", to_date(col("date"), "yyyyMMdd")).drop("date")
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
    val nasdaq_summary_df =
      nasdaq_summary
        .withColumn("ma10", avg($"close".cast("Double")).over(ma10WindowSpec))
        .withColumn("ma10", round($"ma10", 2))
        .withColumn("ma20", avg($"close".cast("Double")).over(ma20WindowSpec))
        .withColumn("ma20", round($"ma20", 2))
        .withColumn("ma50", avg($"close".cast("Double")).over(ma50WindowSpec))
        .withColumn("ma50", round($"ma50", 2))
        .withColumn("ma150", avg($"close".cast("Double")).over(ma150WindowSpec))
        .withColumn("ma150", round($"ma150", 2))
        .withColumn("volumeMa10", avg($"volume".cast("Double")).over(ma10WindowSpec))
        .withColumn("volumeMa10", round($"volumeMa10", 2))
        .withColumn("volumeSum90", sum($"volume".cast("Double")).over(threeMonthWindowSpec))
        .withColumn("volumeSum90", round($"volumeSum90", 2))
        .orderBy($"date1".desc)
        .withColumn("dollarVolume", $"volumeMa10" * $"close")
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
        .withColumn("above150", when($"close" > $"ma150", 1).otherwise(0))
        .withColumn("symbolToCopy", concat(lit("NASDAQ:"), $"symbol", lit(",")))
        .filter($"dollarVolume" > 1000000 and $"date1" === today)
        .cache()

    val move15Df = nasdaq_summary_df
      .filter(($"close" > ($"min15" + ($"min15" * 0.10))) && ($"pct15d" > 20)  && ($"close" >= 9)) //&& ($"adr" >= 4)
    val universeDf_Narrow = nasdaq_summary_df
      .filter(($"stockpctchange" > -1 && $"stockpctchange" < 1)  && ($"close" >= 5)) //&& ($"adr" >= 3)

    val move30Df = nasdaq_summary_df
      .filter(($"close" > ($"min30" + ($"min30" * 0.30))) && ($"pct30d" > 30)  &&  $"close" >= $"ma150" && ($"adr" >= 2) && ($"close" >= 9))
    val move90Df = nasdaq_summary_df
      .filter(($"close" > ($"min90" + ($"min90" * 0.40))) && ($"pct90d" > 70) &&  $"close" >= $"ma150" && ($"adr" >= 2) && ($"close" >= 9))
    val move180Df = nasdaq_summary_df
      .filter(($"close" > ($"min180" + ($"min180" * 0.50))) && ($"pct180d" > 90) &&  $"close" >= $"ma150" && ($"adr" >= 2) && ($"close" >= 9))

    val valueDf = nasdaq_summary_df
      .filter(($"close" <= $"ma10" && $"close" >= $"ma150") && ($"adr" >= 3) && ($"close" > 9))

    val floatCalcDf = outstandingDf.as("o").join(nasdaq_summary_df.as("s"), Seq("symbol"), "inner")
      .filter($"o.outstanding"/$"s.volumeSum90" < 1.42 && $"s.close" > 9 && $"o.outstanding" > 0
        && $"s.close" >= $"s.ma150").select("symbolToCopy")

    floatCalcDf.coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("nasdaq_float_stocks")

    valueDf.select("symbolToCopy")
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("nasdaq_value_stocks")

    move15Df.select("symbolToCopy")
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("nasdaq_ma15_move")


    val universeDf = move30Df.select("symbolToCopy")
      .union(move90Df.select("symbolToCopy"))
      .union(move180Df.select("symbolToCopy"))
      .distinct()

    universeDf
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("nasdaq_combine")

    val totalStocks = nasdaq_summary_df.filter($"close" > 5).count()
    val above10 = nasdaq_summary_df.filter($"above10" === 1 && $"above20" === 1 && $"above50" === 1 && $"above150" === 1 ).count()
    val above20 = nasdaq_summary_df.filter($"above20" === 1 && $"above50" === 1 && $"above150" === 1).count()
    val above50 = nasdaq_summary_df.filter($"above50" === 1 && $"above150" === 1).count()
    val bestUptrendDf = nasdaq_summary_df
      .filter($"above10" === 1
        && $"above20" === 1
        && $"above50" === 1
        && $"above150" === 1 && $"close" > 9)

    bestUptrendDf.filter($"adr" >= 5).select("symbolToCopy")
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("nasdaq_best_stocks")

    nasdaq_summary_df.filter($"above20" === 1 && $"above50" === 1 && $"above150" === 1 )
      .filter($"close" <= 10 && $"pct90d" >= 70).select("symbolToCopy")
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("nasdaq_best_under10")

    nasdaq_summary_df.filter($"above50" === 1 && $"above150" === 1 )
      .filter($"close" > 10 &&  $"close" <= 30 && $"pct90d" >= 70).select("symbolToCopy")
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("nasdaq_best_under30")

    nasdaq_summary_df.filter($"above50" === 1 && $"above150" === 1 )
      .filter($"close" > 30 &&  $"close" <= 50 && $"pct90d" >= 70).select("symbolToCopy")
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("nasdaq_best_under50")

    nasdaq_summary_df.filter($"above50" === 1 && $"above150" === 1 )
      .filter($"close" > 50 &&  $"close" <= 100 && $"pct90d" >= 70).select("symbolToCopy")
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("nasdaq_best_under100")

    nasdaq_summary_df.filter($"above50" === 1 && $"above150" === 1 )
      .filter($"close" > 100 && $"pct90d" >= 70).select("symbolToCopy")
      .coalesce(1)
      .write.format("text")
      .mode("overwrite")
      .save("nasdaq_best_above100")

    val best_uptrend = bestUptrendDf.count()
    println("Total: " + totalStocks + " Above 10: "+ above10 +" Above 20: " + above20 + " Above 50: " + above50)
    println("Best uptrend Count: " + best_uptrend)
    spark.stop()
  }
}