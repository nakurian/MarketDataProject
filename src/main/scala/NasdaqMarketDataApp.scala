import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, lit, round, sum, to_date, min, max, concat}

import scala.Console.println

object NasdaqMarketDataApp {
  def main(args: Array[String]): Unit = {
    val today = "2024-05-03"
    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local")
      .config("spark.driver.memory","2g")
      .master("local[4]")
      .getOrCreate()

    val nasdaq_summary_raw = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/MarketData")
      .option("dbtable", "nasdaq.summarystock")
      .option("user", "postgres")
      .load()

    import spark.implicits._
    val nasdaq_summary = nasdaq_summary_raw.withColumn("date1", to_date(col("date"), "yyyyMMdd"))
    val ma10WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-9, 0)
    val ma20WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-19, 0)
    val ma50WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-49, 0)
    val range20WindowSpec = Window.partitionBy($"symbol").orderBy($"date1".asc).rowsBetween(-19, 0)
    val nasdaq_summary_df =
      nasdaq_summary.drop("date")
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
        .withColumn("symbolToCopy", concat(lit("NASDAQ:"), $"symbol", lit(",")))
        .filter($"dollarVolume" > 500000 and $"date1" === today)
    nasdaq_summary_df.show(10)
    println("Total count greater than 500000 dollar volume ", nasdaq_summary_df.count())
//    val filtered_nasdaq_summary_df = nasdaq_summary_df
//      .filter($"close" >= $"ma10" && $"close" >= $"ma20" && $"close" >= $"ma50")
//      .filter($"close" >= 5)
//    println("Total stocks close greater than all moving averages ", filtered_nasdaq_summary_df.count())
//
//    filtered_nasdaq_summary_df.coalesce(1)
//      .write.format("com.databricks.spark.csv")
//      .mode("overwrite")
//      .option("header", "true")
//      .save("nasdaq_gt_ma")
//
//    filtered_nasdaq_summary_df.filter($"adr" > 4)
//      .coalesce(1)
//      .write.format("com.databricks.spark.csv")
//      .mode("overwrite")
//      .option("header", "true")
//      .save("nasdaq_adr_gt_four")

    //val ma10MinMaxWindowSpec = Window.partitionBy($"symbol", $"close").orderBy($"date1".asc).rowsBetween(-9, 0)
    val move20Df = nasdaq_summary_df
      .withColumn("pct10d", (($"max10" - $"min10") / $"min10") * 100)
      .filter(($"close" > ($"min10" + ($"min10" * 0.10))) && ($"pct10d" > 20) && ($"adr" >=4) && ($"close" >= 5))

    move20Df.show(10)
    move20Df.filter($"symbol".equalTo("TSLA")).show()

    move20Df
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("nasdaq_ma20_move")

    spark.stop()
  }
}