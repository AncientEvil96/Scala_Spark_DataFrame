import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object WordCount {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)

    val spark = SparkSession.builder().appName("Test DataFrame").getOrCreate()
    import spark.sqlContext.implicits._

    spark.read.option("header", "true").csv(input)
      .select(concat_ws(" ", $"class", $"comment") as "docs")
      .select(split($"docs", "\\s") as "words")
      .select(explode($"words") as "word")
      .groupBy($"word").count()
      .write.mode("overwrite").csv(output)
    spark.stop()
  }
}