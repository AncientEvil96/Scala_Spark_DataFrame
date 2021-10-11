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

//sbt package

//docker cp target/scala-2.11/scalasparkdf_2.11-0.1.jar gbspark:/home/hduser/

//spark-submit \
//  --class WordCount \
//--master yarn \
//  --deploy-mode cluster \
//scalasparkdf_2.11-0.1.jar \
//  /user/hduser/ppkm/ppkm_dataset.csv /user/hduser/scala-ppkm-df-out


//hdfs dfs -ls /user/hduser/scala-ppkm-df-out
//hdfs dfs -cat /user/hduser/scala-ppkm-df-out/*