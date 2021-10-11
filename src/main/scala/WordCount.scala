import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


//Соберите WordCount приложение, запустите на датасете ppkm_sentiment
//* Измените WordCount так, чтобы он удалял знаки препинания и приводил все слова к единому регистру
//* Добавьте в WordCount возможность через конфигурацию задать список стоп-слов, которые будут отфильтрованы во время работы приложения
//Измените выход WordCount так, чтобы сортировка была по количеству повторений, а список слов был во втором столбце, а не в первом
//* Почему в примере в выходном файле получилось 200 партиций?


object WordCount {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)

    val spark = SparkSession.builder().appName("Test DataFrame").getOrCreate()
    import spark.sqlContext.implicits._

    spark.read.option("header", "true").csv(input)
      .select(concat_ws(" ", $"class", $"comment") as "docs")
      .select(split($"docs", "\\s") as "words")
      .map(words => words.toString().trim().replaceAll("[^a-zA-Z ]", "").toLowerCase())
      .select(explode($"words") as "word")
      .groupBy($"word").count()
      .sort($"count".desc)
      .select("count", "word")
      .write.mode("overwrite").csv(output)
    spark.stop()
  }
}

//.trim().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s")

//sbt package

//docker cp target/scala-2.11/scala_spark_dataframe_2.11-0.1.jar gbhdp:/home/hduser/

//spark-submit --class WordCount --master yarn --deploy-mode cluster scala_spark_dataframe_2.11-0.1.jar /user/hduser/ppkm/ppkm_dataset.csv /user/hduser/ppkm-df-out


//hdfs dfs -ls /user/hduser/ppkm-df-out
//hdfs dfs -cat /user/hduser/ppkm-df-out/*