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
    val stop_word = args(2).split(',')

    val spark = SparkSession.builder().appName("Test DataFrame").getOrCreate()
    import spark.sqlContext.implicits._

    spark.read.option("header", "true").csv(input)
      .select(concat_ws(" ", $"class", $"comment") as "docs")
      .select(split($"docs", "\\s") as "words")
      .select(explode($"words") as "word")
      .groupBy($"word").count() //группировка
      .select("count", "word")
      // убрали знаки припинания + все в нижний регистр
      .withColumn("word_n", regexp_replace(lower($"word"), "[^a-zA-Z ]", ""))
      .select("count", "word_n") //почему то без этого дальше не хотело идти
      .filter("word_n != \"\"") //отфильтровали пустые строки
      .filter(!$"word_n".isin(stop_word: _*)) //отфильтровали стоп слова кторые идут через запятую
      .sort($"count".desc) //сортировка по количеству
      .repartition(1) //сделаем 1 партицию вместо 200
      .write.mode("overwrite")
      .option("sep", ";") // поменяли раздилитель
      .csv(output)
    spark.stop()
  }
}

//.trim().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s")

//sbt package

//docker cp target/scala-2.11/scala_spark_dataframe_2.11-0.1.jar gbhdp:/home/hduser/

//spark-submit --class WordCount --master yarn --deploy-mode cluster scala_spark_dataframe_2.11-0.1.jar /user/hduser/ppkm/ppkm_dataset.csv /user/hduser/ppkm-df-out


//hdfs dfs -ls /user/hduser/ppkm-df-out
//hdfs dfs -cat /user/hduser/ppkm-df-out/*
//hdfs dfs -rm -r -skipTrash ppkm-df-out