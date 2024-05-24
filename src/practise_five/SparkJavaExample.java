package practise_five;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkJavaExample {
    public static void main(String[] args) {
        // Создание SparkContext
        JavaSparkContext sc = new JavaSparkContext("local[*]", "SparkJavaExample");

        // Загрузка данных из файла
        JavaRDD<String> text = sc.textFile("/home/caxapa/Spark/ira_tweets_csv_hashed.csv");

        // Фильтрация строк и преобразование к нужному формату
        JavaRDD<String[]> cleanedData = text
                .filter(row -> !row.startsWith("\"tweetid\""))
                .map(row -> row.split("\",\""));

        // Фильтрация данных по значению в колонке и преобразование к нужному формату
        JavaRDD<String[]> filteredData = cleanedData.filter(row -> {
            float value = Float.parseFloat(row[24].replaceAll("\"", ""));
            return value >= 5.0f;
        });

        // Преобразование данных в пары ключ-значение (идентификатор пользователя, количество сообщений)
        JavaPairRDD<String, Integer> userMessagesCount = filteredData
                .mapToPair(row -> new Tuple2<>(row[1], 1))
                .reduceByKey(Integer::sum);

        // Преобразование пар ключ-значение в JavaRDD<Tuple2<String, Integer>>
        JavaRDD<Tuple2<String, Integer>> resultRDD = userMessagesCount.map(tuple -> new Tuple2<>(tuple._1(), tuple._2()));

        // Вывод результатов
        resultRDD.collect().forEach(System.out::println);
        // Остановка SparkContext
        sc.stop();
    }
}
