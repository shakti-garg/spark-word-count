package spark;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.regex.Pattern;

public final class JavaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <inputFile> <outputFile>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("WordCountInJava")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).toJavaRDD();

        JavaPairRDD<String, Integer> counts = lines
                .flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b);

        counts.saveAsTextFile(args[1]);

        spark.stop();
    }
}
