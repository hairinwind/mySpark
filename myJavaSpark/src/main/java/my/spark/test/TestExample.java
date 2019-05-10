package my.spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * This is the sample for junit test
 * 
 * @author yao.dong
 *
 */
public class TestExample {

	public static void main(String[] args) {
		String appName = "TestExample";
		SparkConf conf = new SparkConf().setAppName(appName);
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> input = sc.textFile("src/main/resources/test.txt");

		JavaPairRDD<String, Integer> suitsAndValues = runETL(input);

		// Output the number of hearts found
		/*this is the original sample code*/
		//suitsAndValues.saveAsTextFile(output);
		
		/*I don't want to save the file, I only want to output it to the console
		but I get the error org.apache.spark.SparkException: Task not serializable
		because object not serializable (class: java.io.PrintStream...*/ 
		//suitsAndValues.foreach(System.out::println);
		/* why this is allowed ???  */
		suitsAndValues.foreach(x -> System.out.println(x));
	}

	public static JavaPairRDD<String, Integer> runETL(JavaRDD<String> input) {
		// Split up into suits and numbers and transform into pairs
		JavaPairRDD<String, Integer> suitsAndValues = input.mapToPair(w -> {
			String[] split = w.split("\t");

			int cardValue = Integer.parseInt(split[0]);
			String cardSuit = split[1];

			return new Tuple2<String, Integer>(cardSuit, cardValue);
		});

		return suitsAndValues;
	}

}
