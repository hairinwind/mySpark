package my.spark.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class MyRDD {
	public static void main(String[] args) {
		String appName = "myRDD";
		SparkConf conf = new SparkConf().setAppName(appName);
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data);
		
//		System.out.println("distData.count: " + distData.count());
		
		
		JavaRDD<String> lines = sc.textFile("C:/mySoft/spark-2.3.0-bin-hadoop2.7/README.md");
		JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
			public Integer call(String s) { return s.length(); }
		});
		
		
//		List<Integer> lineLengthList = lineLengths.collect();
//		for (Integer len : lineLengthList) {
//			System.out.println(len);
//		}
		
		lineLengths.foreach(x -> System.out.println(x));
		
		int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) { return a + b; }
		});
		
		System.out.println("README.md total length: " + totalLength);
		
		
		sc.close();
		sc.stop();
	}
}
