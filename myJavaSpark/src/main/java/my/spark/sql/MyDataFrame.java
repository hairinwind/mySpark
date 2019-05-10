package my.spark.sql;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MyDataFrame {
	public static void main(String[] args) throws AnalysisException {
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .getOrCreate();
		
		Dataset<Row> df = spark.read().json("src/main/resources/tails.json");
		
		df.show();
		
		System.out.println("... display schema ... ");
		df.printSchema();
		
		System.out.println("... display airline and model ... ");
		df.select(col("currentOperatorCode"), col("model")).show();
		
		System.out.println("... display BEJ tails ... ");
		df.filter(col("currentOperatorCode").equalTo("BEJ")).show();
		
		// Register the DataFrame as a SQL temporary view
		df.createOrReplaceTempView("people");

		System.out.print("... sql get all ...");
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();
		
//		System.out.println("... temp view does not exist...");
//		spark.newSession().sql("SELECT * FROM people").show();
		
		// sql on globalTempView
		df.createGlobalTempView("airlines");
		
		System.out.print("... sql distinct ..."); 
		spark.newSession().sql("SELECT distinct currentOperatorCode FROM global_temp.airlines").show();
		
		
	}
}
