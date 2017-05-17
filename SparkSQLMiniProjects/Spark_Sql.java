/**
 * 
 */

/**
 *
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.functions;

public class Spark_Sql {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.setProperty("hadoop.home.dir", "C:/hadoop-2.3.0");

		SparkConf conf = new SparkConf().setAppName("SQL").setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(ctx);

		SparkSession spark = SparkSession.builder().config("log4j.rootCategory", "WARN")

				.getOrCreate();

		Dataset<Row> df = spark.read().option("header", "true").csv(
				"C:/hadoop_and_spark/HDPCD-Certification-master/datasets/flightdelays/flight_delays1.csv");

		// df.printSchema();

		// Displays the content of the DataFrame to stdout
		// df.show();

		// Dataset<Row> test =
		// sqlContext.jsonFile("C:/Users/Desktop/sfo_weather.csv");
		//
		// test.toJavaRDD().foreach(x -> System.out.println(x));

		// df.groupBy("Month").count().show();

		// df.createOrReplaceTempView("flight_delays");

		// Dataset<Row> fd_DF = spark.sql("select * from flight_delays limit
		// 10");

		df.filter(df.col("ArrDelay").between(100, 200)).show();

		// df.groupBy("department").agg(max(df.col("age")),
		// sum(df.col("expense")));

		// fd_DF.show();

		
		
		
	}
}
