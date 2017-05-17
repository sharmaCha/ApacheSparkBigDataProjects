import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 */

/**
 * 
 *
 */
public class Closures_In_Spark {

	static int counter = 0;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf conf = new SparkConf().setAppName("Closures").setMaster("local[2]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1, 5, 7, 8));

		listRDD.collect().forEach(x -> System.out.println(x));
		
		sc.close();

	}

}
