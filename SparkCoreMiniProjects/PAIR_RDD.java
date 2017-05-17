import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 
 */

/**
 *
 */
public class PAIR_RDD {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf conf = new SparkConf().setAppName("PAIR_RDD").setMaster("local[2]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines_rdd = sc.textFile("C:/spark-1.6.1-bin-hadoop2.6/README.md"); 
		
		JavaPairRDD<String,Integer> lines_pair_rdd = lines_rdd.mapToPair(x -> new Tuple2<String, Integer>(x.split(" ")[0],x.length()));
		
		lines_pair_rdd.groupByKey().coalesce(1).saveAsTextFile("GROUP_BY_KEY_OUTPUT");
		
	//	lines_pair_rdd.coalesce(1).saveAsTextFile("PAIR_RDD_OUTPUT_KEY_AND_LENGTH");
		
	//	JavaPairRDD<String,Integer> reduce_by_key_rdd = lines_pair_rdd.reduceByKey( (a,b) -> a+b).sortByKey(false);
		
	//	reduce_by_key_rdd.coalesce(1).saveAsTextFile("PAIR_RDD_REDUCE_BY_KEY_OUTPUT");
		
	//	reduce_by_key_rdd.values().foreach(x -> System.out.println(x));
		
	}

}
