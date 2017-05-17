import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/**
 * 
 */
 *
 */
public class PAIR_RDD_2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf conf = new SparkConf().setAppName("PAIR_RDD_2").setMaster("local[2]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>();
		hm.put(1, 1);

		List<Tuple2<Integer, Integer>> l = Arrays.asList(new Tuple2(1, 2), new Tuple2(3, 4),new Tuple2(3,6));

		JavaRDD<Tuple2<Integer, Integer>> li = sc.parallelize(l);

		// li.foreach(x -> System.out.println(x));

		JavaPairRDD<Integer, Integer> pair_rdd = JavaPairRDD.fromJavaRDD(li);

		//	pair_rdd.foreach(x -> System.out.println(x));
		
		JavaPairRDD<Integer,Integer> flat_map_values_rdd = pair_rdd.mapValues(x -> x+5);
		
		//flat_map_values_rdd.foreach(c -> System.out.println(c));
		
		JavaPairRDD<Integer,Integer> flat_map_values_rdd2 = pair_rdd.flatMapValues(new Function<Integer, Iterable<Integer>>() {

			@Override
			public Iterable<Integer> call(Integer arg0) throws Exception {
				// TODO Auto-generated method stub
				
				
				return Arrays.asList(arg0);
			}
		});
		
		flat_map_values_rdd2.foreach(c -> System.out.println(c));
		

	}

}
