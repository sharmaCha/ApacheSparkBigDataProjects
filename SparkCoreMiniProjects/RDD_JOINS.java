import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

/**
 * 
  *
 */
public class RDD_JOINS {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf conf = new SparkConf().setAppName("RDD_JOINS").setMaster("local[2]");
		
		System.setProperty("hadoop.home.dir", "C:/hadoop-2.3.0");
		System.setProperty("spark.hadoop.mapred.output.compress", "true");
		System.setProperty("spark.hadoop.mapred.output.compression.codec", "true");
		System.setProperty("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		System.setProperty("spark.hadoop.mapred.output.compression.type", "BLOCK");
		

		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer, Integer>> kv1 = new ArrayList();

		List<Tuple2<Integer, Integer>> kv2 = new ArrayList();

		kv1.add(new Tuple2(1, 2));
		kv1.add(new Tuple2(3, 4));
		kv1.add(new Tuple2(3, 6));

		kv2.add(new Tuple2(3, 9));

		JavaRDD<Tuple2<Integer, Integer>> kv_rdd = sc.parallelize(kv1);
		JavaRDD<Tuple2<Integer, Integer>> kv_rdd_2 = sc.parallelize(kv2);

		JavaPairRDD<Integer, Integer> pair_rdd1 = JavaPairRDD.fromJavaRDD(kv_rdd);

		JavaPairRDD<Integer, Integer> pair_rdd2 = JavaPairRDD.fromJavaRDD(kv_rdd_2);

		//JavaPairRDD<Integer, Tuple2<Integer, Integer>> join_rdd = pair_rdd1.join(pair_rdd2);
 
		//join_rdd.foreach(x -> System.out.println(x));
		
	//	JavaPairRDD<Integer,Integer> sub_by_key_rdd = pair_rdd1.subtractByKey(pair_rdd2);
		
	//	sub_by_key_rdd.foreach(x -> System.out.println(x));
		
		
	//	JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> right_outer_join_rdd = pair_rdd1.rightOuterJoin(pair_rdd2);
		
		JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> left_outer_join_rdd = pair_rdd1.leftOuterJoin(pair_rdd2);
		
	//	right_outer_join_rdd.foreach(x -> System.out.println(x));
		
	//	System.out.println("--------------------------------------------");
	//	left_outer_join_rdd.foreach(x->System.out.println(x));

	//	left_outer_join_rdd.sa
		left_outer_join_rdd.saveAsTextFile("out_zip");
		
	}

}
