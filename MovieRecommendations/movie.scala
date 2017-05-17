import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

object MovieLensALS {

def main(args: Array[String]) {

val conf = new SparkConf().setAppName("MovieLensALS").setMaster("local[2]")

val sc = new SparkContext(conf)

val rawData = sc.textFile("/home/ml-100k/u.data")

//rawData.first()

val rawRatings = rawData.map(_.split("\t").take(3))

val ratings = rawRatings.map {case Array(user,movie,rating) => Rating(user.toInt,movie.toInt,rating.toDouble)}

//ratings.first()

//train the data:-

val model = ALS.train(ratings, 50, 5, 0.01)

model.userFeatures

model.userFeatures.count

model.productFeatures.count

val predictedRating = model.predict(789, 123)


val userId = 789
val K = 10
val topKRecs = model.recommendProducts(userId, K)

println(topKRecs.mkString("\n"))


val movies = sc.textFile("/home/ml-100k/u.item")
val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()
titles(123)

val moviesForUser = ratings.keyBy(_.user).lookup(789)

println(moviesForUser.size)

moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)

}

}
