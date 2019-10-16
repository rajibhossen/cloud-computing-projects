import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KMeans {
  type Point = (Double,Double)

  var centroids: Array[Point] = Array[Point]()

  def main(args: Array[ String ]) {
    /* ... */

    centroids = /* read initial centroids from centroids.txt */

    for ( i <- 1 to 5 )
        centroids = /* find new centroids using KMeans */

    centroids.foreach(println)
  }
}
