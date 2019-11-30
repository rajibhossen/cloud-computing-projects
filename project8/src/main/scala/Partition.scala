import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Partition {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Cluster")
    val sc = new SparkContext(conf)

    val edges: RDD[Edge[Long]] = sc.textFile(args(0)).map(line => {val (vertex, adjacent) = line.split(",").splitAt(1)
      (vertex(0).toLong, adjacent.toList.map(_.toLong))})
      .flatMap(x => x._2.map(y => (x._1, y)))
      .map(nodes => Edge(nodes._1, nodes._2, nodes._1))

    val graph : Graph[Long, Long] = Graph.fromEdges(edges, "defaultProperty")
      .mapVertices((id,_) => id)

    graph.edges.collect.foreach(println)

    val graph_pregel = graph.pregel(Long.MaxValue, 5)(
      (id, attr, msg) => math.max(attr, msg),
      triplet => {
        if (triplet.srcAttr == -1){
          Iterator((triplet.dstId, triplet.srcAttr))
        }
        else {
          Iterator.empty
        }
      }, (a, b) => math.max(a, b)
    )

    val result = graph_pregel.vertices.map(graphnew => (graphnew._2, 1)).reduceByKey(_+_).sortByKey().collect()
      .map(k => k._1.toString +"" + k._2.toString)
    result.foreach(println)
  }
}
