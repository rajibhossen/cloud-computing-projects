import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Partition{
  var cluster = 0
  val depth = 12

  def readData(input: Array[String]):(Long, Long, List[Long])={
    var cid = -1.toLong
    if (cluster < 10){
      cid = input(0).toLong
      cluster += 1
    }
    (input(0).toLong, cid, input.tail.map(_.toString.toLong).toList)
  }

  def leftOrRightFunc(line: (Long, Long, List[Long])): List[(Long, Either[(Long, List[Long]), Long])] ={
    var temporary = List[(Long, Either[(Long, List[Long]), Long])]()
    if (line._2 > 0){
      line._3.foreach(adjacent => {temporary = (adjacent, Right(line._2))::temporary})
    }
    temporary = (line._1, Left(line._2, line._3))::temporary
    temporary
  }

  def reduceFunc(tuple: (Long, Iterable[Either[(Long, List[Long]), Long]])): (Long, Long, List[Long]) = {
    var adjacent = List[Long]()
    var cid = -1.toLong
    val id = tuple._1
    for (adjItem <- tuple._2)
      adjItem match {
        case Right(c) => cid = c
        case Left((c, adj)) if c > 0 => return (id, c, adj)
        case Left((-1, adj)) => adjacent = adj
      }
    (id, cid, adjacent)
  }

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("GraphPartition")
    val sparkContext = new SparkContext(conf)

    var graph = sparkContext.textFile(args(0)).map(line => {readData(line.split(","))})

    for (_ <- 1 to depth){
      var flatOutPut = graph.flatMap(line => {leftOrRightFunc(line)})
      var grpKey = flatOutPut.groupByKey
      graph = grpKey.map(tuple => reduceFunc(tuple))
    }
    val aggregate = graph.map({ row => (row._2, 1) })
    val count = aggregate.reduceByKey(_ + _)
    count.collect().foreach(println)

    sparkContext.stop()

  }
}
