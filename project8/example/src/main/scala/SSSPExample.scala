/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


/**
 * An example use the Pregel operator to express computation
 * such as single source shortest path
 */
object Partition{
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0)).map(line=>line.split(",").map(line=>line.toLong))
      .map{case Array(a,z @ _*)=>(z.map(f=>(a,f) ))}
    val dataflaten = data.flatMap(r=>r)
    val edgesRDD : RDD[Edge[Any]] = dataflaten.map(f=>Edge(f._1,f._2,""))
    val g = GraphX.fromEdges(edgesRDD, defaultValue = "")

    val initialGraph = g.mapVertices{case(vid,attr)=>  if (vid > 5) attr== -1}

    val graphcc = initialGraph.pregel(Long.MaxValue,5)(
      (id, attr, msg) => math.max(attr, msg), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr == -1) {
          Iterator((triplet.dstId, triplet.srcAttr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.max(a, b) // Merge Message
    )
    val groupCount = graphcc.vertices.map(f => (f._2, 1)).reduceByKey((x, y) => (x + y)).sortByKey().map(f => f._1 + " " + f._2)
    val output = groupCount.collect()
    output.foreach(println)

  }
}

