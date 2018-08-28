package comp9313.lab8

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem5 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("SSSP").setMaster("local")
    val sc = new SparkContext(conf)
    
    val fileName = args(0)
    val k = args(1).toInt
    val edges = sc.textFile(fileName)   
   
    val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong, x(3).toDouble))	
    val graph = Graph.fromEdges[Double, Double](edgelist, 0.0)
	graph.triplets.collect().foreach(println)
    
    val initialGraph = graph.mapVertices((id, _) => Set[VertexId]())
    val res = initialGraph.pregel(Set[VertexId](), k)(
      (id, ns, newns) => ns ++ newns, // Vertex Program
      triplet => {  // Send Message
        Iterator((triplet.dstId, triplet.srcAttr + triplet.srcId))
      },
      (a, b) => a++b // Merge Message
    )
       
    println(res.vertices.filter{case(id, attr) => attr.contains(id)}.collect().mkString("\n"))
  }
}