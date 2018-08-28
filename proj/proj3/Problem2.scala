package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Level, Logger }  
import scala.reflect.classTag  


object Problem2{
    def main(args: Array[String]) {
    val inputFile = args(0)
    val node = args(1).toString
    
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    //create a scala spark context
    val sc = new SparkContext(conf)
    //load our input data
    val vertexLines: RDD[String] = sc.textFile(inputFile)

    val vertices: RDD[(VertexId, Long)] = vertexLines.map(line => {  
                val cols = line.split(" ")  
                (cols(1).toLong, 0.toLong)  
            })   
     val edgeLines: RDD[String] = sc.textFile(inputFile)
     val edges:RDD[Edge[(Long)]] = edgeLines.map(line => {  
                val cols = line.split(" ")  
                Edge(cols(1).toLong, cols(2).toLong)  
            })  
     val graph:Graph[Long, Long] = Graph(vertices, edges)  
 

    val sourceId: VertexId = args(1).toLong

    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 1 else 0)  
    val sssp = initialGraph.pregel(0,vertices.count().toInt)(  
      (id, sign, newSign) => math.max(sign, newSign),
      triplet => {
        if (triplet.srcAttr==1) {  
          
          Iterator((triplet.dstId, 1))  
        } else {  
          Iterator.empty  
        }  
      },  
      (a, b) => math.max(a, b)  
      )
    val filteredVertices= sssp.vertices.filter{ case (vid:VertexId,attr:Int) => attr > 0 }
    
    var total = filteredVertices.collect().length
   
    if(total>0){
      println(total-1)
    }else{
      println(0)
    }
    
    }


}







