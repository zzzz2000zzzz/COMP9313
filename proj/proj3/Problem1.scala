package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem1{
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    //create a scala spark context
    val sc = new SparkContext(conf)
    //load our input data
    val input = sc.textFile(inputFile)
    //Split by lines
    val lines = input.flatMap(_.split("[\n]+")) 
    //Split by blank   
    //(0, 10.0)
    val rdd=lines.map(x =>(x.split(" ")(1),x.split(" ")(3)toDouble))
    //(0,(10.0,1))
    val r2 =rdd.mapValues ( x => (x,1) )
    //(0,(15.0,2))
    val r3 =r2.reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
    //(0,7.5)
    val result = r3.mapValues(x => x._1 / x._2).sortByKey(true).map(a=>a._1+"\t"+a._2)

    

    //result.foreach(println)
    result.saveAsTextFile(outputFolder)
  }
}