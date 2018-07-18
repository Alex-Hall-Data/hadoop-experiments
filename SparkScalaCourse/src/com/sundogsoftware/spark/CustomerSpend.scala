package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CustomerSpend  {
  
  def main(args: Array[String]) {
    
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")
    
    // Read each line of input data
    val orderRDD = sc.textFile("../customer-orders.csv")
   
    val splitLines = orderRDD.map(x => x.split(","))
    
    val parsedLines = splitLines.map(x => (x(0).toInt , x(2).toFloat))
    
    val spendByCustomer = parsedLines.reduceByKey((v1,v2) => v1 + v2 )
    
    val spendSorted = spendByCustomer.map( x => (x._2 , x._1)).sortByKey().collect()
    
    
    spendSorted.foreach(println)
  }

  
}