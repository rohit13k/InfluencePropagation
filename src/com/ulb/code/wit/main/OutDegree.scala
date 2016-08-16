package com.ulb.code.wit.main

import org.apache.spark._
import scala.collection.mutable.HashMap

import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import scala.collection.mutable.HashSet
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
/**
 * @author Rohit
 */
object OutDegree {
  val folder = "C://Users//Rohit//Google Drive//testdata//"
  
   val filelist = Array("slashdot-threads", "facebook-wosn-wall", "higgs-activity_time", "twitter_Punjab_10-12", "twitter_rio2016_12", "dblp_coauthor", "enron", "lkml-reply")
    
  val minPartitions = 4
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("OutDegree").setMaster("local[*]")
    val sc = new SparkContext(conf)
    for (f <- filelist) {
      val path = folder + "groundtruth//" + f + "_outDegree.csv"
      val seeds = folder + "groundtruth//" + f + "_outdegree_50.txt"

      
      val input = sc.textFile(path, minPartitions).map(x => x.split(",")).map(x=>(x(0),x(1).toDouble))
      
      val result = input.takeOrdered(50)(Ordering[Double].reverse.on(x => x._2))
     // println(result.mkString("\n"))

      val fil = new File(seeds)

      val bw = new BufferedWriter(new FileWriter(fil))
      for (line <- result) {

        bw.write(line._1 + "\n")

      }
      bw.close
      println("done " + f)
    }
  }
   def outDegree() {

   for (file <- filelist) {
      var retweet: HashMap[String, HashSet[String]] = HashMap.empty

      for (line <- Source.fromFile(folder + "//input//" + file + ".txt").getLines()) {
        val temp = line.split(",");

        retweet.update(temp(0), retweet.getOrElse(temp(0), HashSet.empty).+=(temp(1)))

      }

      val f = new File(folder + "//groundtruth//" + file + "_outDegree.csv")
      val bw = new BufferedWriter(new FileWriter(f))
      val k = retweet.iterator

      while (k.hasNext) {
        val temp = k.next()
        bw.write(temp._1 + "," + temp._2.size + "\n")
      }

      bw.close
      println("done " + file)
    }
  }
}