package com.ulb.code.wit.main

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import java.util.Date
/**
 * @author Rohit
 */
object Rank {
  val folder = "D://phd//testdata//"
  val file = Array("lkml-reply_training", "higgs-activity_time_training", "slashdot-threads_training", "facebook-wosn-wall_training", "enron_training")
  val minPartitions = 4
  def main(args: Array[String]) {
    var starttime = new Date().getTime
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Pagerank").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val time = folder + "groundtruth//pageRankTime.txt"
    val bwresult = new BufferedWriter(new FileWriter(new File(time)))
    for (f <- file) {
      starttime = new Date().getTime
      val path = folder + "input//Archive//" + f + ".txt"
      val seeds = folder + "groundtruth//final//" + f + "_pagerank_reversed_50.txt"

      val relationships: RDD[Edge[Boolean]] =
        sc.parallelize(Array(Edge(1L, 2L, true), Edge(1L, 4L, true),
          Edge(2L, 4L, true), Edge(3L, 1L, true),
          Edge(3L, 4L, true)))
      // Create an RDD for edges
      val input = sc.textFile(path, minPartitions).map(x => x.split(",")).map(x => Edge(x(1).toLong, x(0).toLong, x(2).toLong))
      val defaultVertex = (0L, 0L)

      // Create the graph
      val graph = Graph.fromEdges(input, defaultVertex, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_ONLY);
      val node = graph.pageRank(0.0001).vertices
      val result = node.distinct().takeOrdered(50)(Ordering[Double].reverse.on(x => x._2))
      // println(result.mkString("\n"))

      val fil = new File(seeds)

      val bw = new BufferedWriter(new FileWriter(fil))
      for (line <- result) {

        bw.write(line._1 + "\n")

      }
      bw.close
      bwresult.write("done " + f + ": " + (new Date().getTime - starttime) + "\n")
      println("done " + f + ": " + (new Date().getTime - starttime))
    }
    bwresult.close()
  }
}