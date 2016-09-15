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
import java.util.Arrays.ArrayList
import scala.collection.mutable.ArrayLike
import scala.collection.mutable.ArrayBuilder
import scala.util.control.Breaks._
/**
 * @author Rohit
 */
object OutDegree {
  val folder = "/Users/rk/Desktop/testdata/"

 // val filelist = Array("slashdot-threads_training", "facebook-wosn-wall_training", "higgs-activity_time_training", "enron_training", "lkml-reply_training")
val filelist = Array("twitter_Punjab_10-12","twitter_rio2016_12")

  val minPartitions = 4
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    outDegree()
    println("done outdegree")
    smartoutDegree()
    println("done smartoutdegree")
    val conf = new SparkConf().setAppName("OutDegree").setMaster("local[*]")
    val sc = new SparkContext(conf)
    for (f <- filelist) {
      val path = folder + "groundtruth//" + f + "_outDegree.csv"
      val seeds = folder + "groundtruth//" + f + "_outdegree_50.txt"

      val input = sc.textFile(path, minPartitions).map(x => x.split(",")).map(x => (x(0), x(1).toDouble))

      val result = input.takeOrdered(50)(Ordering[Double].reverse.on(x => x._2))
      // println(result.mkString("\n"))

      val fil = new File(seeds)

      val bw = new BufferedWriter(new FileWriter(fil))
      for (line <- result) {

        bw.write(line._1 + "\n")

      }
      bw.close

    }
  }
  def outDegree() {

    for (file <- filelist) {
      println(file)
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
  def smartoutDegree() {
    val seedsize = 50
    for (file <- filelist) {
      println(file)
      var retweet: HashMap[String, HashSet[String]] = HashMap.empty

      for (line <- Source.fromFile(folder + "//input//" + file + ".txt").getLines()) {
        val temp = line.split(",");

        retweet.update(temp(0), retweet.getOrElse(temp(0), HashSet.empty).+=(temp(1)))

      }

      var data: ArrayBuilder[(Long, Long)] = ArrayBuilder.make()
      val f = new File(folder + "//groundtruth//" + file + "_smartOutDegree_50.csv")
      val bw = new BufferedWriter(new FileWriter(f))
      val k = retweet.iterator

      while (k.hasNext) {
        val temp = k.next()
        data = data.+=((temp._1.toLong, temp._2.size))
      }
      val result = data.result().sortBy(x => x._2)
      var selectedseed: HashSet[Long] = HashSet.empty
      var coverednodes: HashSet[String] = HashSet.empty
      selectedseed.add(result(0)._1)
      coverednodes = retweet(result(0)._1 + "")
      var maxincrease = Int.MinValue
      var seedcandidate = 0l
      var tempcoverednodes: HashSet[String] = HashSet.empty
      for (i <- 1 to seedsize - 1) {
        maxincrease = Int.MinValue
        seedcandidate = 0l
        for (j <- 1 to result.length - 1) {
          if (!selectedseed.contains(result(j)._1)) {
            tempcoverednodes = coverednodes.++(retweet(result(j)._1 + ""))
            if (tempcoverednodes.size - coverednodes.size > maxincrease) {
              seedcandidate = result(j)._1
              maxincrease = tempcoverednodes.size - coverednodes.size
            }
            if (result(j)._2 < maxincrease) {
              break()
            }

          }
        }
        print(i + ",")
        selectedseed.add(seedcandidate)
        coverednodes = coverednodes.++(retweet(seedcandidate + ""))
      }
      val iterat = selectedseed.iterator
      while (iterat.hasNext) {
        bw.write(iterat.next() + "\n")
      }
      bw.close
      println("done " + file)
    }
  }
}