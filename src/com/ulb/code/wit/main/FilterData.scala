package com.ulb.code.wit.main

import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

/**
 * @author Rohit
 */
object FilterData {
    
  val folder = "C:\\phd\\testdata\\twitterdata\\"
  def main(args: Array[String]): Unit =
    {
      //findCommonKeys(folder + "result\\higgs-activity_Retweet_RetweetCount_100.txt", folder + "result\\higgs-activity_Retweet_1_100.keys", 100)
    //  outDegree
    converttime()
    }
  def fillter() {

    val file = "higgs-activity_time.txt"
    val f = new File(folder + "higgs-activity_Retweet.txt")

    val bw = new BufferedWriter(new FileWriter(f))
    for (line <- Source.fromFile(folder + file).getLines()) {
      val temp = line.split(" ");
      if (temp(3).equals("RT")) {
        bw.write(temp(1) + " " + temp(0) + " " + temp(2) + "\n")
      }

    }
    bw.close
    println("done")
  }
  def outDegree() {

    val file = "slashdot-threads.txt"

    var retweet: HashMap[String, HashSet[String]] = HashMap.empty

    for (line <- Source.fromFile(folder + file).getLines()) {
      val temp = line.split(" ");

      retweet.update(temp(0), retweet.getOrElse(temp(0), HashSet.empty).+=(temp(1)))

    }

    val f = new File(folder + "slashdot-threads_outDegree.csv")
    val bw = new BufferedWriter(new FileWriter(f))
    val k = retweet.iterator
    while (k.hasNext) {
      val temp = k.next()
      bw.write(temp._1 + "," + temp._2.size + "\n")
    }

    bw.close
    println("done")
  }
  def converttime() {
    val file = "twitter_Punjab13-14"
    val f = new File(folder + file + ".txt")

    val bw = new BufferedWriter(new FileWriter(f))
    for (line <- Source.fromFile(folder + file + ".csv").getLines()) {
      val temp = line.split(",");
      
      bw.write(temp(0) + " " + temp(1) + " " + temp(2).toLong / 1000 + "\n")

    }
    bw.close
    println("done")
  }
  def findCommonKeys(file1: String, file2: String, count: Int) {
    var k1: HashSet[String] = HashSet.empty
    var k2: HashSet[String] = HashSet.empty
    var temp = 0
    for (line <- Source.fromFile(file1).getLines()) {
      if (temp < count)
        k1.add(line)
      temp = temp + 1
    }
    temp = 0
    for (line <- Source.fromFile(file2).getLines()) {
      if (temp < count)
        k2.add(line)
      temp = temp + 1
    }
    println(k1.intersect(k2).size)

  }
}