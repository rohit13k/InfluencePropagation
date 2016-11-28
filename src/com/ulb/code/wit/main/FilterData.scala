package com.ulb.code.wit.main

import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer

/**
 * @author Rohit
 */
object FilterData {

  val folder = "D://phd//testdata//"
  val outfolder = "D://phd//testdata//inputC//"
  def main(args: Array[String]): Unit =
    {
      //findCommonKeys(folder + "result\\higgs-activity_Retweet_RetweetCount_100.txt", folder + "result\\higgs-activity_Retweet_1_100.keys", 100)
      //  outDegree
      // converttime()
      //      val filelist = Array("higgs-activity_time","slashdot-threads","dblp_coauthor","enron","facebook-wosn-wall","lkml-reply")
      //      for (file <- filelist) {
      //        dividefile(file)
      //      }
      //      convertForC("twitter_uselection_mentionincluded_3.csv")
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
  def convertForC(file: String) {

    val f = new File(outfolder + file)
    val data: ListBuffer[(String, String, String)] = ListBuffer.empty
    val bw = new BufferedWriter(new FileWriter(f))
    for (line <- Source.fromFile(folder + file).getLines()) {
      val temp = line.split(",");

      data.+=((temp(0), temp(1), temp(2)))
    }
    val sorteddata = data.sortBy(f => {
      f._3
    })
    for ((a, b, t) <- sorteddata) {
      bw.write(a + " " + b + " " + " " + t + "\n")
    }
    bw.close
    println("done")
  }
  def converttime() {
    val file = "twitter_uselection_mentionincluded_3"
    val f = new File(folder + "inputC//" + file + ".txt")

    val bw = new BufferedWriter(new FileWriter(f))
    for (line <- Source.fromFile(folder + "inputC//" + file + ".csv").getLines()) {
      val temp = line.split(" ");

      bw.write(temp(0) + " " + temp(1) + " " + temp(3).trim().toLong / 1000 + "\n")

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

  def dividefile(file: String) {

    val ftraining = new File(folder + file + "_training.txt")
    val ftest = new File(folder + file + "_test.txt")
    val bw = new BufferedWriter(new FileWriter(ftraining))
    val bwtest = new BufferedWriter(new FileWriter(ftest))
    var count = 0
    for (line <- Source.fromFile(folder + file + ".txt").getLines()) {
      count = count + 1

    }
    val trainingsize: Int = count * 8 / 10
    count = 0
    for (line <- Source.fromFile(folder + file + ".txt").getLines()) {

      count = count + 1
      if (count < trainingsize)
        bw.write(line + "\n")
      else {
        bwtest.write(line + "\n")
      }

    }
    bw.close
    bwtest.close
    println("done")
  }
}