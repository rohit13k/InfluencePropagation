package com.ulb.code.wit.main
import scala.io.Source
import collection.mutable.HashMap
import scala.collection.mutable.Stack
import java.util.Date
import ds.src.ModifiedHLL
import ds.src.HyperLogLog

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.collection.mutable.SetBuilder
import java.io.{ File, FileWriter, BufferedWriter }
import scala.collection.mutable.StringBuilder
/**
 * @author Rohit
 */
class InfluenceSetApproxReverse(windowpercent: Double, num_of_buckets: Int, datafile: String) {
  //  val folder = "C://phd//test data//"
  //  val outfile = folder + "higgs-activity"
  var nodesummary: scala.collection.Map[Int, HyperLogLog] = Map.empty

  def compute(sb: StringBuilder) {
    nodesummary = Map.empty
    var nodes = new HashMap[Int, ModifiedHLL]()
    var edges = new Stack[(Int, Int, Long)]
    var line = ""
    var dstart = 0l
    var dend = 0l
    var count = 0
    var temp: Array[String] = Array.empty
    var t1 = new Date().getTime

    for (line <- Source.fromFile(datafile).getLines()) {
      //      val temp = line.split(" ")

      temp = line.split(",")
      //      if (temp.length == 4 & !temp(3).equals("RE")) {
      if (count == 0) {
        dstart = temp(2).toLong
      }
      dend = temp(2).toLong
      edges.push((temp(1).toInt, temp(0).toInt, temp(2).toLong))
      nodes.+=((temp(0).toInt, new ModifiedHLL(num_of_buckets)))
      nodes.+=((temp(1).toInt, new ModifiedHLL(num_of_buckets)))
      count = count + 1
    }
    val window = (dend - dstart) * windowpercent / 100
    //    System.gc()
    println("window : " + window)
    println(dend + "::" + dstart)
    var t2 = new Date().getTime
    var time_add = 0l
    var time_union = 0l
    println("time to read:" + (t2 - t1))
    println("#nodes : " + nodes.size)
    println("#edges : " + edges.size)
    //println("window : " + window)
    var edgelength = edges.size
    count = 0
    //   println("Started: " + new Date)
    val starttime = new Date().getTime
    var elem = (0, 0, 0l)
    var S_u: ModifiedHLL = null
    var S_v: ModifiedHLL = null
    while (edgelength > 0) {
      elem = edges.pop()
      edgelength = edgelength - 1
      S_u = nodes.getOrElse(elem._1, null)
      S_v = nodes.getOrElse(elem._2, null)
      if (S_u == null || S_v == null) {
        print("Something wrong!! " + elem)
      } else {
        t1 = new Date().getTime
        S_u.add(elem._2, elem._3)
        t2 = new Date().getTime
        time_add = time_add + (t2 - t1)
        S_u.union(S_v.buckets, elem._3, window)
        t1 = new Date().getTime
        time_union = time_union + (t1 - t2)
        nodes.update(elem._1, S_u)
      }
      count += 1
      //      if (count % 100000 == 0) {
      //        print("add : " + time_add + " union: " + time_union)
      //        println(" done " + count + " edges left " + edges.size + " at " + new Date)
      //        time_add = 0l
      //        time_union = 0l;
      //      }
    }
    nodesummary = nodes.map(f => {

      (f._1, f._2.convertToHLL())
    })
    val endtime = new Date().getTime
    println("Time: " + (endtime - starttime))
    sb.append("Time: " + (endtime - starttime) + "\n")
    nodes.clear()
    edges.clear()
    //    System.gc()
    var finalfreeMemory = Runtime.getRuntime.freeMemory()

    println("Memory: " + (Runtime.getRuntime.totalMemory() - finalfreeMemory) / (1024 * 1024))
    //    val fapprox = new File(outfile + "_approxmhll.csv")
    //
    //    val bwapprox = new BufferedWriter(new FileWriter(fapprox))
    //
    //    nodes.foreach(x => {
    //      bwapprox.write(x._1 + "," + x._2.estimate() + "\n")
    //    })
    //    bwapprox.close()
  }

  def findInflunce(seednode: Array[Int]): Int = {
    var is = nodesummary.getOrElse(seednode(0), new HyperLogLog(num_of_buckets))
    for (i <- 1 to seednode.length - 1) {
      is.union(nodesummary.getOrElse(seednode(i), new HyperLogLog(num_of_buckets)).buckets())
    }

    is.estimate().toInt
  }
}