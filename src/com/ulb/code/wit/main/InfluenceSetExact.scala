package com.ulb.code.wit.main
import scala.io.Source
import collection.mutable.HashMap
import scala.collection.mutable.Stack
import java.util.Date
import java.io.{ File, FileWriter, BufferedWriter }
import ds.src.ModifiedHLL
import ds.src.HyperLogLog
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.collection.mutable.StringBuilder

/**
 * @author Rohit
 */
class InfluenceSetExact(windowpercent: Double, datafile: String) {

  var nodesummary: scala.collection.Map[Int, scala.collection.Set[Int]] = Map.empty
  def compute(sb: StringBuilder) {
    nodesummary = Map.empty
    var line = ""
    var nodes = new HashMap[Int, HashMap[Int, Long]]()
    var dstart = 0l
    var dend = 0l
    var count = 0
    var edges = new Stack[(Int, Int, Long)]

    for (line <- Source.fromFile(datafile).getLines()) {

      val temp = line.split(",")

      if (count == 0) {
        dstart = temp(2).toLong
      }
      dend = temp(2).toLong
      edges.push((temp(0).toInt, temp(1).toInt, temp(2).toLong))
      nodes.+=((temp(0).toInt, new HashMap))
      nodes.+=((temp(1).toInt, new HashMap))
      count = count + 1

    }
    val window = (dend - dstart) * windowpercent / 100

    val starttime = new Date().getTime

    var edgelength = edges.size
    count = 0

    var elem = (0, 0, 0l)
    var S_u: HashMap[Int, Long] = null
    var S_v: HashMap[Int, Long] = null
    var timetomerge = 0l
    while (edgelength > 0) {

      elem = edges.pop()
      edgelength = edgelength - 1
      S_u = nodes.getOrElse(elem._1, null)
      S_v = nodes.getOrElse(elem._2, null)

      if (S_u == null || S_v == null) {
        print("Something wrong!! " + elem)
      } else {
        if (elem._1 != elem._2) {
          S_u = add(S_u, (elem._2, elem._3))
          val tstart = new Date().getTime
          S_u = merge(elem._1, S_u, S_v, elem._3, window)
          timetomerge = timetomerge + (new Date().getTime - tstart)
          nodes.update(elem._1, S_u)
        }
      }

    }

    nodesummary = nodes.mapValues(f => f.keySet)
    nodes = null
    edges = null
    val endtime = new Date().getTime
    println("Time to find influence : " + (endtime - starttime))
    sb.append("Timeto find influence : " + (endtime - starttime) + "\n")

    var finalfreeMemory = Runtime.getRuntime.freeMemory()

  }

  def findseeds(seeds: Int): Array[Int] = {

    var result: Array[Int] = new Array(seeds)
    var is: scala.collection.Set[Int] = Set.empty
    var sortedlist = nodesummary.toArray
    sortedlist = sortedlist.sortBy(f => { -1 * f._2.size })
    is = sortedlist(0)._2
    var lastposition = 0
    var temp = 0
    result(0) = sortedlist(0)._1
    var seed: scala.collection.Set[Int] = Set.empty
    var node: Int = 0
    var max = -1
    for (i <- 1 to seeds - 1) {
      seed = Set.empty
      node = 0
      max = -1
      breakable {
        for (j <- 0 to sortedlist.size - 1) {
          if (!result.contains(sortedlist(j)._1)) {
            if (max > sortedlist(j)._2.size) {
              break
            }

            temp = sortedlist(j)._2.diff(is).size

            if (temp > max) {
              node = sortedlist(j)._1
              seed = sortedlist(j)._2
              max = temp
              lastposition = j
            }
          }
        }
      }
      result(i) = node

      is = is.union(seed)

    }

    println("final influence " + is.size)

    result
  }
  def findInflunce(seednode: Array[Int]): Int = {
    var is = nodesummary.getOrElse(seednode(0), Set.empty)

    for (i <- 1 to seednode.length - 1) {
      is = is.union(nodesummary.getOrElse(seednode(i), Set.empty))
    }

    is.size
  }
  private def setToHLL(S_u: HashMap[Int, Long], bucket: Int): HyperLogLog = {
    var hll: HyperLogLog = new HyperLogLog(bucket)
    var kit = S_u.iterator
    while (kit.hasNext) {
      val temp = kit.next
      hll.add(temp._1)
    }
    hll
  }
  private def setTomHLL(S_u: HashMap[Int, Long], bucket: Int): ModifiedHLL = {
    var mhll: ModifiedHLL = new ModifiedHLL(bucket)
    var kit = S_u.iterator
    while (kit.hasNext) {
      val temp = kit.next
      mhll.add(temp._1)
    }
    mhll
  }
  private def add(S_u: HashMap[Int, Long], v: (Int, Long)): HashMap[Int, Long] = {
    if (S_u.contains(v._1)) {
      if (S_u.getOrElse(v._1, 0l) > v._2) {
        S_u.update(v._1, v._2)
      }
    } else {
      S_u.+=(v)
    }
    S_u
  }

  private def merge(sourcenode: Int, S_u: HashMap[Int, Long], S_v: HashMap[Int, Long], newTime: Long, window: Double): HashMap[Int, Long] = {
    val iterator = S_v.iterator
    var value: (Int, Long) = null
    while (iterator.hasNext) {
      value = iterator.next()
      if (value._1 != sourcenode) {
        if (window == 0 || (value._2 - newTime) <= window) {
          add(S_u, value)
        }
      }
    }
    S_u
  }
}