package com.ulb.code.wit.main

import scala.io.Source
import scala.collection.mutable.HashSet
import java.io.FileWriter
import java.io.File
import java.io.BufferedWriter
import scala.util.Random
import scala.collection.mutable.HashMap
import java.io.{ ObjectOutputStream, ObjectInputStream }
import java.io.{ FileOutputStream, FileInputStream }
import sys.process._
import java.util.Date

object Test {

  def main(args: Array[String]): Unit =
    {
      var seed = 50
      var file = "twitter_Punjab13-14"
      var folder = "C:\\phd\\testdata\\"
      var iFile = folder + "input\\" + file + ".txt"
      var oFile = folder + "metis\\" + file + ".gr"
      var mappingFile = folder + "metis\\" + file + ".dat"
      var resultFile = folder + "metis\\" + file + ".imstats"
      var keyFile = folder + "metis\\keys\\" + file + "_" + seed + ".keys"
      createDIMACSGraph(iFile, oFile, mappingFile)
      //      println("dimacs graph created")
       runSKIM(oFile, resultFile, seed)
       regenerateKeyIds(mappingFile, resultFile, keyFile)
      //      createStaticFile(iFile, oFile)

      //      createStaticFilewithRandomProbability(iFile, oFile, seed)
    }
  def runSKIM(oFile: String, resultFile: String, seed: Int) {
    val result = "/Users/rk/Documents/phd/code/ms-skim/bin/RunSKIM -i " + oFile + " -type dimacs -k 64 -l 64 -N " + seed + " -leval 512 -oc " + resultFile !
  }
  def createDIMACSGraph(iFile: String, oFile: String, mappingFile: String) {
    var line: Array[String] = Array.empty
    var input = Source.fromFile(iFile).getLines()
    var node = new HashSet[Int]

    var edgelist = new HashSet[(Int, Int)]
    while (input.hasNext) {
      line = input.next().split(",")
      node.add(line(0).toInt)
      node.add(line(1).toInt)
      edgelist.add(line(0).toInt, line(1).toInt)

    }

    var nodeArray = node.toList.sortBy(x => x)

    var edge = edgelist.toList.map(x => {

      (nodeArray.indexOf(x._1) + 1, nodeArray.indexOf(x._2) + 1)
    }).sortBy(r => (r._1, r._2))

    var fw = new FileWriter(new File(oFile))
    var bw = new BufferedWriter(fw)
    bw.write("p sp " + node.size + " " + edge.size + "\n")
    var it = edge.iterator
    while (it.hasNext) {
      val elem = it.next()
      bw.write("a " + elem._1 + " " + elem._2 + "\n")
    }
    bw.flush()
    bw.close()
    val os = new ObjectOutputStream(new FileOutputStream(mappingFile))
    os.writeObject(nodeArray)
    os.close()

  }
  def regenerateKeyIds(mappingFile: String, result: String, keyFile: String) {
    val is = new ObjectInputStream(new FileInputStream(mappingFile))
    val obj = is.readObject().asInstanceOf[List[Int]]
    is.close()
    var input = Source.fromFile(result).getLines()
    //skipping 3 lines
    input.next
    input.next
    input.next
    var fw = new FileWriter(new File(keyFile))
    var bw = new BufferedWriter(fw)
    while (input.hasNext) {
      var line = input.next().split("\t")
      bw.write(obj(line(0).toInt + 1) + "\n")
    }
    bw.flush
    bw.close
  }

  def createStaticFile(iFile: String, oFile: String) {
    var line: Array[String] = Array.empty
    var input = Source.fromFile(iFile).getLines()
    var node = new HashSet[String]

    var edgelist = new HashSet[String]
    while (input.hasNext) {
      line = input.next().split(",")
      edgelist.add(line(0) + " " + line(1))
      node.add(line(0))
      node.add(line(1))

    }
    var fw = new FileWriter(new File(oFile))
    var bw = new BufferedWriter(fw)
    bw.write(node.size + " " + edgelist.size + "\n")
    var it = edgelist.iterator
    while (it.hasNext) {

      bw.write(it.next + "\n")
    }
    bw.flush()
    bw.close()

  }
  def createStaticFilewithRandomProbability(iFile: String, oFile: String, seed: Long) {
    var line: Array[String] = Array.empty
    var input = Source.fromFile(iFile).getLines()
    var node = new HashSet[String]

    var edgelist = new HashSet[String]
    while (input.hasNext) {
      line = input.next().split(",")
      edgelist.add(line(0) + " " + line(1))
      node.add(line(0))
      node.add(line(1))

    }
    var rand = new Random(seed)
    var fw = new FileWriter(new File(oFile))
    var bw = new BufferedWriter(fw)
    bw.write(node.size + " " + edgelist.size + "\n")
    var it = edgelist.iterator
    while (it.hasNext) {
      var prob: Double = (Math.round(rand.nextDouble() * 100)).toDouble / 100.0
      bw.write(it.next + " " + prob + "\n")
    }
    bw.flush()
    bw.close()

  }
  

}