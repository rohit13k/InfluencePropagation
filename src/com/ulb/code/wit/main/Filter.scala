package com.ulb.code.wit.main
import java.io.FileInputStream
import scala.io.Source
import scala.collection.mutable.HashSet
import java.io.FileWriter
import java.io.File
import java.io.BufferedWriter
import scala.util.Random
import scala.collection.mutable.HashMap
import java.io.{ ObjectOutputStream, ObjectInputStream }
import java.io.{ FileOutputStream, FileInputStream }
object Filter {
  def main(args: Array[String]): Unit =
    {
      var folder = "D:\\phd\\testdata\\"
      val file = Array("slashdot-threads", "enron", "facebook-wosn-wall", "higgs-activity_time", "lkml-reply", "dblp_coauthor")
      for (f <- file) {
        var line: Array[String] = Array.empty
        var input = Source.fromFile(folder + "\\input\\Archive\\" + f + "_training.txt").getLines()

        var fw = new FileWriter(new File(folder + "\\inputC\\" + f + "_training.txt"))
        var bw = new BufferedWriter(fw)
        while (input.hasNext) {
          line = input.next().split(",")
          bw.write(line(0) + " " + line(1) + " " + line(2) + "\n")
        }
        bw.flush()
        bw.close()
      }
    }
}