package com.ulb.code.wit.main
import scala.util.control._

import java.io.{ File, FileWriter, BufferedWriter }
import java.util.Date
import java.util.Properties
import java.io.FileInputStream
import scala.io.Source
import scala.collection.mutable.HashSet
import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ArrayBuilder
/**
 * @author Rohit
 */
object Testing {
  var number_of_buckets = 128
  var window = { 60 * 60 * 24 * 1 }
  var seeds = { 10 }
  val filename = "dblp_coauthor"
  var folder = "/Users/rk/Google Drive/testdata/groundTruth"
  //  val datafile = folder + "higgs-activity_time.csv"
  var datafile = folder + "input//" + filename + ".txt"
  //  val datafile = folder + "higgs-social_network.txt"
  //    val datafile = folder + "sample.txt"

  var outfile = folder + "output//" + filename
  //  val outfile = folder + "higgs-social_network"
  //  val outfile = folder + "higgs-activity"
  //  val file = Array("higgs-activity_time")
  val file = Array("slashdot-threads", "cit-HepPh", "enron", "facebook-wosn-wall", "higgs-activity_time", "lkml-reply")

  val windows = Array(1, 2, 5, 10);

  //    val folder = "/Users/rk/Documents/phd/testdata/"

  def main(args: Array[String]) {

    test
    //    val files = Array("Gowalla", "foursquare", "BrightKi")
    //
    //    val window = Array(36000, 72000, 180000);
    //    val f = Array(2, 5, 10, 20);
    //    for (n <- 0 to files.length - 1) {
    //      println(files(n))
    //      for (m <- 0 to window.length - 1) {
    //        println(window(m))
    //        for (i <- 0 to f.length - 1) {
    //          for (j <- i + 1 to f.length - 1) {
    //            val firstFile = "D:\\dataset\\new\\" + files(n) + "\\LBSNData\\" + files(n) + "_w" + window(m) + "_f" + f(i) + "_s100.keys"
    //            val secondFile = "D:\\dataset\\new\\" + files(n) + "\\LBSNData\\" + files(n) + "_w" + window(m) + "_f" + f(j) + "_s100.keys"
    //
    //            print(f(i) + "," + f(j) + ",")
    //            findCommonKeys(firstFile, secondFile, 10)
    //            println
    //          }
    //        }
    //      }
    //    }
    //    val firstFile = "D:\\dataset\\new\\NYC\\foursquare\\foursquare_w36000_f5_s50.keys"
    //     val secondFile = "D:\\dataset\\new\\NYC\\foursquare\\foursquareWeightedFriend_w10_f5_s50.keys"
    //   findCommonKeys(firstFile, secondFile, 10)
    //    //   convertfile(datafile)
    //    //        test
    //    //    check
    //    //generateKey
    //    // parseResult("C://phd//testdata//runTimeForC.txt")
    //    //    testReverse
    //    //    CheckGraph(datafile);
    //    //
    //    //        for (files <- file) {
    //    //          // println(files)
    //    //          println()
    //    //          val key = 100
    //    //          for (i <- 0 to windows.length - 1) {
    //    //            for (j <- (i + 1) to windows.length - 1) {
    //    //              print(windows(i) + ":" + windows(j) + ":")
    //    //              findCommonKeys(folder + "outputExact//" + files + "_" + windows(i) + "_"+key+".keys", folder + "outputExact//" + files + "_" + windows(j) + "_"+key+".keys")
    //    //            }
    //    //          }
    //    //    
    //    //        }

  }
  def testReverse() {
    for (input <- file) {
      for (wind <- windows) {
        println("*********************************************")
        println("file:: " + input + " window:: " + wind)
        val inputpath = folder + File.separator + "input" + File.separator + input + ".txt";
        val outputpath = folder + File.separator + "reverseOutput" + File.separator + input + "_" + wind
        testApproxReverse(wind, inputpath, outputpath)
      }
    }
  }
  def testApprox() {
    for (input <- file) {
      for (wind <- windows) {
        println("*********************************************")
        println("file:: " + input + " window:: " + wind)
        val inputpath = folder + File.separator + "input" + File.separator + input + ".txt";
        val outputpath = folder + File.separator + "reverseOutput" + File.separator + input + "_" + wind
        testApprox(wind, inputpath, outputpath)
      }
    }
  }
  def findCommonKeys(file1: String, file2: String, k: Int) {
    var k1: HashSet[String] = HashSet.empty
    var k2: HashSet[String] = HashSet.empty
    var count = 0;
    val loop = new Breaks;
    loop.breakable {
      for (line <- Source.fromFile(file1).getLines()) {
        k1.add(line)
        count = count + 1;
        if (count == k)
          loop.break()
      }
    }
    count = 0
    loop.breakable {
      for (line <- Source.fromFile(file2).getLines()) {
        k2.add(line)
        count = count + 1;
        if (count == k)
          loop.break()
      }
    }
    print(k1.intersect(k2).size)

  }

  def test() {
    val prop = new Properties();
    prop.load(new FileInputStream("config.properties"));
    folder = prop.getProperty("graphFolder")
    val files = prop.getProperty("graphFiles").split(",")
    val window = prop.getProperty("window").split(",")
    val seeds = prop.getProperty("seeds").split(",")
    val outfolder = prop.getProperty("outputFolder")
    val fout = new File(outfolder);
    if (!fout.exists()) {
      fout.mkdirs();
    }
    val sb: StringBuilder = new StringBuilder()
    val mode = prop.getProperty("mode")
    number_of_buckets = prop.getProperty("number_of_buckets").toInt
    for (i <- 0 to files.length - 1) {
      datafile = folder + files(i) + ".txt"
      outfile = outfolder + files(i)
      println("starting for file : " + files(i))
      for (j <- 0 to window.length - 1) {
        println("starting for window : " + window(j))
        sb.append("starting for window : " + window(j) + "\n")
        var maxseed = 0
        for (k <- 0 to seeds.length - 1) {
          if (seeds(k).toInt > maxseed) {
            maxseed = seeds(k).toInt
          }
        }

        println("max seeds : " + maxseed)
        sb.append("max seeds : " + maxseed + "\n")
        if (mode.equals("approx")) {
          System.gc()
          val isapprox = new InfluenceSetApprox(window(j).toDouble, number_of_buckets, datafile)
          isapprox.compute(sb)
          var nodessummaryApprox = isapprox.nodesummary
          val fapprox = new File(outfile + "_" + window(j) + "_approx.csv")

          val bwapprox = new BufferedWriter(new FileWriter(fapprox))

          nodessummaryApprox.foreach(x => {
            bwapprox.write(x._1 + "," + x._2.estimate() + "\n")
          })
          bwapprox.close()
          var tstart = new Date().getTime
          var seednode = isapprox.findseeds(maxseed)
          println("time to find seeds : " + (new Date().getTime - tstart))
          sb.append("time to find seeds : " + (new Date().getTime - tstart) + "\n")
          for (k <- 0 to seeds.length - 1) {
            val fkey = new File(outfile + "_" + window(j) + "_" + seeds(k) + ".keys")

            val bwkey = new BufferedWriter(new FileWriter(fkey))
            for (m <- 0 to seeds(k).toInt - 1) {
              bwkey.write(seednode(m) + "\n")
            }
            bwkey.close()
          }

        } else {
          System.gc()
          val isexact = new InfluenceSetExact(window(j).toDouble, datafile)
          isexact.compute(sb)
          var nodessummaryExact = isexact.nodesummary
          val fexact = new File(outfile + "_" + window(j) + "_exact.csv")

          val bwexact = new BufferedWriter(new FileWriter(fexact))

          nodessummaryExact.foreach(x => {
            bwexact.write(x._1 + "," + x._2.size + "\n")
          })
          bwexact.close()
          var tstart = new Date().getTime
          var seednode = isexact.findseeds(maxseed)
          println("time to find seeds : " + (new Date().getTime - tstart))
          sb.append("time to find seeds : " + (new Date().getTime - tstart) + "\n")
          for (k <- 0 to seeds.length - 1) {
            val fkey = new File(outfile + "_" + window(j) + "_" + seeds(k) + "_exact.keys")

            val bwkey = new BufferedWriter(new FileWriter(fkey))
            for (m <- 0 to seeds(k).toInt - 1) {
              bwkey.write(seednode(m) + "\n")
            }
            bwkey.close()
          }
        }
      }
    }
    val f = new File(outfolder + "summary_" + new Date().getTime + ".txt")

    val bw = new BufferedWriter(new FileWriter(f))
    bw.write(sb.toString());
    bw.close;
  }
  def compareBoth() {
    val isapprox = new InfluenceSetApprox(window, number_of_buckets, datafile)
    isapprox.compute(new StringBuilder)
    var seednodeApprox = queryApprox(isapprox)
    println("************************************************")
    val isexact = new InfluenceSetExact(window, datafile)
    isexact.compute(new StringBuilder)
    var seednodeExact = queryExact(isexact)
    println("************************************************")
    println("Exact influnce with approx seeds: " + isexact.findInflunce(seednodeApprox))
    println("Approx influnce with exact seeds: " + isapprox.findInflunce(seednodeExact))
    //    
  }
  def check() {
    val window = Array(1);
    val seeds = 10
    println("seeds: " + seeds)
    for (i <- 0 to window.length - 1) {
      val isexact = new InfluenceSetApprox(window(i), number_of_buckets, datafile)
      isexact.compute(new StringBuilder)

      for (k <- 0 to window.length - 1) {
        val key = SimulatePropagation.getData(folder + "output//keys//" + filename + "_" + window(k) + "_" + seeds + ".keys")
        val keysInt = key.map { x => x.toInt }
        print(" , " + window(k) + " , " + isexact.findInflunce(keysInt))
      }
      println

    }
  }
  def queryApprox(isapprox: InfluenceSetApprox): Array[Int] = {
    var tstart = new Date().getTime
    var seednodeApprox = isapprox.findseeds(seeds)
    print("Approx Seed nodes: ")
    for (i <- 0 to seeds - 1) {
      print(seednodeApprox(i) + ",")
    }
    println()
    println("time to find " + seeds + " seeds : " + (new Date().getTime - tstart))

    println("influnce: " + isapprox.findInflunce(seednodeApprox))

    seednodeApprox
  }
  def queryExact(isexact: InfluenceSetExact): Array[Int] = {

    var tstart = new Date().getTime
    var seednodeExact = isexact.findseeds(seeds)
    print("Exact Seed nodes: ")
    for (i <- 0 to seeds - 1) {
      print(seednodeExact(i) + ",")
    }
    println()
    println("time to find " + seeds + "  seeds : " + (new Date().getTime - tstart))
    tstart = new Date().getTime
    println("influnce: " + isexact.findInflunce(seednodeExact))

    seednodeExact
  }
  def testApprox(wind: Int, file: String, fout: String) {
    val isapprox = new InfluenceSetApprox(wind, number_of_buckets, file)
    isapprox.compute(new StringBuilder)
    var nodessummaryApprox = isapprox.nodesummary
    val fapprox = new File(fout + "_approx.csv")

    val bwapprox = new BufferedWriter(new FileWriter(fapprox))

    nodessummaryApprox.foreach(x => {
      bwapprox.write(x._1 + "," + x._2.estimate() + "\n")
    })
    bwapprox.close()
    //    var tstart = new Date().getTime
    //    var seednode = isapprox.findseeds(seeds)
    //    println("time to find seeds : " + (new Date().getTime - tstart))
    //    tstart = new Date().getTime
    //    println("influnce: " + isapprox.findInflunce(seednode))
    //    println("time to find influnce: " + (new Date().getTime - tstart))

  }
  def testApproxReverse(wind: Int, file: String, fout: String) {
    val isapprox = new InfluenceSetApproxReverse(wind, number_of_buckets, file)
    isapprox.compute(new StringBuilder)
    var nodessummaryApprox = isapprox.nodesummary
    val fapprox = new File(fout + "_reverse.csv")

    val bwapprox = new BufferedWriter(new FileWriter(fapprox))

    nodessummaryApprox.foreach(x => {
      bwapprox.write(x._1 + "," + x._2.estimate() + "\n")
    })
    bwapprox.close()

  }
  def testExact() {
    val isexact = new InfluenceSetExact(window, datafile)
    isexact.compute(new StringBuilder)
    var nodessummaryExact = isexact.nodesummary
    val f = new File(outfile + "_result.csv")

    val bw = new BufferedWriter(new FileWriter(f))

    nodessummaryExact.foreach(x => {
      bw.write(x._1 + "," + x._2.size + "\n")
    })
    bw.close()
    var tstart = new Date().getTime
    var seednode = isexact.findseeds(seeds)
    println("time to find seeds : " + (new Date().getTime - tstart))
    tstart = new Date().getTime
    println("influnce: " + isexact.findInflunce(seednode))
    println("time to find influnce: " + (new Date().getTime - tstart))

  }

  def convertfile(file: String) {
    val f = new File(folder + "inputC//" + filename + ".txt")

    val bw = new BufferedWriter(new FileWriter(f))
    for (line <- Source.fromFile(file).getLines()) {

      bw.write(line.replaceAll(",", " ") + "\n")
    }
    bw.close
    println("done")
  }

  def CheckGraph(file: String) {
    var nodeswithIncomingEdge: HashSet[String] = HashSet.empty
    var nodeswithOutgoingEdge: HashSet[String] = HashSet.empty
    for (line <- Source.fromFile(file).getLines()) {
      val temp = line.split(",")
      nodeswithIncomingEdge.add(temp(1))
      nodeswithOutgoingEdge.add(temp(0))
    }
    println("file : " + file)
    val temp = nodeswithIncomingEdge.union(nodeswithOutgoingEdge).size
    println("nodes with no incoming edge: " + (temp - nodeswithIncomingEdge.size))
    println("nodes with no outgoing edge: " + (temp - nodeswithOutgoingEdge.size))
    println("total nodes: " + temp)
  }
  def generateKey() {
    val prop = new Properties();
    prop.load(new FileInputStream("config.properties"));
    val files = prop.getProperty("graphFiles").split(",")
    val window = prop.getProperty("window").split(",")
    val seeds = prop.getProperty("seeds").split(",")
    for (file <- files) {
      for (wind <- window) {
        for (seed <- seeds) {
          getKeys(folder + file, seed.toInt, wind)
        }
      }
    }

  }
  def getKeys(ifile: String, key: Int, wind: String) {
    var result: ArrayBuilder[Int] = ArrayBuilder.make()
    val input = ifile + "_" + wind + "_100.keys"
    println(input)
    for (line <- Source.fromFile(ifile + "_" + wind + "_100.keys").getLines()) {
      result.+=(line.toInt)
    }
    val data = result.result()

    val f = new File(ifile + "_" + wind + "_" + key + ".keys")
    val bw = new BufferedWriter(new FileWriter(f))
    for (i <- 0 to key - 1) {
      bw.write(data(i) + "\n")
    }
    bw.close()

  }
  def parseResult(ifile: String) {
    var linecount = 0
    var previous = 0
    var count = 0
    for (line <- Source.fromFile(ifile).getLines()) {
      linecount = linecount + 1
      if (linecount == 2 || (linecount - previous) == 8) {
        print(line + ",")
        previous = linecount
      }
      if (line.contains("time taken in")) {
        val time = line.split(":")(1).split(" ")(0)
        val temp = line.split(":")(3).trim()
        println(time + "," + temp)
      }

    }

  }
}