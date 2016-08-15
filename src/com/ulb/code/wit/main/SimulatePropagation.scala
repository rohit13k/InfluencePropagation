package com.ulb.code.wit.main
import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.immutable.HashSet
import scala.util.Random
import scala.collection.mutable.ArrayBuilder
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.impl.Future
import scala.concurrent.Await
import java.util.Date
/**
 * @author rk
 */
object SimulatePropagation {
  val file = Array("dblp_coauthor")
  val splitter = " "
  //  val file = Array("slashdot-threads", "enron", "facebook-wosn-wall", "higgs-activity_time", "lkml-reply", "dblp_coauthor")

  val window = Array(1, 10, 20);
  val seeds = Array(10, 50, 100)
  //    val folder = "/Users/rk/Documents/phd/testdata/"
  //  val folder = "C:\\phd\\testdata\\"
  val folder = "C:\\phd\\testdata\\groundTruth\\"

  val simulation = 100
  def main(args: Array[String]): Unit =
    {
      // testSimulateWithWindow
      //      testSimulateWithWindowinParrallen()
      val file = "higgs-activity_Retweet"
      val iFile = getData(folder + file + ".txt")
      var result: (Double, Double) = (0.0, 0.0)
      for (j <- 0 to seeds.length - 1) {
        println("seeds: " + seeds(j))
        for (k <- 0 to window.length - 1) {
          val ts = new Date().getTime
          print("window, " + window(k))
          var pageFile = getData(folder + "result\\" + file + "_PageRank_100.txt", seeds(j))
          result = simulatewithWindow(pageFile, iFile, simulation, window(k).toLong)
          print(", mean , " + Math.round(result._1.toDouble) + " ,sd ," + Math.round(result._2))

          var countFile = getData(folder + "result\\" + file + "_outDegree_100.txt", seeds(j))
          result = simulatewithWindow(countFile, iFile, simulation, window(k).toLong)
          print(", mean , " + Math.round(result._1.toDouble) + " ,sd ," + Math.round(result._2))

          var keyFile = getData(folder + "result\\" + file + "_" + window(k) + "_100.keys", seeds(j))
          //          var keyFile = folder + "exactoutput/" + file(i) + "_" + window(k) + "_" + seeds(j) + ".keys"

          result = simulatewithWindow(keyFile, iFile, simulation, window(k).toLong)
          print(", " + window(k) + "," + Math.round(result._1.toDouble) + "," + Math.round(result._2))

          println

          //  println("\n" + "My approach: seeds: " + seeds(j) + " window : " + window(k) + " : mean :: " + result._1 + " standard deviation ::" + result._2)

        }
      }
    }
  def testSimulateWithWindow() {

    var result: (Double, Double) = (0.0, 0.0)
    for (i <- 0 to file.length - 1) {
      val iFile = getData(folder + "input\\" + file(i) + ".txt")

      println("\n" + "************************************")
      println(file(i))
      for (j <- 0 to seeds.length - 1) {
        println("seeds: " + seeds(j))
        for (k <- 0 to window.length - 1) {
          val ts = new Date().getTime
          print("window, " + window(k))
          var skimkeyFile = getData(folder + "metis\\keys\\" + file(i) + "_" + seeds(j) + ".keys")
          result = simulatewithWindow(skimkeyFile, iFile, simulation, window(k).toLong)
          print(", mean , " + Math.round(result._1.toDouble) + " ,sd ," + Math.round(result._2))
          var keyFile = getData(folder + "outputC\\" + file(i) + "_" + window(k) + "_" + seeds(j) + ".keys")
          //          var keyFile = folder + "exactoutput/" + file(i) + "_" + window(k) + "_" + seeds(j) + ".keys"

          for (m <- 0 to window.length - 1) {
            result = simulatewithWindow(keyFile, iFile, simulation, window(m).toLong)
            print(", " + window(m) + "," + Math.round(result._1.toDouble) + "," + Math.round(result._2))
          }
          println

          //  println("\n" + "My approach: seeds: " + seeds(j) + " window : " + window(k) + " : mean :: " + result._1 + " standard deviation ::" + result._2)

        }
      }
    }
  }
  def getData(file: String): Array[String] = {
    var input = Source.fromFile(file).getLines()
    var result: ArrayBuilder[String] = ArrayBuilder.make()
    while (input.hasNext) {
      result.+=(input.next)

    }
    result.result()
  }
  def getData(file: String, numberOflines: Int): Array[String] = {
    var input = Source.fromFile(file).getLines()
    var result: ArrayBuilder[String] = ArrayBuilder.make()
    var i = 0
    while (input.hasNext && i < numberOflines) {
      result.+=(input.next)
      i = i + 1
    }
    result.result()
  }
  def testSimulateWithWindowinParrallen() {

    var result: (Double, Double) = (0.0, 0.0)
    for (i <- 0 to file.length - 1) {
      var iFile = getData(folder + "input\\" + file(i) + ".txt")
      println("\n" + "************************************")
      println(file(i))
      for (j <- 0 to seeds.length - 1) {
        println("seeds: " + seeds(j))
        for (k <- 0 to window.length - 1) {

          print("window, " + window(k))
          var skimkeyFile = getData(folder + "metis\\keys\\" + file(i) + "_" + seeds(j) + ".keys")
          result = simulatewithWindowParallel(skimkeyFile, iFile, simulation, window(k).toLong)
          print(", mean , " + Math.round(result._1.toDouble) + " ,sd ," + Math.round(result._2))
          var keyFile = getData(folder + "outputC\\" + file(i) + "_" + window(k) + "_" + seeds(j) + ".keys")
          //          var keyFile = folder + "exactoutput/" + file(i) + "_" + window(k) + "_" + seeds(j) + ".keys"

          for (m <- 0 to window.length - 1) {
            result = simulatewithWindowParallel(keyFile, iFile, simulation, window(m).toLong)
            print(", " + window(m) + "," + Math.round(result._1.toDouble) + "," + Math.round(result._2))
          }
          println
        }
      }

    }
  }
  //  def testSimulate() {
  //
  //    var result: (Double, Double) = (0.0, 0.0)
  //    for (i <- 0 to file.length - 1) {
  //      var iFile = folder + "input/" + file(i) + ".txt"
  //      println("************************************")
  //      println(file(i))
  //      for (j <- 0 to seeds.length - 1) {
  //        var skimkeyFile = folder + "metis\\keys\\" + file(i) + "_" + seeds(j) + ".keys"
  //        result = simulate(skimkeyFile, iFile, simulation)
  //        println("SKIM: seeds: " + seeds(j) + " : mean :: " + result._1 + " standard deviation ::" + result._2)
  //        for (k <- 0 to window.length - 1) {
  //
  //          //    var keyFile = folder + "output/keys/" + file(i) + "_" + window(k) + "_" + seeds(j) + ".keys"
  //          var keyFile = folder + "output\\keys\\" + file(i) + "_" + window(k) + "_" + seeds(j) + ".keys"
  //          result = simulate(keyFile, iFile, simulation)
  //          println("My approach: seeds: " + seeds(j) + " window : " + window(k) + " : mean :: " + result._1 + " standard deviation ::" + result._2)
  //
  //        }
  //      }
  //    }
  //  }
  //  def simulate(keyFile: String, iFile: String, simulationCount: Int): (Double, Double) = {
  //    var spread = 0.0
  //    var input = Source.fromFile(keyFile).getLines()
  //    var seeds: HashSet[Int] = HashSet.empty
  //    var spreadData: ArrayBuilder[Double] = ArrayBuilder.make()
  //    while (input.hasNext) {
  //      seeds = seeds.+(input.next.toInt)
  //    }
  //    for (i <- 0 to simulationCount - 1) {
  //      val temp = simulateSpread(iFile, seeds)
  //      spread = spread + temp
  //      //      println("i =" + i + "  spread=" + spread)
  //      spreadData = spreadData.+=(temp)
  //    }
  //    val mean = spread / simulationCount
  //    val sd = Math.sqrt((spreadData.result().map { x => (x - mean) * (x - mean) }).sum / simulationCount)
  //    (mean, sd)
  //  }
  def simulatewithWindow(keyFile: Array[String], iFile: Array[String], simulationCount: Int, windowpercent: Long): (Double, Double) = {
    var spread = 0.0
    var spreadData: ArrayBuilder[Double] = ArrayBuilder.make()

    var seeds: HashSet[Int] = HashSet.empty

    var count = 0;
    for (i <- 0 to keyFile.length - 1) {
      seeds = seeds.+(keyFile(i).toInt)
    }

    var dstart = 0l
    var dend = 0l

    for (i <- 0 to iFile.length - 1) {
      val line = iFile(i).split(splitter)
      if (count == 0) {
        dstart = line(2).toLong
      }
      dend = line(2).toLong
      count = count + 1
    }
    val window = (dend - dstart) * windowpercent / 100;

    for (i <- 0 to simulationCount - 1) {
      val temp = simulateSpreadWithTimeWindow(iFile, seeds, window)
      spread = spread + temp
      spreadData.+=(temp)
      //      println("i =" + i + "  spread=" + spread)
    }
    val mean = spread / simulationCount
    val data = spreadData.result();
    val sd = Math.sqrt((data.map { x => (x - mean) * (x - mean) }).sum / simulationCount)
    (mean, sd)
  }
  def simulatewithWindowParallel(keyFile: Array[String], input: Array[String], simulationCount: Int, windowpercent: Long): (Double, Double) = {
    var spread = 0.0
    var spreadData: ArrayBuilder[Double] = ArrayBuilder.make()

    var seeds: HashSet[Int] = HashSet.empty

    var count = 0;
    for (i <- 0 to keyFile.length - 1) {
      seeds = seeds.+(keyFile(i).toInt)
    }

    var dstart = 0l
    var dend = 0l
    for (i <- 0 to input.length - 1) {
      val line = input(i) split (splitter)
      if (count == 0) {
        dstart = line(2).toLong
      }
      dend = line(2).toLong
      count = count + 1
    }

    val window = (dend - dstart) * windowpercent / 100

    val tasks: Seq[Future[Int]] = for (i <- 1 to simulationCount - 1) yield Future {
      simulateSpreadWithTimeWindow(input, seeds, window)
    }
    val aggregated: Future[Seq[Int]] = Future.sequence(tasks)

    val result: Seq[Int] = Await.result(aggregated, Duration.Inf)

    for (i <- 0 to result.length - 1) {
      val temp = result(i)
      spread = spread + temp
      spreadData.+=(temp)
      //      println("i =" + i + "  spread=" + spread)
    }
    val mean = spread / simulationCount
    val data = spreadData.result();
    val sd = Math.sqrt((data.map { x => (x - mean) * (x - mean) }).sum / simulationCount)
    (mean, sd)
  }
  def simulateSpread(iFile: String, seeds: HashSet[Int]): Int = {
    var input = Source.fromFile(iFile).getLines()
    var line: Array[String] = Array.empty
    var node: HashMap[Int, Boolean] = HashMap.empty
    while (input.hasNext) {
      line = input.next.split(splitter)
      //if the nodes are for first time add them in node list with activation as false
      if (!node.contains(line(0).toInt)) {
        node.+=((line(0).toInt, false))
      }
      if (!node.contains(line(1).toInt)) {
        node.+=((line(1).toInt, false))
      }
      //if the first node is in seeds activate it
      if (seeds.contains(line(0).toInt)) {
        node.update(line(0).toInt, true)

      }

      //if first node is active do a coin toss and update the 2nd node id true
      if (node.getOrElse(line(0).toInt, false)) {
        if (toss == 1) {
          node.update(line(1).toInt, true)
        }
      }
    }
    var spread = 0

    var iter = node.foreach(f => {
      if (f._2) {
        spread = spread + 1
      }
    })

    spread
  }

  def simulateSpreadWithTimeWindow(iFile: Array[String], seeds: HashSet[Int], window: Long): Int = {

    var line: Array[String] = Array.empty
    var node: HashMap[Int, (Boolean, Long)] = HashMap.empty
    for (i <- 0 to iFile.length - 1) {
      line = iFile(i).split(splitter)
      val etime = line(2).toLong
      //if the nodes are for first time add them in node list with activation as false
      if (!node.contains(line(0).toInt)) {
        if (seeds.contains(line(0).toInt)) {
          node.+=((line(0).toInt, (true, etime)))

        } else {
          node.+=((line(0).toInt, (false, 0)))
        }
      } else {

        //        if (seeds.contains(line(0).toInt) && (!oldvalue._1)) {
        if (seeds.contains(line(0).toInt)) {
          node.update(line(0).toInt, (true, etime))

        }
      }
      if (!node.contains(line(1).toInt)) {
        //        if (seeds.contains(line(1).toInt)) {
        //          node.+=((line(1).toInt, (true, etime)))
        //
        //        } else {
        node.+=((line(1).toInt, (false, 0)))
        //        }

      }

      //if first node is active do a coin toss and update the 2nd node id true
      val oldvalue = node.getOrElse(line(0).toInt, { (false, 0l) })
      if (oldvalue._1) {
        if ((etime - oldvalue._2) <= window) {
          val valu = node.getOrElse(line(1).toInt, { (false, 0l) })
          if (oldvalue._2 > valu._2) {
            if (toss == 1) {
              node.update(line(1).toInt, (true, oldvalue._2))
            }
          }
        }

      }
    }
    var spread = 0

    var iter = node.foreach(f => {
      if (f._2._1) {
        spread = spread + 1
      }
    })

    spread

  }

  def toss(): Int = {
    var rand = new Random()
    rand.nextInt(2)
  }
}