package de.tnitsche.mmds

import scala.io.Source
import scala.collection.mutable
import java.util.Arrays
import java.io.PrintWriter
import java.util.zip.GZIPOutputStream
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.file.{Paths, Files}

object Main {
  val FILE = "c:/tmp/sentences.txt"
  val OUTDIR = "f:/tmp/mmds/"
  val KEYSIZE = 5

  def main(args: Array[String]): Unit = {
    val t0 = System.nanoTime()
    loop((10 to 5632).toList)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
  }

  def splitIntoFilesByLength(filename: String, outdir: String) = {
    var fileByLen = mutable.Map[Int, PrintWriter]()
    var i = 0
    for (line <- Source.fromFile(filename).getLines()) {
      i += 1
      if (i % 100000 == 0) println(i + " " + Runtime.getRuntime().freeMemory())
      val len = line.split(" ").size - 1
      if (!fileByLen.contains(len)) {
        val padded = "%05d".format(len)
        val file = new PrintWriter(OUTDIR + padded + ".txt")

        //val file = new PrintWriter(new GZIPOutputStream(new FileOutputStream(OUTDIR + padded + ".txt.gz")))
        fileByLen.put(len, file)
      }
      fileByLen(len).println(line)
    }
    for (file <- fileByLen.values) {
      file.close()
    }
  }

  private def loop(sLenList: List[Int]) = {
    var prevPreMap = scala.collection.mutable.Map[String, List[Int]]()
    var prevPostMap = scala.collection.mutable.Map[String, List[Int]]()
    var prevDataMap = scala.collection.mutable.Map[Int,Array[String]]()
    var result = 0
    for (sLen <- sLenList) {
      println(sLen + ":\n")
	    val (dataMap, preMap, postMap) = readAndIndex(sLen)
	    result += checkShorterCandidates(dataMap, prevDataMap, prevPreMap, prevPostMap)
	    prevPreMap = preMap
	    prevPostMap = postMap
	    prevDataMap = dataMap
	    result += checkCandidates(dataMap, preMap, postMap)
    }
    println("\n========================\n Result:" + result)
  }

  
  private def checkShorterCandidates(dataMap: scala.collection.mutable.Map[Int, Array[String]], 
      prevDataMap: scala.collection.mutable.Map[Int,Array[String]], 
      prevPreMap: scala.collection.mutable.Map[String, List[Int]], 
      prevPostMap: scala.collection.mutable.Map[String, List[Int]]): Int = {
    val t0 = System.nanoTime()
    println("\n======================== Check shorter candidates")

    var resCount = 0
    var compCount = 0
    for (id <- dataMap.keys) {
      val sentence = dataMap(id)
      val (res, comp) = checkShorterSentence(dataMap, prevDataMap, prevPreMap, prevPostMap,  id)
      resCount += res
      compCount += comp
    }
    val t1 = System.nanoTime()
    println("check candidates: " + (t1 - t0) / 1000000 + "ms")
    println("========================\n" + resCount + " (comparisons: " + compCount + " - ratio: " + resCount.toFloat / compCount + ")")
    return resCount
  }
  
  private def checkCandidates(dataMap: scala.collection.mutable.Map[Int, Array[String]], 
      preMap: scala.collection.mutable.Map[String, List[Int]], 
      postMap: scala.collection.mutable.Map[String, List[Int]]): Int = {
    val t0 = System.nanoTime()
    println("\n======================== Check candidates")

    var resCount = 0
    var compCount = 0
    for (id <- dataMap.keys) {
      val sentence = dataMap(id)
      val (res, comp) = checkSentence(dataMap, preMap, postMap,  id)
      resCount += res
      compCount += comp
    }
    val t1 = System.nanoTime()
    println("check candidates: " + (t1 - t0) / 1000000 + "ms")
    println("========================\n" + resCount + " (comparisons: " + compCount + " - ratio: " + resCount.toFloat / compCount + ")")
    return resCount
  }
  
  private def checkShorterSentence(dataMap: scala.collection.mutable.Map[Int,Array[String]], 
      prevDataMap: scala.collection.mutable.Map[Int,Array[String]], 
      preMap: scala.collection.mutable.Map[String,List[Int]], 
      postMap: scala.collection.mutable.Map[String,List[Int]], 
      id: Int): (Int, Int) = {
    var resCount = 0
    var compCount = 0
    val sentence = dataMap(id)
    val prefix = sentence.take(KEYSIZE).mkString(" ")
    val postfix = sentence.takeRight(KEYSIZE).mkString(" ")
    var set = List[Int]() 
    if (preMap.contains(prefix)) {
      set = set ++ preMap(prefix)
    }
    if (postMap.contains(postfix)) {
      set = set ++ postMap(postfix)
    }
    set = set.distinct
    for (s1 <- set) {
      compCount += 1
      if (hasEditDistanceLE1(dataMap(id), prevDataMap(s1))) resCount += 1
    }
    return (resCount, compCount)
  }
  
  private def checkSentence(dataMap: scala.collection.mutable.Map[Int,Array[String]], 
      preMap: scala.collection.mutable.Map[String,List[Int]], 
      postMap: scala.collection.mutable.Map[String,List[Int]], 
      id: Int): (Int, Int) = {
    var resCount = 0
    var compCount = 0
    val sentence = dataMap(id)
    val prefix = sentence.take(KEYSIZE).mkString(" ")
    val postfix = sentence.takeRight(KEYSIZE).mkString(" ")
    var set = (List() ++ preMap(prefix) ++ postMap(postfix)).filter(p => p > id).distinct
    for (s1 <- set) {
      compCount += 1
      if (hasEditDistanceLE1(dataMap(id), dataMap(s1))) resCount += 1
    }
    return (resCount, compCount)
  }

  private def readAndIndex(sLength: Int): (scala.collection.mutable.Map[Int, Array[String]], 
      scala.collection.mutable.Map[String, List[Int]], 
      scala.collection.mutable.Map[String, List[Int]]) = {
    val t0 = System.nanoTime()
    var dataMap = mutable.Map[Int, Array[String]]()
    var prefixMap = mutable.Map[String, List[Int]]().withDefaultValue(List())
    var postfixMap = mutable.Map[String, List[Int]]().withDefaultValue(List())
    var i = 0
    val fileName = OUTDIR + "%05d".format(sLength) + ".txt"
    if (Files.exists(Paths.get(fileName))) {
	    for (line <- Source.fromFile(fileName).getLines()) {
	      i += 1
	      if (i % 100000 == 0) println(i + " " + Runtime.getRuntime().freeMemory())
	      var lineArr = line.split(" ");
	      val words = lineArr.tail
	      var id = lineArr.head.toInt;
	      dataMap(id) = words
	      val prefix = words.take(KEYSIZE).mkString(" ")
	      prefixMap(prefix) = prefixMap(prefix) :+ id
	      val postfix = words.takeRight(KEYSIZE).mkString(" ")
	      postfixMap(postfix) = postfixMap(postfix) :+ id
	    }
    }
    val t1 = System.nanoTime()
    println("readAndIndex: " + (t1 - t0) / 1000000 + "ms")

    (dataMap, prefixMap, postfixMap)
  }

  def hasEditDistanceLE1(s1: Array[String], s2: Array[String]): Boolean = {
    if (s1.size != s2.size) {
      var sh = s1.toList
      var lo = s2.toList
      if (s1.size > s2.size) {
        sh = s2.toList
        lo = s1.toList
      }
      var offset = 0
      for (i <- 0 to lo.size - 1) {
        if (lo.take(i) ++ lo.drop(i + 1) == sh) return true
      }
      return false
    } else {
      var c = 0;
      for (i <- 0 to s1.size - 1) {
        if (s1(i) == s2(i)) c += 1
      }
      return s1.size - c <= 1
    }
  }
}