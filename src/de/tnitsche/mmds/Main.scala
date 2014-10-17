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
    loop((44 to 54).toList)
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
    var prevPreMap = mutable.Map[String, List[Int]]()
    var prevPostMap = mutable.Map[String, List[Int]]()
    var prevDataMap = mutable.Map[Int,Array[String]]()
    var result = 0
    for (sLen <- sLenList) {
    	println(sLen + ":")
	    val (dataMap, preMap, postMap) = readAndIndex(sLen)
	    result += checkCandidates(dataMap, preMap, postMap, prevDataMap, prevPreMap, prevPostMap)._1 
	    // ready for garbage collection
	    prevPreMap = preMap
	    prevPostMap = postMap
	    prevDataMap = dataMap
    }
    println("\n========================\n Result:" + result)
  }

  
  private def checkCandidates(dataMap: scala.collection.mutable.Map[Int, Array[String]],  
      preMap: mutable.Map[String, List[Int]], 
      postMap: mutable.Map[String, List[Int]],
      prevDataMap: mutable.Map[Int,Array[String]], 
      prevPreMap: mutable.Map[String, List[Int]], 
      prevPostMap: mutable.Map[String, List[Int]]): (Int, Int) = {
    val t0 = System.nanoTime()
    var resCount = 0
    var compCount = 0
    for (id <- dataMap.keys) {
      val sentence = dataMap(id)
      val (res, comp) = checkShorterSentence(sentence, prevDataMap, prevPreMap, prevPostMap,  id)
      resCount += res
      compCount += comp
      val (res2, comp2) = checkSentence(sentence, dataMap, preMap, postMap, id)
      resCount += res2
      compCount += comp2
    }
    val t1 = System.nanoTime()
    println("check candidates: " + (t1 - t0) / 1000000 + "ms")
    println("=" + resCount + " (comparisons: " + compCount + " - ratio: " + resCount.toFloat / compCount + ")\n")
    return (resCount, compCount)
  }
  
  private def checkShorterSentence(sentence: Array[String], 
      prevDataMap: mutable.Map[Int,Array[String]], 
      preMap: mutable.Map[String,List[Int]], 
      postMap: mutable.Map[String,List[Int]], 
      id: Int): (Int, Int) = {
    var (resCount, compCount) = (0, 0)
    val (prefix, postfix) = createPrefixAndPostfix(sentence)
    var set = (List[Int]() ++ preMap.getOrElse(prefix, List()) ++ postMap.getOrElse(postfix, List())).distinct
    for (s1 <- set) {
      compCount += 1
      if (hasEditDistanceLE1(sentence, prevDataMap(s1))) resCount += 1
    }
    return (resCount, compCount)
  }

  private def createPrefixAndPostfix(sentence: Array[String]): (String, String) = {
    val prefix = sentence.take(KEYSIZE).mkString(" ")
    val postfix = sentence.takeRight(KEYSIZE).mkString(" ")
    (prefix, postfix)
  }
  
  private def checkSentence(sentence: Array[String], 
      dataMap: mutable.Map[Int,Array[String]], 
      preMap: mutable.Map[String,List[Int]], 
      postMap: mutable.Map[String,List[Int]], 
      id: Int): (Int, Int) = {
    var (resCount, compCount) = (0, 0)
    val (prefix, postfix) = createPrefixAndPostfix(sentence)
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
    val fileName = OUTDIR + "%05d".format(sLength) + ".txt"
    if (Files.exists(Paths.get(fileName))) {
	    for (line <- Source.fromFile(fileName).getLines()) {
	      var lineArr = line.split(" ");
	      val words = lineArr.tail
	      var id = lineArr.head.toInt;
	      dataMap(id) = words
	      val (prefix, postfix) = createPrefixAndPostfix(words)
	      prefixMap(prefix) = prefixMap(prefix) :+ id
	      postfixMap(postfix) = postfixMap(postfix) :+ id
	    }
    }
    val t1 = System.nanoTime()
    println("readAndIndex: (" + fileName + ")  " + (t1 - t0) / 1000000 + "ms")

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