package de.tnitsche.mmds

import scala.io.Source
import scala.collection.mutable
import java.util.Arrays
import java.io.PrintWriter
import java.util.zip.GZIPOutputStream
import java.io.FileOutputStream
import java.io.OutputStreamWriter

object Main {
  val FILE = "c:/tmp/sentences.txt"
  val OUTDIR = "f:/tmp/mmds/"
  val KEYSIZE = 5

  def main(args: Array[String]): Unit = {
    val t0 = System.nanoTime()
    firstPass
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")

  }
  
  def splitIntoFilesByLength(filename: String, outdir: String) = {
    var fileByLen = mutable.Map[Int, PrintWriter]()
    var i = 0
    for(line <- Source.fromFile(filename).getLines()) {
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
    for(file <- fileByLen.values) {
      file.close()
    }
  }

  private def firstPass = {
    var dataMap = mutable.Map[Int, Array[String]]()
    var prefixMap = mutable.Map[String, List[Int]]().withDefaultValue(List())
    var postfixMap = mutable.Map[String, List[Int]]().withDefaultValue(List())
    var i = 0
    for(line <- Source.fromFile("F:/tmp/mmds/00041.txt").getLines()) {
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
    
    val preMap = prefixMap.filter(p => p._2.size > 1)
    val postMap = postfixMap.filter(p => p._2.size > 1)
    
    var resCount = 0
    var compCount = 0
    for (id <- dataMap.keys) {
      val sentence = dataMap(id)
      val prefix = sentence.take(KEYSIZE).mkString(" ")
      val postfix = sentence.takeRight(KEYSIZE).mkString(" ")
      val set = (List() ++ preMap(prefix) ++ postMap(postfix)).distinct.sorted
      for (s1 <- set; if (id > s1)) {
        compCount += 1
        if (hasEditDistanceLE1(dataMap(id), dataMap(s1))) resCount += 1
      }
    }
    println("\n========================\n" + resCount + " (comparisons: " + compCount + " - ratio: " + resCount.toFloat / compCount + ")")
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
        if (lo.take(i) ++ lo.drop(i+1) == sh) return true
      }
      return false
    } else {
      var c = 0;
      for(i <- 0 to s1.size - 1) {
        if (s1(i) == s2(i)) c += 1
      }
      return s1.size - c <= 1
    }
  }  
}