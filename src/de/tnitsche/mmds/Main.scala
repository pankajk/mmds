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
    
    //val lenList = splitIntoFilesByLength(FILE, OUTDIR)
    val lenList = (10 to 5632).toList // no need to split the file again and again
    loop(lenList)
    
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
  }

  /**
   * Splits the input file into lots of files grouped by sentence length.
   * I was surprised that it is possible to have >1000 files open in parallel for writing... ;)
   */
  def splitIntoFilesByLength(infile: String, outdir: String): List[Int] = {
    var fileByLen = mutable.Map[Int, PrintWriter]()
    for (line <- Source.fromFile(infile).getLines()) {
      val len = line.split(" ").size - 1
      if (!fileByLen.contains(len)) {
        val file = new PrintWriter(fileNameForLength(len))
        fileByLen.put(len, file)
      }
      fileByLen(len).println(line)
    }
    for (file <- fileByLen.values) {
      file.close()
    }
    (List[Int]() ++ fileByLen.keys).sorted
  }

  /**
   * The main loop takes the list of sentences lengths and processes the matching files.
   */
  private def loop(sLenList: List[Int]) = {
    var prevPreMap = mutable.Map[Int, List[Int]]()
    var prevPostMap = mutable.Map[Int, List[Int]]()
    var prevDataMap = mutable.Map[Int,Array[String]]()
    var result = 0
    for (sLen <- sLenList) {
      println(sLen + ":")
      val (dataMap, preMap, postMap) = readAndIndex(sLen)
      result += checkCandidates(dataMap, preMap, postMap, prevDataMap, prevPreMap, prevPostMap)._1
      prevPreMap = preMap
      prevPostMap = postMap
      prevDataMap = dataMap
    }
    println("\n========================\n Result:" + result)
  }

  /**
   * Loops over the sentences with a certain length and checks the candidates found in the HashMaps
   * with length-1 and same length for sentences with edit distance 1.
   */
  private def checkCandidates(dataMap: scala.collection.mutable.Map[Int, Array[String]] , 
      preMap: mutable.Map[Int, List[Int]], 
      postMap: mutable.Map[Int, List[Int]], 
      prevDataMap: mutable.Map[Int,Array[String]], 
      prevPreMap: mutable.Map[Int, List[Int]], 
      prevPostMap: mutable.Map[Int, List[Int]]): (Int, Int) = {
    val t0 = System.nanoTime()
    var resCount = 0
    var compCount = 0
    for (id <- dataMap.keys) {
      val sentence = dataMap(id)
      val (prefix, postfix) = createPrefixAndPostfix(sentence)
      val (cc, rc) = checkShorter(prevDataMap, prevPreMap, prevPostMap, sentence, id, prefix, postfix)
      val (cc2, rc2) = checkEqualLength(dataMap, preMap, postMap, sentence, id, prefix, postfix)
      compCount += cc + cc2
      resCount += rc + rc2
    }
    val t1 = System.nanoTime()
    println("check candidates: " + (t1 - t0) / 1000000 + "ms")
    println("= " + resCount + " (comparisons: " + compCount + " - ratio: " + resCount.toFloat / compCount + ")\n")
    return (resCount, compCount)
  }

  /**
   * Checks a single sentence against candidates with same length. Candidates are sentences which have the same 5-word
   * prefix (preMap) or postfix (postMap).
   */
  private def checkEqualLength(dataMap: mutable.Map[Int, Array[String]],
		  preMap: mutable.Map[Int, List[Int]],
		  postMap: mutable.Map[Int, List[Int]],
		  sentence: Array[String], id: Int, prefix: Int, postfix: Int): (Int, Int) = {
    val sameLengthSentenceSet = (preMap(prefix) ++ postMap(postfix)).filter(p => p > id).distinct
    return (sameLengthSentenceSet.size, sameLengthSentenceSet.count(key => hasEditDistanceLE1(sentence, dataMap(key))))
  }

  /**
   * Checks a single sentence against candidates with length-1. Candidates are sentences which have the same 5-word
   * prefix (prevPreMap) or postfix (prevPostMap).
   */
  private def checkShorter(prevDataMap: mutable.Map[Int, Array[String]],
		  prevPreMap: mutable.Map[Int, List[Int]],
		  prevPostMap: mutable.Map[Int, List[Int]],
		  sentence: Array[String], id: Int, prefix: Int, postfix: Int): (Int, Int) = {
    val shorterSentenceSet = (prevPreMap.getOrElse(prefix, List()) ++ prevPostMap.getOrElse(postfix, List())).distinct
    return (shorterSentenceSet.size, shorterSentenceSet.count(key => hasEditDistanceLE1(sentence, prevDataMap(key))))
  }

  /**
   * Reads one of the files by sentence length, creates and hashes them by prefix and postfix.
   */
  private def readAndIndex(sLength: Int): (mutable.Map[Int, Array[String]], 
	      mutable.Map[Int, List[Int]], 
	      mutable.Map[Int, List[Int]]) = {
    val t0 = System.nanoTime()
    var dataMap = mutable.Map[Int, Array[String]]()
    var prefixMap = mutable.Map[Int, List[Int]]().withDefaultValue(List())
    var postfixMap = mutable.Map[Int, List[Int]]().withDefaultValue(List())
    val fileName = fileNameForLength(sLength)
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

  /**
   * The method that actually checks if edit distance between to sentences is 0 or 1.
   */
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
  
  /**
   * Creates prefix and postfix hash keys from a sentence. 
   */
  private def createPrefixAndPostfix(sentence: Array[String]): (Int, Int) = {
    val prefix = sentence.take(KEYSIZE).mkString(" ").hashCode()
    val postfix = sentence.takeRight(KEYSIZE).mkString(" ").hashCode()
    (prefix, postfix)
  }
  
  /**
   * Helper function to format the filename.
   */
  private def fileNameForLength(len: Int): String = {
    OUTDIR + "%05d".format(len) + ".txt"
  }
}