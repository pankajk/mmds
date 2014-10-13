package de.tnitsche.mmds

import scala.io.Source
import scala.collection.mutable
import java.util.Arrays
import java.io.PrintWriter
import java.util.zip.GZIPOutputStream
import java.io.FileOutputStream
import java.io.OutputStreamWriter

object Main {
  //var sMap = mutable.Map[Int, mutable.MutableList[Int]]().withDefaultValue(mutable.MutableList[Int]())
  val HMS = 10000019;
  val PRIMES = Array(2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541,
    547, 557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021, 1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069, 1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163, 1171, 1181, 1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291, 1297, 1301, 1303, 1307, 1319, 1321, 1327, 1361, 1367, 1373, 1381, 1399, 1409, 1423, 1427, 1429, 1433, 1439, 1447, 1451, 1453, 1459, 1471, 1481, 1483, 1487, 1489, 1493, 1499, 1511, 1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571, 1579, 1583, 1597, 1601, 1607, 1609, 1613, 1619, 1621, 1627, 1637, 1657, 1663, 1667, 1669, 1693, 1697, 1699, 1709, 1721, 1723, 1733, 1741, 1747, 1753, 1759, 1777, 1783, 1787, 1789, 1801, 1811, 1823, 1831, 1847, 1861, 1867, 1871, 1873, 1877, 1879, 1889, 1901, 1907, 1913, 1931, 1933, 1949, 1951, 1973, 1979, 1987, 1993, 1997, 1999, 2003, 2011, 2017, 2027, 2029, 2039, 2053, 2063, 2069, 2081, 2083, 2087, 2089, 2099, 2111, 2113, 2129, 2131, 2137, 2141, 2143, 2153, 2161, 2179, 2203, 2207, 2213, 2221, 2237, 2239, 2243, 2251, 2267, 2269, 2273, 2281, 2287, 2293, 2297, 2309, 2311, 2333, 2339, 2341, 2347, 2351, 2357, 2371, 2377, 2381, 2383, 2389, 2393, 2399, 2411, 2417, 2423, 2437, 2441, 2447, 2459, 2467, 2473, 2477, 2503, 2521, 2531, 2539, 2543, 2549, 2551, 2557, 2579, 2591, 2593, 2609, 2617, 2621, 2633, 2647, 2657, 2659, 2663, 2671, 2677, 2683, 2687)

  val FILE = "c:/tmp/sentences.txt"
  val OUTDIR = "f:/tmp/mmds/"
  val KEYSIZE = 5

  def main(args: Array[String]): Unit = {
    val t0 = System.nanoTime()
    firstPass
    //val sMap = firstPass
    //splitIntoFilesByLength(FILE, OUTDIR)
    //val sMap = firstPass
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

  def countByLen() : mutable.Map[Int, Int] = {
    var byLen = mutable.Map[Int, Int]().withDefaultValue(0)
    var i = 0
    for(line <- Source.fromFile("F:/tmp/mmds/00010.txt").getLines()) {
      i += 1
      if (i % 100000 == 0) println(i + " " + Runtime.getRuntime().freeMemory())
      val len = line.split(" ").size - 1
      byLen(len) += 1
    }
    println(byLen.toList.sortBy(a => a._1))
    return byLen
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
      // println(words.tail)
      dataMap(id) = words
      prefixMap(words.take(KEYSIZE).mkString(" ")) = prefixMap(words.take(KEYSIZE).mkString(" ")) :+ id
      postfixMap(words.takeRight(KEYSIZE).mkString(" ")) = postfixMap(words.takeRight(KEYSIZE).mkString(" ")) :+ id
    }    
    
    println(prefixMap.size + " " + postfixMap.size)
    val preMap = prefixMap.filter(p => p._2.size > 1)
    val postMap = postfixMap.filter(p => p._2.size > 1)
    println(preMap.map(p => p._2.size).max)
    println(postMap.map(p => p._2.size).max)
    println(preMap.size + " " + postMap.size)
    
    var resCount = 0
    for (id <- dataMap.keys) {
      val sentence = dataMap(id)
      val set = Set() ++ preMap(sentence.take(KEYSIZE).mkString(" ")) ++ postMap(sentence.takeRight(KEYSIZE).mkString(" "))
      for (s1 <- set) {
        if (id < s1) {
          val b = hasEditDistanceLE1(dataMap(id), dataMap(s1))
          if (b) resCount += 1
        }
      }
    }
    println("\n========================\n" + resCount)
  }

  def hasEditDistanceLE1(s1: Array[String], s2: Array[String]): Boolean = {
    if (s1.size != s2.size) {
      var sh = s1.toList
      var lo = s2.toList
      if (s1.size > s2.size) {
        sh = s2.toList
        lo = s1.toList
      }
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