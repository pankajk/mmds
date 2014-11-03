package de.tnitsche.mmds

import scala.io.Source
import scala.collection.mutable
import java.util.Arrays
import java.io.PrintWriter
import java.util.zip.GZIPOutputStream
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.file.{Paths, Files}
import akka.actor.Actor
import akka.actor.PoisonPill
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.Future
import akka.routing.Broadcast
import scala.concurrent.ExecutionContext.Implicits.global
import akka.routing.SmallestMailboxRouter

object Main {
  val FILE = "c:/tmp/sentences.txt"
  val OUTDIR = "f:/tmp/mmds/"
  val KEYSIZE = 5
  val BATCH_SIZE = 80  // optimum is much lower than I expected
  implicit val timeout = Timeout(60000)

  val system = ActorSystem("SimpleSystem")
  val router = system.actorOf(Props[CompareWorker].withRouter(
    SmallestMailboxRouter(nrOfInstances = 4)), name = "router")
  
  def main(args: Array[String]): Unit = {
    val t0 = System.nanoTime()

    //val lenList = splitIntoFilesByLength(FILE, OUTDIR)
    val lenList = (10 to 5632).toList // no need to split the file again and again
    loop(lenList)

    val t1 = System.nanoTime()
    router ! Broadcast(PoisonPill)
    system.shutdown // kill all background threads
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
    var compCount = 0
    var futures: mutable.ArrayBuffer[Future[Result]] = mutable.ArrayBuffer()
    var currentBatch = List[Work]()
    for (id <- dataMap.keys) {
      val sentence = dataMap(id)
      val (prefix, postfix) = createPrefixAndPostfix(sentence)
      val sameLengthSentenceSet = (preMap(prefix) ++ postMap(postfix)).filter(p => p > id).distinct
      val shorterSentenceSet = (prevPreMap.getOrElse(prefix, List()) ++ prevPostMap.getOrElse(postfix, List())).distinct
      compCount += sameLengthSentenceSet.size + shorterSentenceSet.size
      currentBatch :+= Work(sentence, sameLengthSentenceSet.map(i => dataMap(i)))
      currentBatch :+= Work(sentence, shorterSentenceSet.map(i => prevDataMap(i)))
      if (currentBatch.size > BATCH_SIZE) {
        futures :+= checkWithActor(currentBatch)
        currentBatch = List[Work]()
      }
    }
    futures :+= checkWithActor(currentBatch) // last batch (< BATCH_SIZE)
    
    prevDataMap.clear // heap space is an issue, free it early
    prevPreMap.clear
    prevPostMap.clear
    
    val resultFuture: Future[List[Result]] = Future.sequence(futures.toList)
    val results = Await.result(resultFuture, timeout.duration).asInstanceOf[List[Result]]
    var resCount = results.map(r => r.count).sum
    val t1 = System.nanoTime()
    println("check candidates: " + (t1 - t0) / 1000000 + "ms")
    println("= " + resCount + " (comparisons: " + compCount + " - ratio: " + resCount.toFloat / compCount + ")\n")
    return (resCount, compCount)
  }

  /**
   * Routes the Work to the Actor system, using the CompareWorker
   */
  private def checkWithActor(work: List[Work]): Future[Result] = {
    ask(router, Batch(work)).mapTo[Result]
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
  
  /**
   * The Worker does the actual comparisons of a sentence against the match candidates
   */
  class CompareWorker extends Actor {
    /**
     * Actor receive
     */
    def receive = {
      case Batch(work) => 
      		sender ! Result(checkBatch(work)) // perform the work
      case PoisonPill               => context.stop(self)
    }
    
    /**
     * Checks a batch of sentences
     */
    def checkBatch(batch: List[Work]): Int = {
      batch.map(b => checkSentence(b.tested, b.candidates)).sum
    }

    /**
     * Checkes a sentence against a List of candidates
     */
    def checkSentence(tested: Array[String], candidates: List[Array[String]]): Int = {
      candidates.count(cand => hasEditDistanceLE1(tested, cand))
    }

    /**
     * Checks two sentences for edit distance <= 1.
     * Is called *very* often and can likely be optimized further. 
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
  }

  sealed trait CompMessage

  case object Compare extends CompMessage

  case class Batch(batch: List[Work]) extends CompMessage
  
  case class Result(count: Int) extends CompMessage
  
  case class Work(tested: Array[String], candidates: List[Array[String]])

}
