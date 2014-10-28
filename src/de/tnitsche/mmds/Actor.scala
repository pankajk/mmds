package de.tnitsche.mmds

import akka.actor.Actor
import java.util.concurrent.CountDownLatch
import akka.routing.ActorRefRoutee
import akka.routing.Router
import akka.routing.RoundRobinRoutingLogic
import akka.actor.Props

sealed trait CompMessage
 
case object Compare extends CompMessage
 
case class Work(tested: Array[String], candidates: Array[Array[String]]) extends CompMessage
 
case class Result(count: Int) extends CompMessage

/**
 *  TODO: make it work! :D
 */
class Worker extends Actor {
  def receive = {
    case Work(tested, candidates) =>
      // self.reply Result(checkSentence(tested, candidates)) // perform the work
  }
  
  def checkSentence(tested: Array[String], candidates: Array[Array[String]]): Int = {
    candidates.count(cand => hasEditDistanceLE1(tested,cand))
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

class Master(
  nrOfWorkers: Int, nrOfMessages: Int, nrOfElements: Int, latch: CountDownLatch)
  extends Actor {
 
  var pi: Double = _
  var nrOfResults: Int = _
  var start: Long = _
 
  var router = {
    val routees = Vector.fill(5) {
      val r = context.actorOf(Props[Worker])
      context watch r
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }
  
  def receive = { 
    case Work(tested, candidates) => 
    
  }
 
  override def preStart() {
    start = System.currentTimeMillis
  }
 
  override def postStop() {
    // tell the world that the calculation is complete
    latch.countDown()
  }
}