package parascale.future.basic

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Demonstrates how to launch a future, wait for it, and get the result.
  * See https://alvinalexander.com/scala/concurrency-with-scala-futures-tutorials-examples
  */
object Example1 extends App {

  // 2 - create a Future
  val f = Future {
    sleep(500)
    1 + 1
  }

  val result = Await.result(f, 1 second)
  println(result)
  sleep(1000)
}
