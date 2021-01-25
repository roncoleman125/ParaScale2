/*
 Copyright (c) Ron Coleman

 Permission is hereby granted, free of charge, to any person obtaining
 a copy of this software and associated documentation files (the
 "Software"), to deal in the Software without restriction, including
 without limitation the rights to use, copy, modify, merge, publish,
 distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to
 the following conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package parascale.mapreduce

import parascale.future.perfect.{ask, candidates, _sumOfFactorsInRange}

object MrFuturePerfectNumberFinder extends App {
  (0 until candidates.length).foreach { index =>
    val num = candidates(index)
    println(num + " is perfect? "+ ask(isPerfectConcurrent,num))
  }

  def isPerfectConcurrent(candidate: Long): Boolean = {
    val RANGE = 1000000L

    val numberOfPartitions = (candidate.toDouble / RANGE).ceil.toInt

    val ranges = for (i <- 0L until numberOfPartitions) yield {
      val lower: Long = i * RANGE + 1

      val upper: Long = candidate min (i + 1) * RANGE

      (lower, upper)
    }

    import scala.concurrent.{Await, Future}
    import scala.concurrent.ExecutionContext.Implicits.global
    val futurePartialSums = ranges.map { lowerUpper =>
      Future {
        val (lower, upper) = lowerUpper

        _sumOfFactorsInRange(lower, upper, candidate)
      }
    }

    val total = futurePartialSums.foldLeft(List[Long]()) { (list, future) =>
      import scala.concurrent.duration._
      val partialSum = Await.result(future, 100 seconds)

      list ++ List(partialSum)
    }.reduce { (a, b) => a+b }

    2 * candidate == total
  }
}
