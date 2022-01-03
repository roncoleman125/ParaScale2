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
package parascale.future.perfect

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * This object evaluates a list of candidates as perfect numbers.
 * @author Ron.Coleman
 */
object FuturePerfectNumberFinder extends App {
  (0 until candidates.length).foreach { index =>
    val num = candidates(index)
    println(num + " is perfect? "+ ask(isPerfectConcurrent,num))
  }

  /**
    * Uses concurrency to determine true if the candidate is perfect.
    * @param candidate Candidate number
    * @return True if candidate is perfect, false otherwise
    */
  def isPerfectConcurrent(candidate: Long): Boolean = {
    val RANGE = 1000000L

    // Take  ceiling as this gets the last partial partition.
    val numPartitions = (candidate.toDouble / RANGE).ceil.toInt

    // Each future gets a partition with its lower and upper range
    val futures = for(k <- 0L until numPartitions) yield Future {
      val lower: Long = k * RANGE + 1
      val upper: Long = candidate min (k + 1) * RANGE

      // This method returns the SOF which is the promise
      sumOfFactorsInRange(lower, upper, candidate)
    }

    // Wait for the children futures
    val total = futures.foldLeft(0L) { (sum, future) =>
      // Await waits for the future and unwraps the result
      import scala.concurrent.duration._
      val result = Await.result(future, Duration.Inf)

      // Add the partial sum into the total sum
      sum + result
    }

    // If we get here all the children futures have completed
    (2 * candidate) == total
  }



  /**
    * Computes the sum of factors in a range using a loop which is robust for large numbers.
    * @param lower Lower part of range
    * @param upper Upper part of range
    * @param candidate Number
    * @return Sum of factors
    */
  def sumOfFactorsInRange(lower: Long, upper: Long, candidate: Long): Long = {
    val sum = (lower to upper).foldLeft(0L) { (sum, index) =>
      if (candidate % index == 0L)
        sum + index
      else
        sum
    }
    sum
  }
}


