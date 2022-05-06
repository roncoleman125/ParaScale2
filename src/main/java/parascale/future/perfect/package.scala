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
package parascale.future

package object perfect {
  /**
    * Candidate perfect numbers
    * See https://en.wikipedia.org/wiki/List_of_perfect_numbers
    */
  val candidates: List[Long] =
    List(
      6,
      7,                    // Negative test
      28,
      30,                   // Negative test
      496,
      8128,
      33550336,
      33550336+1,           // Negative test
      8589869056L+1,        // Negative test
      8589869056L,
      137438691328L,
      2305843008139952128L  // May take est. 170 years to prove.
    )

  /**
    * Queries a number.
    * @param method Method to invoke
    * @param number Number to query
    * @return True if the method is true, false otherwise
    */
  def ask(method: Long => Boolean, number: Long): String = {
    val t0 = System.nanoTime

    val isTrue = method(number)

    val t1 = System.nanoTime

    val answer = if(isTrue) "YES" else "NO"

    answer + " dt = "+(t1-t0)/1000000000.0 + "s"
  }

  /**
    * Sums the factors of a number using a range which may explode for large numbers.
    * @param number Number
    * @return sum of factors
    */
  def sumOfFactors(number: Long) = {
    (1L to number).foldLeft(0L) { (sum, i) => if (number % i == 0L) sum + i else sum }
  }

//  /**
//    * Sums the factors using a loop which is more robust for large numbers.
//    * @param number Number
//    * @return sum of factors
//    */
//  def _sumOfFactors(number: Long) = {
//    var index = 1L
//    var sum = 0L
//    while(index <= number.toLong) {
//      if(number % index == 0)
//        sum += index
//      index += 1
//    }
//    sum
//  }

  /**
    * Computes the sum of factors in a range using a range which may fail for large numbers.
    * @param lower Lower part of range
    * @param upper Upper part of range
    * @param number Number
    * @return Sum of factors
    */
  def sumOfFactorsInRange(lower: Long, upper: Long, number: Long): Long = {
    Range.Long(lower, upper, 1).foldLeft(0L) { (sum, i) => if (number % i == 0) sum + i else sum }
  }

  /**
    * Computes the sum of factors in a range using a loop which is robust for large numbers.
    *
    * @param lower  Lower part of range
    * @param upper  Upper part of range
    * @param number Number
    * @return Sum of factors
    */
  def _sumOfFactorsInRange(lower: Long, upper: Long, number: Long): Long = {
    var index: Long = lower

    var sum = 0L

    while (index <= upper) {
      if (number % index == 0L)
        sum += index

      index += 1L
    }

    sum
  }
}
