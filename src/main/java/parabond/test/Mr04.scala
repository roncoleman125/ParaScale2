/*
 * Copyright (c) Ron Coleman
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Scaly Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package parabond.test

import parabond.casa.MongoDbObject
import parabond.util.{MongoHelper, Result}
import parabond.mr._
import parabond.util.Constant._

import scala.util.Random
import parabond.value.SimpleBondValuator
import parascale.util.{getPropertyOrElse, parseBoolean}

/** Test driver */
object Mr04 {
  def main(args: Array[String]): Unit = {
    (new Mr04).test
  }
}

/**
  * This class runs a fine-grain mapreduce for arbitrary number of portfolios in the parabond database.
  * Namely, it tries to parallelize single bonds.
  * @author Ron Coleman
  */
class Mr04 {
  /** Initialize the random number generator */
  val ran = new Random(0)

  /** Write a detailed report */
  val details = true

  val numCores = Runtime.getRuntime().availableProcessors()

  /** Unit test entry point */
  def test {
    // Set the number of portfolios to analyze
    val n = getPropertyOrElse("n",PORTF_NUM)

    val me =  this.getClass().getSimpleName()
    val outFile = me + "-dat.txt"

    val fos = new java.io.FileOutputStream(outFile,true)
    val os = new java.io.PrintStream(fos)

    os.print(me+" "+ "N: "+n+" ")

    val details = getPropertyOrElse("details",parseBoolean,false)

    // Build the portfolio list
    // Connect to the portfolio collection
    val t2 = System.nanoTime

    val portfsCollecton = mongo("Portfolios")

    val inputs = (1 to n).foldLeft(List[(Int,Int)]()) { (list, p) =>
      val portfId = ran.nextInt(100000)+1

      // Retrieve the portfolio
      val portfsQuery = MongoDbObject("id" -> portfId)

      val portfsCursor = portfsCollecton.find(portfsQuery)

      // Get the bonds in the portfolio
      val bondIds = MongoHelper.asList(portfsCursor, "instruments")

      val pairs = bondIds.foldLeft(List[(Int,Int)]()) { (sum, bondId) =>
        sum ::: List((portfId,bondId))
      }

      list ++ pairs
    }

    val t3 = System.nanoTime

    // Map-reduce the input
    val t0 = System.nanoTime
    val resultsUnsorted = mapreduceFine(inputs, mapping, reducing)
    val t1 = System.nanoTime

    // Generate the output report
    if(details)
    	println("%6s %10.10s %-5s %-2s".format("PortId","Price","Bonds","dt"))

    val list = resultsUnsorted.foldLeft(List[Result]()) { (list, rsult) =>
      val (portfId, result) = rsult

      list ++ List(result)
    }

    val results = list.sortWith(_.t0 < _.t0)

    if (details)
      results.foreach { result =>
        val id = result.portfId

        val dt = (result.t1 - result.t0) / 1000000000.0

        val bondCount = result.bondCount

        val price = result.value

        println("%6d %10.2f %5d %6.4f %12d %12d".format(id, price, bondCount, dt, result.t1 - t0, result.t0 - t0))
      }

    val dt1 = results.foldLeft(0.0) { (sum,result) =>
      sum + (result.t1 - result.t0)

    } / 1000000000.0

    val dtN = (t3 - t2 + t1 - t0) / 1000000000.0

    val speedup = dt1 / dtN

    val e = speedup / numCores

    os.println("dt(1): %7.4f  dt(N): %7.4f  cores: %d  R: %5.2f  e: %5.2f ".
        format(dt1,dtN,numCores,speedup,e))

    os.flush

    os.close

    println(me+" DONE! %d %7.4f".format(n,dtN))
  }
}