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

import parabond.mr.MapReduce
import parabond.util.{MongoHelper, Result}
import parabond.mr._
import parascale.util.{getPropertyOrElse, parseBoolean}

/** Test driver */
object Mr03 {
  def main(args: Array[String]): Unit = {
    (new Mr03).test
  }
}

/**
  * This class runs a memory-bound mapreduce for arbitrary number of portfolios in the parabond database.
  * Namely, it tries to parallelize a group of portfolios on a single core.
  * @author Ron Coleman
  */
class Mr03 {
  /** Number of bond portfolios to analyze */
  val PORTF_NUM = 100

  /** Write a detailed report */
  val details = true

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

    val t2 = System.nanoTime
    val input = MongoHelper.loadPortfsParallel(n)
    val t3 = System.nanoTime

    // Map-reduce the input
    val t0 = System.nanoTime
    val resultsUnsorted = MapReduce.memorybound(input, mapping, reducing)
    val t1 = System.nanoTime

    // Generate the output report
    if (details)
      println("%6s %10.10s %-5s %-2s".format("PortId", "Price", "Bonds", "dt"))

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

    val dt1 = results.foldLeft(0.0) { (sum, result) =>
      sum + (result.t1 - result.t0)

    } / 1000000000.0

    val dtN = (t1 - t0) / 1000000000.0

    val speedup = dt1 / dtN

    val numCores = Runtime.getRuntime().availableProcessors()

    val e = speedup / numCores

    os.print("dt(1): %7.4f  dt(N): %7.4f  cores: %d  R: %5.2f  e: %5.2f ".
        format(dt1,dtN,numCores,speedup,e))     
    
    os.println("load t: %8.4f ".format((t3-t2)/1000000000.0))   
    
    os.flush
    
    os.close
    
    println(me+" DONE! %d %7.4f".format(n,dtN))   
  }
}