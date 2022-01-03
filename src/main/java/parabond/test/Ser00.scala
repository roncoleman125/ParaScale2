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
import parabond.util.{Helper, MongoHelper, Result}

import scala.util.Random
import parabond.value.SimpleBondValuator
import parascale.util._
import parabond.util.Constant.{DIAGS_DIR, PORTF_NUM}

/** Test driver */
object Ser00 {
  def main(args: Array[String]): Unit = {
    (new Ser00).test
  }
}

/**
 * This class implements the composite serial algorithm.
 * @author Ron Coleman, Ph.D.
 */
class Ser00 {
  /** Initialize the random number generator */
  val ran = new Random(0)  

  def test {  
    // Set the number of portfolios to analyze
    val n = getPropertyOrElse("n",PORTF_NUM)

    val me =  this.getClass().getSimpleName()

    val dir = getPropertyOrElse("dir",DIAGS_DIR)

    val outFile = dir + me + "-dat.txt"
    
    val fos = new java.io.FileOutputStream(outFile,true)
    val os = new java.io.PrintStream(fos)

    redirectErr

    os.print(me+" "+ "N: "+n+" ")    
    
    val details = if(System.getProperty("details") != null) true else false
    
    val portfIds = (1 to n).foldLeft(List[Int]()) { (list, p) =>
      val r = ran.nextInt(100000)+1
      list ::: List(r)
    }    
    
    val t0 = System.nanoTime
    
    val results = portfIds.foldLeft(List[Result]()) { (sum, portfId) =>
      // Value each bond in the portfolio
      val t0 = System.nanoTime

      // Retrieve the portfolio
      val portfsQuery = MongoDbObject("id" -> portfId)

      val portfsCursor = MongoHelper.portfolioCollection.find(portfsQuery)

      // Get the bonds in the portfolio
      val bondIds = MongoHelper.asList(portfsCursor, "instruments")

      val value = bondIds.foldLeft(0.0) { (sum, id) =>
        // Get the bond from the bond collection
        val bondQuery = MongoDbObject("id" -> id)

        val bondCursor = MongoHelper.bondCollection.find(bondQuery)

        val bond = MongoHelper.asBond(bondCursor)

        // Price the bond
        val valuator = new SimpleBondValuator(bond, Helper.yieldCurve)

        val price = valuator.price

        // Add the price into the aggregate sum
        sum + price
      }
      
      MongoHelper.updatePrice(portfId, value)
      
      val t1 = System.nanoTime
      
      Result(portfId,value,bondIds.size,t0,t1) :: sum     
    }
    
    val t1 = System.nanoTime
  
    // Generate the output report
    if (details) {
      println("%6s %10.10s %-5s %-2s".format("PortId", "Price", "Bonds", "dt"))

      results.reverse.foreach { result =>
        val id = result.portfId

        val dt = (result.t1 - result.t0) / 1000000000.0

        val bondCount = result.bondCount

        val price = result.value

        println("%6d %10.2f %5d %6.4f %12d %12d".format(id, price, bondCount, dt, result.t1 - t0, result.t0 - t0))
      }
    }
    
    val dt1 = results.foldLeft(0.0) { (sum,result) =>      
      sum + (result.t1 - result.t0)
      
    } / 1000000000.0
    
    
    val dtN = (t1 - t0) / 1000000000.0
    
    val speedup = dt1 / dtN
    
    val numCores = Runtime.getRuntime().availableProcessors()
    
    val e = 1.0
    
    os.println("dt(1): %7.4f  dt(N): %7.4f  cores: %d  R: %5.2f  e: %5.2f ".
        format(dt1,dtN,numCores,speedup,e))  
    
    os.flush
    
    os.close
    
    println(me+" DONE! %d %7.4f".format(n,dtN))    
  }
}