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
import parabond.util.{Helper, Job, MongoHelper, Result}
import parabond.value.SimpleBondValuator
import parabond.util.Constant.PORTF_NUM
import parascale.util.getPropertyOrElse

import scala.util.Random

/** Test driver */
object Ser02 {
  def main(args: Array[String]): Unit = {
    (new Ser02).test
  }
}

/**
 * This class implements the composite serial algorithm.
 * @author Ron Coleman, Ph.D.
 */
class Ser02 {
  /** Initialize the random number generator */
  val ran = new Random(0)   

  /** Runs unit test */
  def test {
    // Set the number of portfolios to analyze
    val arg = System.getProperty("n")
    
    val n = if(arg == null) PORTF_NUM else arg.toInt
    
    val me =  this.getClass().getSimpleName()
    val outFile = me + "-dat.txt"
    
    val fos = new java.io.FileOutputStream(outFile,true)
    val os = new java.io.PrintStream(fos)
    
    os.print(me+" "+ "N: "+n+" ")

    val details = getPropertyOrElse("details", false)
    
    // Build the portfolio list    
    val jobs = for(i <- 0 until n) yield Job(ran.nextInt(100000)+1,null,null)
    
    val list = jobs.toList
   
    // Parallel map the input
    val now = System.nanoTime  
    
    val outputs = jobs.map(price)
    
    val t1 = System.nanoTime
    
    // Generate the detailed output report
    if(details) {
    	println("%6s %10.10s %-5s %-2s".format("PortId","Price","Bonds","dt"))
    	
      outputs.foreach { output =>
        val id = output.portfId

        val dt = (output.result.t1 - output.result.t0) / 1000000000.0

        val bondCount = output.result.bondCount

        val price = output.result.value

        println("%6d %10.2f %5d %6.4f %12d %12d".format(id, price, bondCount, dt, output.result.t1 - now, output.result.t0 - now))
      }
    }
    
    // Compute the "inner" time
    val dt1 = outputs.foldLeft(0.0) { (sum, output) =>
      sum + (output.result.t1 - output.result.t0)

    } / 1000000000.0
    
    // Compute the "outer" time
    val dtN = (t1 - now) / 1000000000.0

    val speedup = dt1 / dtN

    val numCores = Runtime.getRuntime().availableProcessors()

    val e = speedup / numCores
    
    os.println("dt(1): %7.4f  dt(N): %7.4f  cores: %d  R: %5.2f  e: %5.2f ".
        format(dt1,dtN,numCores,speedup,e))  
    
    os.flush
    
    os.close
    
    println(me+" DONE! %d %7.4f".format(n,dtN))      
  }
   
  /**
   * Prices a portfolio using the "basic" algorithm.
   */
  def price(portf: Job): Job = {
    // Value each bond in the portfolio
    val t0 = System.nanoTime

    // Retrieve the portfolio
    val portfId = portf.portfId
    
    val portfsQuery = MongoDbObject("id" -> portfId)

    val portfsCursor = MongoHelper.portfolioCollection.find(portfsQuery)
    
    // Get the bonds ids in the portfolio
    val bondIds = MongoHelper.asList(portfsCursor,"instruments")
    
    // Price each bond and sum all the prices
    val value = bondIds.foldLeft(0.0) { (sum, id) =>
      // Get the bond from the bond collection
      val bondQuery = MongoDbObject("id" -> id)

      val bondCursor = MongoHelper.bondCollection.find(bondQuery)

      val bond = MongoHelper.asBond(bondCursor)
      
      // Price the bond
      val valuator = new SimpleBondValuator(bond, Helper.yieldCurve)

      val price = valuator.price
      
      // The price into the aggregate sum
      sum + price
    } 
    
    // Update the portfolio price    
    MongoHelper.updatePrice(portfId,value)  
  
    val t1 = System.nanoTime
    
    Job(portfId,null,Result(portfId,value,bondIds.size,t0,t1))
  }
}