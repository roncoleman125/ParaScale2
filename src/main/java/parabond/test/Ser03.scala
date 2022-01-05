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

import parabond.util.{Helper, Job, MongoHelper, Result}
import parabond.value.SimpleBondValuator
import parascale.util.getPropertyOrElse

import scala.util.Random

/** Test driver */
object Ser03 {
  def main(args: Array[String]): Unit = {
    (new Ser03).test
  }
}

/**
 * This class implements the memory-bound serial algorithm.
 * @author Ron Coleman, Ph.D.
 */
class Ser03 {
  /** Number of bond portfolios to analyze */
  val PORTF_NUM = 100
  
  /** Initialize the random number generator */
  val ran = new Random(0)   
  
  /** Write a detailed report */
  val details = false
  
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
    
    // Load all the bonds into into memory
    // Note: the input is a list of Data instances, each element of which contains a list
    // of bonds
    val t2 = System.nanoTime
    val jobs = loadJobs(n)
    val t3 = System.nanoTime

    // Process the data
    val now = System.nanoTime
    val outputs = jobs.map(priced)
    val t1 = System.nanoTime

    // Generate the output report
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

    val dt1 = outputs.foldLeft(0.0) { (sum,result) =>
      sum + (result.result.t1 - result.result.t0)

    } / 1000000000.0

    val dtN = (t1 - now) / 1000000000.0

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

  /**
   * Prices a portfolio assuming all the bonds for a portfolio are already loaded
   * into memory.
   */
  def priced(input: Job): Job = {

    // Value each bond in the portfolio
    val t0 = System.nanoTime

    val value = input.bonds.foldLeft(0.0) { (sum, bond) =>
      // Price the bond
      val valuator = new SimpleBondValuator(bond, Helper.yieldCurve)

      val price = valuator.price

      // The price into the aggregate sum
      sum + price
    }

    // Update the portfolio price
    MongoHelper.updatePrice(input.portfId,value)

    val t1 = System.nanoTime

    // Return the result for this portfolio
    new Job(input.portfId,null,Result(input.portfId,value,input.bonds.size,t0,t1))
  }

  /**
   * Parallel load the jobs with embedded bonds.
   * @param n Number of portfolios to load
   */
  def loadJobs(n: Int): List[Job] = {
    val portfIds = for(i <- 0 until n) yield ran.nextInt(100000)+1
    
    val list = portfIds.foldLeft (List[Job]()) { (jobs, portfId) =>
      val intermediate = MongoHelper.fetchBonds(portfId)
      
      new Job(portfId,intermediate.bonds,null) :: jobs
    }
    
    list
  }  
}