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
import scala.util.Random
import parabond.entry.SimpleBond
import parascale.util._
import parabond.util.Constant.{DIAGS_DIR, PORTF_NUM}
import scala.collection.parallel.CollectionConverters._

/** Test driver */
object Par06 {
  def main(args: Array[String]): Unit = {
    new Par06 test
  }
}

/**
 * This class uses parallel collections to price n portfolios in the
 * parabond database using the fine-grain memory-bound algorithm.
 * @author Ron Coleman, Ph.D.
 */
class Par06 {
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
    
    os.print(me+" "+ "N: "+n+" ")

    val details = getPropertyOrElse("details",parseBoolean,false)
    
    // Load all the bonds into into memory
    // Note: the input is a list of Data instances, each element of which contains a list
    // of bonds
    val t2 = System.nanoTime
    val jobs = loadPortfsParFold(n)
    val t3 = System.nanoTime   
    
    // Build the portfolio list
    val t0 = System.nanoTime
    val results = jobs.par.map(price)
    val t1 = System.nanoTime

    // Generate the detailed output report
    if(details) {
      println("%6s %10.10s %-5s %-2s".format("PortId","Price","Bonds","dt"))

      results.foreach { output =>
        val id = output.result.portfId

        val dt = (output.result.t1 - output.result.t0) / 1000000000.0

        val bondCount = output.result.bondCount

        val price = output.result.value

        println("%6d %10.2f %5d %6.4f %12d %12d".format(id, price, bondCount, dt, output.result.t1 - t0, output.result.t0 - t0))
      }
    }

    val dt1 = results.foldLeft(0.0) { (sum,result) =>
      sum + (result.result.t1 - result.result.t0)

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

    println(me+" DONE! %d %7.4f %7.4f".format(n, dt1, dtN))
  }

  def price(job: Job): Job = {

    // Value each bond in the portfolio
    val t0 = System.nanoTime

//    val bondIds = asList(portfsCursor,"instruments")
    val bonds = job.bonds

    val output = job.bonds.par.map { bond =>
      val t0 = System.nanoTime

      val valuator = new SimpleBondValuator(bond, Helper.yieldCurve)

      val price = valuator.price

      val t1 = System.nanoTime

      new SimpleBond(bond.id,bond.coupon,bond.freq,bond.tenor,price)
    }.par.reduce(sum)

    MongoHelper.updatePrice(job.portfId,output.maturity)

    val t1 = System.nanoTime

    Job(job.portfId,null,Result(job.portfId,output.maturity,job.bonds.size,t0,t1))
  }  

  def sum(a: SimpleBond, b:SimpleBond) : SimpleBond = {
    new SimpleBond(0,0,0,0,a.maturity+b.maturity)
  } 

  /**
   * Parallel load the portfolios with embedded bonds.
   * Note: This version uses parallel fold to reduce all the
   */
  def loadPortfsParFold(n: Int): List[Job] = {
    // Initialize the portfolios to retrieve
    val portfs = for(i <- 0 until n) yield Job(ran.nextInt(100000)+1,null,null)
    
    val z = List[Job]()
    
    val list = portfs.par.fold(z) { (a,b) =>
      // Make a into list (it already is one but this tells Scala it's one)
      // Seems a = z initially
      val opa = a match {
        case y : List[_] =>
          y
      }
      
      b match {
        // If b is a list, just append the two lists
        case opb : List[_] =>
          opb ++ opa
        
        // If b is a data, append the data to the list
        case x : Job =>
          val intermediate = MongoHelper.fetchBonds(x.portfId) 
          
          List(Job(x.portfId,intermediate.bonds,null)) ++ opa
      }         

    }
    
    list match {
      case l : List[_] =>
        l.asInstanceOf[List[Job]]
      case _ =>
        List[Job]()
    }
  }    
  
}