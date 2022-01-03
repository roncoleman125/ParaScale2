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

import scala.util.Random
import parabond.casa.MongoDbObject
import parabond.util.{Helper, MongoHelper, Result}
import parabond.value.SimpleBondValuator
import parabond.mr._
import parascale.util._
import parabond.util.Constant._

/** Test driver */
object Mr05 {
  def main(args: Array[String]): Unit = {
    (new Mr05).test
  }
}

/**
 * This class runs a mapreduce unit test for n portfolios in the
 * parabond database, alternating between parallel and serial methods.
 * @author Ron Coleman, Ph.D.
 */
class Mr05 {
  /** Initialize the random number generator */
  val ran = new Random(0)   
  
  /** Write a detailed report */
  val details = true
  
  /** Unit test entry point */
  def test {
    val n = getPropertyOrElse("n",PORTF_NUM)

    print("\n"+this.getClass()+" "+ "N: "+n +" ")

    (1 to n).foreach { p =>
      // Choose a random portfolio
      val portfId = ran.nextInt(100000)+1

      val results = new Array[Result](2)
      
      // Evaluate the portfolios in random order
      if(portfId %2 == 0) {
        results(0) = priceParallel(portfId)
        results(1) = priceSerially(portfId)
      }
      else {
        results(1) = priceSerially(portfId)
        results(0) = priceParallel(portfId)
      }
      
      val dt0 = (results(0).t1 - results(0).t0) / 1000000000.0
      
      val dt1 = (results(1).t1 - results(1).t0) / 1000000000.0      
        
      println("%6d %10.2f %10.2f %6.4f %6.4f".format(results(0).portfId, results(0).value, results(1).value, dt0, dt1))
      
    }
  }
  
  def priceParallel(portfId : Int) : Result = {
    val t0 = System.nanoTime
    
    val list = List(portfId)
    
    val result = mapreduce(list, mapping, reducing)
    
    val t1 = System.nanoTime
    
    val rsult = result(portfId)
    
    Result(portfId,rsult.value,rsult.bondCount,t0,t1)
  }
  
  def priceSerially(portfId : Int) : Result = {
      // Connect to the portfolio collection
      val t0 = System.nanoTime
      
      val portfsCollecton = mongo("Portfolios")
      
      val portfsQuery = MongoDbObject("id" -> portfId)

      val portfsCursor = portfsCollecton.find(portfsQuery)

      // Get the bonds in the portfolio
      val bondIds = MongoHelper.asList(portfsCursor, "instruments")

      // Connect to the bonds collection
      val bondsCollection = mongo("Bonds")

      val value = bondIds.foldLeft(0.0) { (sum, id) =>
        
        // Get the bond from the bond collection
        val bondQuery = MongoDbObject("id" -> id)

        val bondCursor = bondsCollection.find(bondQuery)

        val bond = MongoHelper.asBond(bondCursor)

        // Price the bond
        val valuator = new SimpleBondValuator(bond, Helper.yieldCurve)

        val price = valuator.price

        // Add the price into the aggregate sum
        sum + price
      }    
      
      val t1 = System.nanoTime
      Result(portfId,value,bondIds.size,t0,t1)
  }
}