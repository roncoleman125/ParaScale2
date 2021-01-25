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
package parabond.cluster

import org.apache.log4j.Logger
import casa.MongoDbObject
import parabond.util.{Helper, Job, MongoHelper, Result}
import parabond.util.Constant.PORTF_NUM
import parabond.value.SimpleBondValuator
import parascale.util.getPropertyOrElse
import scala.util.Random
import scala.collection.parallel.CollectionConverters._

/**
  * Runs a basic node which retrieves the portfolios in random order and prices the portfolios
  * as a parallel collection.
  */
object BasicNode extends App {
  val LOG = Logger.getLogger(getClass)

  // Set the run parameters
  val seed = getPropertyOrElse("seed",0)
  val n = getPropertyOrElse("n", PORTF_NUM)
  val begin = getPropertyOrElse("begin", 0)

  // Reset the check portfolios over ALL portfolios
  val checkIds = checkReset(n)

  // Run the analysis
  val partition = Partition(n, begin)
  val analysis = new BasicNode analyze(partition)

  report(LOG, analysis, checkIds)
}

/**
  * Prices one portfolio per core using the basic or "naive" algorithm.
  */
class BasicNode extends Node {
  def analyze(partition: Partition): Analysis = {
    // Clock in
    val t0 = System.nanoTime

    // Seed must be same for every host in cluster as this establishes
    val ran = new Random(partition.seed)

    // Shuffled deck of portfolios -- random sample without replacement
    val sample = (0 until partition.size).toList
    val deck = ran.shuffle(sample)

    // Number of portfolios to analyze
    // Start and end (inclusive) indices in analysis sequence
    val begin = partition.begin
    val end = begin + partition.n

    // The jobs working we're on, k+1 since portf ids are 1-based
    val portfIds = for(k <- begin until end) yield Job(deck(k) + 1)

    // Get the proper collection depending on whether we're measuring T1 or TN
    val jobs = if(partition.para) portfIds.par else portfIds

    // Run the analysis
    val results = jobs.map(price)

    // Clock out
    val t1 = System.nanoTime

    Analysis(results.toList, t0, t1)
  }

  /**
    * Prices a portfolio using the "naive" algorithm.
    * It makes two requests of the database:<p>
    * 1) fetch the portfolio<p>
    * 2) fetch bonds in that portfolio.<p>
    * After the second fetch the bond is then valued and added to the portfoio value
    */
  def price(job: Job): Job = {
    // Value each bond in the portfolio
    val t0 = System.nanoTime

    // Retrieve the portfolio
    val portfId = job.portfId

    val portfsQuery = MongoDbObject("id" -> portfId)

    val portfsCursor = MongoHelper.portfolioCollection.find(portfsQuery)

    // Get the bonds ids in the portfolio
    val bondIds = MongoHelper.asList(portfsCursor,"instruments")

    // Price each bond and sum all the prices
    val value = bondIds.foldLeft(0.0) { (sum, id) =>
      // Get the bond from the bond collection by its key id
      val bondQuery = MongoDbObject("id" -> id)

      val bondCursor = MongoHelper.bondCollection.find(bondQuery)

      val bond = MongoHelper.asBond(bondCursor)

      // Price the bond
      val valuator = new SimpleBondValuator(bond, Helper.yieldCurve)

      val price = valuator.price

      // Update portfolio price
      sum + price
    }

    // Update the portfolio price in the database
    MongoHelper.updatePrice(portfId,value)

    val t1 = System.nanoTime

    Job(portfId,job.bonds,Result(portfId,value,bondIds.size,t0,t1))
  }
}
