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
import parabond.casa.MongoDbObject
import parabond.util.Constant.{NUM_PORTFOLIOS, PORTF_NUM}
import parabond.util.{Job, MongoHelper}
import parascale.util.getPropertyOrElse
import scala.collection.parallel.CollectionConverters._

/**
  * Runs a coarse node which retrieves the portfolios in block random order and prices the blocks sequentially.
  */
object CoarseGrainedNode extends App {
  val LOG = Logger.getLogger(getClass)

  val seed = getPropertyOrElse("seed",0)
  val size = getPropertyOrElse("size", NUM_PORTFOLIOS)
  val n = getPropertyOrElse("n", PORTF_NUM)
  val begin = getPropertyOrElse("begin", 1)

  val checkIds = checkReset(n)

  val analysis = new CoarseGrainedNode(Partition(n=n, begin=begin)) analyze

  // Validate the check id by...
  // 1. Testing a random portfolio against the check value
  // 2. Resetting the portfolio value -- to test the check method invoked by report
  val checkId = checkIds(0);

  val portfsQuery = MongoDbObject("id" -> checkId)

  val portfsCursor = MongoHelper.portfolioCollection.find(portfsQuery)

  val price = MongoHelper.asDouble(portfsCursor,"price")

  if(price != CHECK_VALUE)
    MongoHelper.updatePrice(checkId, CHECK_VALUE)

  report(LOG, analysis, checkIds)
}

/**
  * Prices a block of portfolios per core.
  */
class CoarseGrainedNode(partition: Partition) extends Node(partition) {
  // Node used the price portfolios.
  def node = new BasicNode(partition)

  /**
    * Runs the portfolio analyses.
    * @return Analysis
    */
  override def analyze(): Analysis = {
    // Clock in
    val t0 = System.nanoTime

    val deck = getDeck()

    // The jobs working we're on, k+1 since portf ids are 1-based
    assert(deck.size == (end-begin+1))

    val jobs = (0 until deck.size).foldLeft(List[Job]()) { (jobs, k) =>
      jobs ++ List(new Job(k))
    }

    // Block the indices according to number of cores: each core gets a single clock.
    val numCores = getPropertyOrElse("cores",Runtime.getRuntime.availableProcessors)

//    val blksize = partition.n / numCores
    // Get block size of portfolios a core will analyze.
    // Fixed proposed by R.A. Dartey, 26 May 2021.
    val blksize = (partition.n.toDouble / numCores).ceil.toInt

    val blocks = for(coreno <- 0 until numCores) yield {
      val start = coreno * blksize
      val finish = start + blksize

      jobs.slice(start, finish)
    }

    // .par needs to go here otherwise compiler rejects
    val results = blocks.par.map(price)

    // Need Seq[Data], not ParSeq[Seq[Data]], for reporting and compiler specs
    val flattened = results.flatten

    // Clock out
    val t1 = System.nanoTime

    Analysis(flattened.toList, t0, t1)
  }

  /**
    * Prices a collection of tasks.
    * Assumes tasks is a serial collection.
    * Check above to find blocks is IndexSeq[IndexSeq[Task]]. So .par on it will be
    * ParSeq[IndexSeq[Task]]. So tasks should be IndexSeq[Task].
    * @param jobs Jobs
    * @return Completed work
    */
  def price(jobs: Seq[Job]) : Seq[Job] = {
    jobs.map(node.price)
  }
}
