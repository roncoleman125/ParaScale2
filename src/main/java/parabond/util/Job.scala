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
package parabond.util

import parabond.entry.SimpleBond
import parabond.util.Constant.NONE

/**
 * Result of valuation for a portfolio including performance statistics
 */
object Result {
  /**
    * Creates a result given start and end times.
    * @param t0 Start time
    * @param t1 Finish time
    * @return Result
    */
  def apply(t0: Long, t1: Long): Result = Result(NONE, 0, 0, t0, t1)

  /**
    * Creates a result given only T1.
    * @param dt T1 time
    * @return Result
    */
  def apply(dt: Long): Result = Result(NONE, 0, 0, System.nanoTime, System.nanoTime + dt)
}

/**
  * Result class
  * @param portfId Portfolio id
  * @param value Portfoio value
  * @param bondCount Bonds in this portfolio
  * @param t0 Start time
  * @param t1 Finish time
  */
case class Result(portfId: Int, value: Double, bondCount: Int, t0: Long, t1: Long) extends Serializable
  
/**
 * Data mapped between input and output of price method
 */
object Job {
  def apply(portfId: Int): Job = Job(portfId, null ,null)
}

/**
  * Job to be processed.
  * @param portfId Portfolio id (required)
  * @param bonds null or bonds in the job
  * @param result null or result of analysis
  */
case class Job(portfId: Int, bonds: List[SimpleBond], result: Result)
