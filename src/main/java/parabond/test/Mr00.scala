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

import parabond.mr._
import parabond.util.JavaMongoHelper

/** Test driver */
object Mr00 {
  def main(args: Array[String]): Unit = {
    (new Mr00).test
  }
}

/**
 * This class runs a mapreduce unit test for a limited number portfolios in the parabond database.
 * @author Ron Coleman
 */
class Mr00 {
  /** Executes test */
  def test {
    // To completely hush mongo
    JavaMongoHelper.hush()

    // Create the input of a list of Tuple2(portf id, curve coefficients).
    val input = (1 to 4).foldLeft(List[Int]()) { (list, p) =>
      list ++ List(p)
    }
    
    // Run the map-reduce
    val t0 = System.nanoTime
    
    val results = mapreduce(input, mapping, reducing)
    
    val t1 = System.nanoTime
    
    println("%6s %10.10s".format("PortId","Value"))

    // Generate the report by portfolio with the run-time
    for((portfId, result) <- results) {
      println("%6d %10.2f".format(portfId, result.value))
    }

    val dt = (t1 - t0) / 1000000000.0
    
    println("dt = %f".format(dt))    
  }
}