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

import parascale.util.getPropertyOrElse
import scala.util.Random

/**
  * Base node class.
  */
abstract class Node(partition: Partition) {
  /** Beginning index */
  val begin = partition.begin

  /* Exclusive end index */
  val end = begin + partition.n

  /**
    * Analyzes the portfolios.
    * @return Analysis
    */
  def analyze(): Analysis

  /**
    * Gets the sequence randomized deck of portfolio ids to analyze.
    * @return Deck of portfolio ids
    */
  def getDeck(): List[Int] = {
    val ran = new Random(partition.seed)

    val sample = (begin until end).toList

    val deck = ran.shuffle(sample)
    deck
  }
}

object Node {
  /**
    * Gets a compatible node for the indicated partition.
    * <p>Use the VM option, -Dnode=className> to change the default node.
    * @param partition Indicated partition
    * @return Node
    */
  def getInstance(partition: Partition): Node = {
    // Get the class
    val prop = getPropertyOrElse("node", "parabond.cluster.BasicNode")

    val clazz = Class.forName(prop)

    // Get  constructor with Partition as the one and only parameter
    try {
      // Throws exception here if there is no such constructor
      val constructor = clazz.getConstructor(classOf[Partition])

      // If we get here, we have a proper constructor
      val node = constructor.newInstance(partition)

      // Make sure this is not class masquerading as a Node
      if(!node.isInstanceOf[Node])
        return null

      node.asInstanceOf[Node]
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
    //    // Find the appropriate constructor
    //    val constructors = clazz.getConstructors()
    //
    //    // One we're looking for has Partition as the first and only formal parameter.
    //    // An alternative way to do this is to use th
    //    constructors.find { constructor =>
    //      val paramTypes = constructor.getParameterTypes
    //      paramTypes.size == 1 && paramTypes(0) == classOf[Partition]
    //    } match {
    //      case Some(constructor) =>
    //        // This will fail if the clazz is NOT a Node
    //        val node = constructor.newInstance(partition).asInstanceOf[Node]
    //        node
    //
    //      case None =>
    //        // If we get here, found no compatible Node
    //        null
    //    }
  }
}

