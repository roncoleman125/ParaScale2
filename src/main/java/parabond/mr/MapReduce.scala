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
package parabond.mr

import parabond.entry.SimpleBond
import parabond.util.Result
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * This object contains convenience methods for running mapreduce operations
  * on a portfolio of bonds.
  */
object MapReduce {
  /**
    * Maps a portfolio to its value using trivial reduction.
    * @param input Portfolio ids
    * @param mapping Mapping function
    * @param reducing Reducing function
    * @return Mapping from portfolio id to result list (only index zero has value)
    */
  def basic (
              input: List[Int],
              mapping: Int => List[Result],
              reducing: (Int, List[Result]) => Result): Map[Int, Result] = {
    case class Intermediate(portfId: Int, results: List[Result])

    // Value the portfolios in parallel
    val futures = for (portfId <- input) yield Future {
      Intermediate(portfId, mapping(portfId))
    }

    val results = futures.foldLeft(Map[Int, Result]()) { (map, future) =>
      val intermediate = Await.result(future, 100 seconds)

      val portfId = intermediate.portfId

      map + (portfId -> reducing(portfId, intermediate.results))
    }

    results
  }

  /**
    * Values the portfolios in groups, ie, a single future values multiple portfs instead of a single portf.
    * @param input List of portfolio ids
    * @param mapping Mapping function
    * @param reducing Reducing function
    * @param numMappers Number of mappers
    * @param numReducers Number of reducers
    * @return Map from portfolio id to a list of its values (actually only the first value)
    */
  def coarse(
              input: List[Int],
              mapping: Int => List[Result],
              reducing: (Int, List[Result]) => Result,
              numMappers: Int,
              numReducers: Int): Map[Int, Result] = {
    case class Intermediate(portfId: Int, results: List[Result])
    case class Reduced(portfid: Int, values: List[Result])

    // Value a group of portfolios within a single future in parallel
    val mappedFutures: Iterator[Future[List[Intermediate]]] =
      for (group <- input.grouped(input.length / numMappers)) yield Future {
        for (portfId <- group) yield
          Intermediate(portfId, mapping(portfId))
      }


    val reducedFutures: Iterator[Future[Map[Int,Result]]] =
      for(reducibleFuture <- mappedFutures) yield Future {
        val intermediates = Await.result(reducibleFuture, 100 seconds)

        val innermap = intermediates.foldLeft(Map[Int, Result]()) { (map, intermediate) =>
          val portfId = intermediate.portfId

          map + (portfId -> reducing(portfId, intermediate.results))
        }

        innermap
      }

    reducedFutures.foldLeft(Map[Int, Result]()) { (map, future) =>
      val result = Await.result(future, 100 seconds)

      map ++ result
    }
  }

  /**
    * Values the portfolios given all the portfolios and bonds have been loaded into memory.
    * @param input List of pairs of portfolio ids and bonds (not bond ids)
    * @param mapping Mapping function
    * @param reducing Reducing function
    * @return Mapping from portfid -> value
    */
  def memorybound(input: List[(Int, List[SimpleBond])],
                                  mapping: (Int,List[SimpleBond]) => List[Result],
                                  reducing: (Int,List[Result]) => Result): Map[Int, Result] = {
    case class Intermediate(portfId: Int, results: List[Result])

    val futures = for((portfId, bonds) <- input) yield Future {
        Intermediate(portfId, mapping(portfId,bonds))
    }

    val results = futures.foldLeft(Map[Int, Result]()) { (map, future) =>
      val intermediate = Await.result(future, 100 seconds)

      val portfId = intermediate.portfId

      map + (portfId -> reducing(portfId, intermediate.results))
    }

    results
  }

  /**
    * Values portfolio one bond per future in two passes.
    * @param input List of pairs of portfolio ids and bonds (not bond ids)
    * @param mapping Mapping function
    * @param reducing Reducing function
    * @return Mapping from portfid -> value
    */
  def fine(input: List[(Int, Int)],
                   mapping: (Int, Int) => Result,
                   reducing: (Int,List[Result]) => Result): Map[Int, Result] = {
    case class Intermediate(portfId: Int, result: Result)

    // Pass 0: spawn the futures
    val futures = for((portfId, bonds) <- input) yield Future {
      Intermediate(portfId, mapping(portfId,bonds))
    }

    // Pass 1: Gather the intermediate results
    val map = Map[Int, List[Result]]().withDefault(k => List[Result]())

    val pass1 = futures.foldLeft(map) { (portfToResultsMap, future) =>
      val intermediate = Await.result(future, 100 seconds)

      val portfId: Int = intermediate.portfId

      val results: List[Result] = portfToResultsMap(portfId)

      portfToResultsMap + (portfId -> (results ++ List(intermediate.result)))
    }

    // Pass 2: reduce the results of pass 1 as a colleciton of futures
    val pass2 = for(portfToResultsMap <- pass1) yield Future {
      val (portfId, intermediateResults) = portfToResultsMap

      val value = reducing(portfId, intermediateResults)

      Tuple2(portfId, value)
    }

    pass2.foldLeft(Map[Int, Result]()) { (map, future) =>
      val result = Await.result(future, 100 seconds)

      map + (result._1 -> result._2)
    }
  }

  /**
    * Values portfolio one bond per future in single pass.
    * @param input List of pairs of portfolio ids and bonds (not bond ids)
    * @param mapping Mapping function
    * @param reducing Reducing function
    * @return Mapping from portfid -> value
    */
  def fine1(input: List[(Int, Int)],
           mapping: (Int, Int) => Result,
           reducing: (Int,List[Result]) => Result): Map[Int, Result] = {
    case class Intermediate(portfId: Int, result: Result)

    val futures = for((portfId, bonds) <- input) yield Future {
      Intermediate(portfId, mapping(portfId,bonds))
    }

    futures.foldLeft(Map[Int, Result]().withDefault(k => Result(-1,0,0,Int.MaxValue,Int.MinValue))) { (map, future) =>
      val intermediate = Await.result(future, 100 seconds)

      val portfId = intermediate.portfId

      val resulta = map(portfId)
      val resultb = intermediate.result

      val t0 = Math.min(resulta.t0, resultb.t0)
      val t1 = Math.max(resulta.t1, resultb.t1)

      val reduced = Result(portfId,resulta.value+resultb.value,resulta.bondCount+resultb.bondCount,t0,t1)

      map + (portfId -> reduced)
    }
  }
}




