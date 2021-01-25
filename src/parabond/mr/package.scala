package parabond

import casa.{MongoConnection, MongoDbObject}
import parabond.entry.SimpleBond
import parabond.util.{Helper, MongoHelper, Result}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import parabond.value.SimpleBondValuator

package object mr {


  /** Gets a connection to the parabond database */
  val mongo = MongoConnection(MongoHelper.getHost)("parabond")

  /**
    * Mapping function
    * @param portfId Portfolio id
    * @return List of (portf id, bond value))
    */
  def mapping(portfId: Int): List[Result] = {
    val portfsCollecton = mongo("Portfolios")

    val portfsQuery = MongoDbObject("id" -> portfId)

    val portfsCursor = portfsCollecton.find(portfsQuery)

    val bondIds = MongoHelper.asList(portfsCursor,"instruments")

    val bondsCollection = mongo("Bonds")

    val t0 = System.nanoTime

    val value = bondIds.foldLeft(0.0) { (sum, id) =>
      val bondsQuery = MongoDbObject("id" -> id)

      val bondsCursor = bondsCollection.find(bondsQuery)

      val bond = MongoHelper.asBond(bondsCursor)

      val valuator = new SimpleBondValuator(bond, Helper.yieldCurve)

      val price = valuator.price

      sum + price
    }

//    val entry = MongoDbObject("id" -> portfId, "instruments" -> bondIds, "value" -> value)
//    mongo("Portfolios").insertOne(entry)

    MongoHelper.updatePrice(portfId,value)

    val t1 = System.nanoTime

    val dt = (t1 - t0) / 1000000000.0

    println("value = %10.2f bonds = %d dt = %f".format(value,bondIds.size,dt))

    List(Result(portfId, value, bondIds.size, t0, t1))
  }

    /**
     * Maps a portfolio to a single price
     * @param portfId Portfolio id
     * @param bondId Bond id for the specified portfolio id
     * @return List of (portf id, bond value))
     */
    def mapping(portfId: Int, bondId: Int): Result = {
      val t0 = System.nanoTime

      val bondsCollection = mongo("Bonds")

      val bondQuery = MongoDbObject("id" -> bondId)

      val bondCursor = bondsCollection.find(bondQuery)

      val bond = MongoHelper.asBond(bondCursor)

      val valuator = new SimpleBondValuator(bond, Helper.yieldCurve)

      val value = valuator.price

      // DO NOT UPDATE PORTFOLIO VALUE -- this is a single bond

      val t1 = System.nanoTime

      Result(portfId, value, 1 , t0, t1)
    }

  /**
    * Maps a portfolio to a single price
    * @param portfId Portfolio id
    * @param bonds
    * @return
    */
  def mapping(portfId: Int, bonds: List[SimpleBond]): List[Result] = {
    val t0 = System.nanoTime

    val price = bonds.foldLeft(0.0) { (sum, bond) =>
      val valuator = new SimpleBondValuator(bond, Helper.yieldCurve)

      val price = valuator.price

      sum + price
    }

    MongoHelper.updatePrice(portfId,price)

    val t1 = System.nanoTime

    List(Result(portfId,price,bonds.size,t0,t1))
  }

  /**
      * Reduces bond prices to a single portfolio price.
      * @param portfId Portfolio id
      * @param valuations Bond valuations
      * @return List of portfolio valuation, one set per portfolio
      */
    def reducing(portfId: Int, valuations: List[Result]): Result = {
      val total = valuations.foldLeft(Result(portfId,0,0,Int.MaxValue,Int.MinValue)) { (composite, result) =>

        val t0_ = Math.min(composite.t0, result.t0)
        val t1_ = Math.max(composite.t1, result.t1)

        Result(portfId, composite.value+result.value, composite.bondCount+1, t0_, t1_)
      }

      // NOTE: this might be a bug if the valuations are incomplete--
      // thus, this method should not be used for parallel collections.
      MongoHelper.updatePrice(portfId, total.value)

      val now = System.nanoTime

      Result(portfId,total.value,total.bondCount,total.t0, now)
    }

  /**
    * Maps a portfolio to its value.
    * @param input Portfolio ids
    * @param mapping Mapping function
    * @param reducing Reducing function
    * @return Mapping from portfolio id to result list (only index zero has value)
    */
  def mapreduce(
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
  def mapreduceCoarse(
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
  def mapreduceMemorybound(input: List[(Int, List[SimpleBond])],
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
  def mapreduceFine(input: List[(Int, Int)],
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
  def mapreduceFine1(input: List[(Int, Int)],
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
