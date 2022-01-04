package parabond.util

import ch.qos.logback.classic.{Level, Logger, LoggerContext}
import com.mongodb.client.FindIterable
import org.bson.Document
import org.slf4j.LoggerFactory
import parabond.casa.{MongoConnection, MongoDbObject}
import parabond.entry.SimpleBond
import parascale.util.getPropertyOrElse

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.collection.parallel.CollectionConverters._

/**
 * This object implements monngo-specific helper functions.
 * @author Ron.Coleman
 */
object MongoHelper {
  /** Sets the mongo host */
  val host: String = getHost

  /** Connects to the parabond DB */
  val mongo = MongoConnection(host)("parabond")

  val portfolioCollection = mongo("Portfolios")
  val bondCollection = mongo("Bonds")

  /** Initialize the random number generator */
  val ran = new Random(0)

  case class PortfIdToBondsMap(portfId: Int, bonds: List[SimpleBond])

  case class Bonds(list: List[SimpleBond])

  def hush(): Unit = {
    val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val rootLogger = loggerContext.getLogger("org.mongodb.driver")
    rootLogger.setLevel(Level.OFF)
  }

  /**
   * Disables Mongo logging.
   */
  def disableLogging: Unit = {
    enableLogging(false)
  }

  /**
   * Enables Mongo logging
   * @param logging If true logging enabled, otherwise disabled.
   */
  def enableLogging(logging: Boolean): Unit = {
    import com.mongodb.diagnostics.logging.Loggers
    import java.util.logging.Level
    import java.util.logging.Logger
    if(logging)
      Logger.getLogger(Loggers.PREFIX).setLevel(Level.INFO)
    else
      Logger.getLogger(Loggers.PREFIX).setLevel(Level.SEVERE)
  }

  /**
   * Loads a list of 2-tuples of portfolios x list of bonds
   */
  def loadPortfs(n: Int): List[(Int, List[SimpleBond])] = {
    // Connect to the portfolio collection
    val portfsCollecton = mongo("Portfolios")

    val input = (1 to n).foldLeft(List[(Int, List[SimpleBond])]()) { (list, pid) =>
      // Select a portfolio
      val lottery = ran.nextInt(100000) + 1

      // Retrieve the portfolio
      val portfsQuery = MongoDbObject("id" -> lottery)

      val portfsCursor = portfsCollecton.find(portfsQuery)

      // Get the bonds in the portfolio
      val bondIds = MongoHelper.asList(portfsCursor, "instruments")

      val bonds = bondIds.foldLeft(List[SimpleBond]()) { (bonds, id) =>
        // Get the bond from the bond collection
        val bondQuery = MongoDbObject("id" -> id)

        val bondCursor = bondCollection.find(bondQuery)

        val bond = MongoHelper.asBond(bondCursor)

        // The price into the aggregate sum
        bonds ++ List(bond)
      }

      list ++ List((lottery, bonds))
    }

    input
  }

  /**
   * Parallel load the portfolios and bonds into memory (actor-based).
   */
  def loadPortfsParallel(n: Int): List[(Int, List[SimpleBond])] = {
    val futures = for (_ <- 1 to n) yield Future {
      // Select a portfolio
      val lottery = ran.nextInt(Constant.NUM_PORTFOLIOS) + 1

      // Fetch its bonds
      fetchBonds(lottery)
    }

    val list = futures.foldLeft(List[(Int, List[SimpleBond])]()) { (list, future) =>
      import scala.concurrent.duration._

      val result = Await.result(future, 100 seconds)

      list ++ List((result.portfId, result.bonds))
    }

    list
  }

  /**
   * Loads portfolios x bonds into memory
   */
  def loadPortfsPar(n: Int): List[(Int, List[SimpleBond])] = {
    val lotteries = for (i <- 0 to n) yield ran.nextInt(100000) + 1

    val list = lotteries.par.foldLeft(List[(Int, List[SimpleBond])]()) { (portfIdBonds, portfId) =>
      val intermediate = fetchBonds(portfId)

      (portfId, intermediate.bonds) :: portfIdBonds
    }

    list
  }

  /** Converts mongo cursor to scala list of int objects */
  def asList(results: FindIterable[Document], field: String): List[Int] = {
    val cursor = results.iterator

    if (cursor.hasNext) {
      val value = cursor.next().get(field)

      value match {
        case list: java.util.List[_] =>
          import scala.collection.JavaConverters._
          list.asInstanceOf[java.util.List[Int]].asScala.toList

        case _ =>
          scala.List[Int]()

      }
    }
    else
      List[Int]()
  }

  /**
   * Converts mongo cursor to a scala double.
   *
   * @param results Cursor
   * @param field   Field to convert
   * @return Double if the value exists and NaN otherwise.
   */
  def asDouble(results: FindIterable[Document], field: String): Double = {
    val cursor = results.iterator

    if (cursor.hasNext) {
      val value = cursor.next().get(field)

      value match {
        case dubble: java.lang.Double =>
          dubble.asInstanceOf[java.lang.Double]

        case _ =>
          Double.NaN

      }
    }
    else
      Double.NaN
  }

  /**
   * Converts mongo cursor to a scala int.
   *
   * @param results Cursor
   * @param field   Field to convert
   * @return Some if the value exists and None otherwise.
   */
  def asInt(results: FindIterable[Document], field: String): Option[Int] = {
    val cursor = results.iterator

    if (cursor.hasNext) {
      val value = cursor.next().get(field)

      value match {
        case ent: java.lang.Double =>
          Some(ent.asInstanceOf[java.lang.Integer])

        case _ =>
          None

      }
    }
    else
      None
  }

  /**
   * Converts the mongo cursor to a bond -- assuming the query cursor
   * as a single bond
   */
  def asBond(results: FindIterable[Document]): SimpleBond = {
    val cursor = results.iterator

    if (cursor.hasNext) {
      val bondParams = cursor.next()

      val id = bondParams.get("id").toString.toInt

      val coupon = bondParams.get("coupon").toString.toDouble

      val freq = bondParams.get("freq").toString.toInt

      val tenor = bondParams.get("tenor").toString.toDouble

      val maturity = bondParams.get("maturity").toString.toDouble

      SimpleBond(id, coupon, freq, tenor, maturity)
    }
    else
      SimpleBond()
  }

  /**
   * Fetches the bonds from the database.
   *
   * @param portfId Portfolio id
   * @return Container of portfolio id and bonds
   */
  def fetchBonds(portfId: Int): PortfIdToBondsMap = {
    // Retrieve the portfolio
    val portfsQuery = MongoDbObject("id" -> portfId)

    val portfsCursor = portfolioCollection.find(portfsQuery)

    // Get the bonds in the portfolio
    val bondIds = MongoHelper.asList(portfsCursor, "instruments")

    // Connect to the bonds collection
    val bondsCollection = mongo("Bonds")

    val bonds = bondIds.foldLeft(List[SimpleBond]()) { (bonds, id) =>
      // Get the bond from the bond collection
      val bondQuery = MongoDbObject("id" -> id)

      val bondCursor = bondsCollection.find(bondQuery)

      val bond = MongoHelper.asBond(bondCursor)

      // The price into the aggregate sum
      bonds ++ List(bond)
    }

    // Method below runs out of semaphores on mongo
    //      val bonds = fetchBondsParallel(bondIds,bondsCollection)

    PortfIdToBondsMap(portfId, bonds)
  }

  /**
   * Updates the portfolio's price.
   *
   * @param portfId Portfolio id
   * @param price   Prince
   * @return modification count
   */
  def updatePrice(portfId: Int, price: Double): Long = {
    val portfQuery = MongoDbObject("id" -> portfId)
    val portfolio = portfolioCollection.find(portfQuery)
    if(portfolio == null)
      return -1

    val newPrice = MongoDbObject("$set" -> MongoDbObject("price" -> price))

    val result = portfolioCollection.updateOne(portfQuery, newPrice)

    result.getModifiedCount
  }

  /**
   * Updates a collection
   *
   * @param list List portfolio, price
   * @return List modificaton count
   */
  def updatePrices(list: List[(Int, Double)]): List[Long] = {
    for (item <- list) yield
      updatePrice(item._1, item._2)
  }

  /**
   * Gets the mongo host.
   * */
  def getHost: String = getPropertyOrElse("host", "127.0.0.1")
}
