package parabond.util

object Constant {
  /** Number of portfolios in the database */
  val NUM_PORTFOLIOS = 100000

  /** Number of bonds inthe database */
  val NUM_BONDS = 5000

  /** Portfolios collection name */
  val COLL_PORTFOLIOS_NAME = "Portfolios"

  /** Bonds collection name */
  val COLL_BONDS_NAME = "Bonds"

  /** Bonds input data */
  val INPUT_BONDS_FILENAME = "bonds.txt"

  /** Portfolio input data */
  val INPUT_PORTFS_FILENAME = "portfs.txt"

  /**
   * Maximum wait time in seconds.
   */
  val MAX_WAIT_TIME = 100000

  /** Directory to write diagnostic output */
  val DIAGS_DIR = "c:/tmp/"

  /** Default number of bond portfolios to analyze */
  val PORTF_NUM = 100

  val NONE = -1
}
