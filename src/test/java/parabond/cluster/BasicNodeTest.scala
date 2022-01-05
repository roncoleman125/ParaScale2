package parabond.cluster

import junit.framework.TestCase
import org.apache.log4j.Logger
import parabond.cluster.BasicNode.getClass
import parabond.util.Constant.PORTF_NUM
import parascale.util.getPropertyOrElse

class BasicNodeTest extends TestCase {
  def test(): Unit = {
    val LOG = Logger.getLogger(getClass)

    // Set the run parameters
    val n = getPropertyOrElse("n", PORTF_NUM)
    val begin = getPropertyOrElse("begin", 0)

    // Reset the check portfolios over ALL portfolios
    val checkIds = checkReset(n)

    // Run the analysis
    val analysis = new BasicNode(Partition(n, begin)) analyze

    report(LOG, analysis, checkIds)
  }
}
