package parabond.cluster

import org.junit.runner.RunWith
import org.junit.runners.Suite

@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array[BasicNodeTest]())
class ClusterSuite
