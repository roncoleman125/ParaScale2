package parabond.db

import scala.util.Random

object GenerateBonds extends App {
  val ANNUAL_FREQUENCIES = Array(1, 4, 12, 52, 365)

  val MEAN_COUPON = 25.0
  val STDEV_COUPON = 5.0

  val ANNUAL_TENORS = Array(1, 2, 3, 4, 5, 7, 10, 30)

  val AT_MATURITY = 1000.0

  val NUM_BONDS = System.getProperty("bonds","5000").toInt

  val ran = new Random(0)
  for(bondno <- 1 to NUM_BONDS) {
    val coupon = Math.max(1.00,MEAN_COUPON + STDEV_COUPON*ran.nextGaussian())
    val freq = ANNUAL_FREQUENCIES(ran.nextInt(ANNUAL_FREQUENCIES.size))
    val tenor = ANNUAL_TENORS(ran.nextInt(ANNUAL_TENORS.size))
    printf("%06d,%6.2f,%3d,%2d,%6.1f\n",bondno,coupon,freq,tenor,AT_MATURITY)
  }
}
