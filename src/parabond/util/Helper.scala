package parabond.util

/**
 * Convenience helper class
 */
object Helper {
  val TOLERANCE = 0.0001

  /**
   * Returns true if the actual and expected are within tolerance
   */
  def valid(actual: Double, expected: Double): Boolean =
    Math.abs(actual - expected) / expected < TOLERANCE

  /**
   * Nth degree polynomial coefficients to interpolate the yield curve.
   * Note: These data were generated using the PolyFitter project
   * in place of the generalized reduced gradient algorithm. Results have
   * been cross-validated with Nelson-Siegel for some on-the-run
   * treasury curve.
   */
  val yieldCurve =
    List(
      0.022250408186472507
        - 0.0034445001723609914,
      0.0015772414196917963,
      -1.6876893139502916E-4,
      7.1081143657141285E-6,
      -1.0265615090890005E-7)
}
