/*
 Copyright (c) Ron Coleman

 Permission is hereby granted, free of charge, to any person obtaining
 a copy of this software and associated documentation files (the
 "Software"), to deal in the Software without restriction, including
 without limitation the rights to use, copy, modify, merge, publish,
 distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to
 the following conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package parascale

import org.apache.log4j.Logger

package object util {
  /**
   * Parses a boolean string.
   * @param s String
   * @return True if "true" and false otherwise
   */
  def parseBoolean(s: String): Boolean = if(s == "true") true else false

  /**
   * Parses a string
   * @param s String
   * @return String
   */
  def parseString(s: String) = s

  /**
   * Gets an integer value from system properties, if it's not found use a default.
   * @param key Property
   * @param default Default integer
   * @return Default integer value
   */
  def getPropertyOrElse(key: String, default: Int): Int = getPropertyOrElse(key,Integer.parseInt,default)

  /**
   * Gets a generic property from the system properyies, if it's not found use a default.
   * @param key Property
   * @param parse Parser
   * @param default Default
   * @tparam T Parameterize type of value
   * @return Key-value or default value
   */
  def getPropertyOrElse[T](key: String, parse: (String) => T, default: T): T = {
    val value = System.getProperty(key)

    if(value == null)
      default
    else
      parse(value)
  }

  /**
   * Gets a system property or a default string value
   * @param key Property
   * @param default Default
   * @return Key-value or default value
   */
  def getPropertyOrElse(key: String, default: String): String = getPropertyOrElse(key,parseString,default)

  /**
   * Gets a system property or a default boolean value
   * @param key
   * @param default
   * @return
   */
  def getPropertyOrElse(key: String, default: Boolean): Boolean = getPropertyOrElse(key,parseBoolean, default)

  /**
   * Redirects the error output if property set.
   * @see @see <a href="https://stackoverflow.com/questions/8708342/redirect-console-output-to-string-in-java">Redirect console output to string in Java</a>
   */
  def redirectErr:Unit = {
    val err = getPropertyOrElse("err","")
    if(err != "") {
      val fos = new java.io.FileOutputStream(err,true)
      val os = new java.io.PrintStream(fos)
      System.setErr(os)
    }
  }

  /**
   * Convenience method for sleeping.
   * @param millis Time in milliseconds to sleep
   */
  def sleep(millis: Long): Unit = Thread.sleep(millis)

  /**
   * Convenience method for sleeping.
   * @param seconds Time in seconds.
   */
  def sleep(seconds: Double): Unit = sleep((seconds * 1000 + 0.5).toLong)
}
