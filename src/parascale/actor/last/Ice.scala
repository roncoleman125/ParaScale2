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
package parascale.actor.last

import java.io.ObjectInputStream
import java.net.Socket

/**
  * Inbound connection entity (ICE).
  * @param socket Inbound client socket.
  * @param callback Outbound callback handler
  */
class Ice(socket: Socket, callback: Actor) extends Runnable {
  import org.apache.log4j.Logger
  val LOG =  Logger.getLogger(getClass)

  /** Runs reply arrivals and handing off to callback actor */
  override def run = {
    LOG.info("ice started (id="+Thread.currentThread.getId+")")

    // Open the connection
    val ois = new ObjectInputStream(socket.getInputStream)

    // Deserialize the object
    val msg = ois.readObject

    LOG.info("received inbound message = " + msg)
    LOG.info("actor handler = " + callback)

    // Forward the message
    callback.send(msg)
    LOG.info("successfully relayed " + msg)

    // Release the connection
    ois.close
    socket.close
  }
}
