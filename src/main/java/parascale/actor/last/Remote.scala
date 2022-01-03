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

import java.net.ServerSocket

/**
  * Creates a remote object.
  * @author Ron.Coleman
  */
object Remote {
  def apply(localPort: Int, actor: Actor) = new Remote(localPort, actor)
}

/**
  * This actor runs on the client side and relays message to the local actor
  * @param local Local actor to whom messages will be forwarded
  * @param port Port to listen for inbound remote messages.
  */
class Remote(port: Int, local: Actor) extends Runnable {
  import org.apache.log4j.Logger
  val LOG =  Logger.getLogger(getClass)

  // Start the thread to receive inbound messages
  val me = new Thread(this)
  me.start

  /** Relays inbound message to the designated local actor */
  def run = {
    val id = Thread.currentThread.getId
    LOG.info("remote daemon id = " + id + " for call-forward actor " + local + " port = " + port)
    val socket = new ServerSocket(port)

    while (true) {
      LOG.info("waiting to accept connection on port " + port)
      val clientSocket = socket.accept()

      LOG.info("got connection from " + clientSocket.getInetAddress.getHostAddress)

      new Thread(new Ice(clientSocket, local)).start
    }
  }
}
