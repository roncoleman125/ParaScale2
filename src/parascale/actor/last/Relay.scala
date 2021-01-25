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

import java.io.ObjectOutputStream
import java.net.{InetAddress, ServerSocket, Socket}

/**
  * This object binds an actor for reply purposes to a destination host.
  * @author Ron.Coleman
  */
object Relay {
  val DEFAULT_PORT = 9000

  /**
    * Makes relays.
    * @param hostSocket Destination host address
    * @param callback Callback actor for replies.
    * @return
    */
  def apply(hostSocket: String, callback: Actor) = new Relay(hostSocket,callback)
}

/**
  * This class is an actor which relays message from the local (or srouce host) to a remote (or destination) host.
  * @param forward Destination forwarding socket
  * @param callback Actor to receive replies.
  */
class Relay(forward: String, callback: Actor) extends Actor {
  import org.apache.log4j.Logger
  val LOG =  Logger.getLogger(getClass)

  // Initialize forwarding parameters
  val params = forward.split(":")

  val forwardAddr = params(0)
  val forwardPort = if(params.length == 2) params(1).toInt else Relay.DEFAULT_PORT
  LOG.info("relaying all messages to "+forwardAddr+":"+forwardPort)

  // Initialize reply target
  val replyAddr =  InetAddress.getLocalHost.getHostAddress
  val replyPort = forwardPort + Thread.activeCount
  LOG.info("listening for replies on "+replyAddr+":"+replyPort)

  /** Runs the worker thread to receive replies. */
  def act = {
    Thread.sleep(250)

    val id = Thread.currentThread.getId
    LOG.info("relay daemon started id = "+id+" for callback actor "+callback)
    val socket = new ServerSocket(replyPort)

    while(true) {
      LOG.info("waiting to accept reply connection on port = " + replyPort)
      val clientSocket: Socket = socket.accept()

      LOG.info("reply connection accepted from host " + clientSocket.getInetAddress.getHostAddress)

      new Thread(new Ice(clientSocket, callback)).start
    }
  }


  /**
    * Sends a message using sockets.
    * @param that A message
    */
  override def send(that: Any): Unit = {
    send(that, callback)
  }

  override def send(that: Any, sender: Actor): Unit = {
    val task = that match {
      case t: Task =>
        t
      case _ =>
        Task(replyAddr+":"+replyPort, that, sender.id)
    }

    LOG.info("relaying "+that+" as "+task)

    val socket = new Socket(forwardAddr, forwardPort)

    val os = socket.getOutputStream
    val oos = new ObjectOutputStream(os)

    oos.writeObject(task)

    oos.flush
    oos.close

    os.close

    LOG.info("successfully sent "+task+" to "+forwardAddr+":"+forwardPort)
  }
}



