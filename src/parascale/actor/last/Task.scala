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
import java.net.{InetAddress, Socket}

import parascale.util.getPropertyOrElse

/** Experimental task enumeration */
object TaskType extends Enumeration {
  val TASK, REPLY = Value
}

/** Task constants */
object Task {
  val TASK = 0
  val REPLY = 1
  val LOCAL_HOST = getPropertyOrElse("bindAddr", InetAddress.getLocalHost.getHostAddress) + ":0"
}

/**
  * Wrapper class for all actor messages.
  * @param src Source socket has address+":"+port
  * @param payload Payload
  * @param kind Kind of task
  */
case class Task(src: String, payload: Any, actorId: Long, kind: Int = Task.TASK) extends Serializable {
  def reply(that: Any): Unit = {
    import Task._

    // Wrap that if necessary in a task for a reply
    val task = that match {
      case task: Task =>
        task

      case _ =>
        Task(LOCAL_HOST, that, -1, REPLY)
    }

    // Determine if reply is remotely or locally bound
    val params = src.split(":")
    val addr = params(0)
    val port = params(1).toInt

    // If port is outside well-known range, assume reply remotely bound
    port match {
      case num: Int if num > 1023 =>
        // Connect to remote reply host
        val socket = new Socket(addr, port)

        // If we get here, there's somebody listening on the other side
        val os = socket.getOutputStream

        // Serialize the task and send it
        val oos = new ObjectOutputStream(os)

        oos.writeObject(task)

        oos.flush
        oos.close

        os.close
      case _ =>
        // Otherwise port is not valid, look up the actor to see if it is on this host
        Actor.lookup(actorId) match {
          case Some(entry) =>
            val (_, actor) = entry

            // -1 means recipient cant replay to this reply
            actor ! Task(task.src, task.payload, -1, REPLY)

          case None =>
            Console.err.println("bad actor id = "+actorId+" dropping reply")
        }
    }
  }

  /**
    * Sends a message.
    * @param that Message
    */
  def !!(that: Any) = reply(that)
}