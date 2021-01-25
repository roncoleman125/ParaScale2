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

/**
  * This class implements a generic synchronized mailbox.
  * @author Ron.Coleman
  */
class Mailbox[T] {
  // Next index in the mail queue
  val NEXT = 0

  // Implement the mail queue as a mutable list buffer.
  import scala.collection.mutable.ListBuffer
  val box = ListBuffer[T]()

  /**
    * Adds an item to the mailbox.
    * @param item An item
    */
  def add(item: T): Unit = synchronized {
    // Adds a task then wakes up any waiting threads
    box.append(item)

    // Note: notifications are NOT buffered, although tasks are!
    this.notify
  }

  /**
    * Removes a task from the queue.
    * @return Some in the mailbox
    */
  def remove: T = synchronized {
    // If the mail queue is emtpy then we'll wait to be notified
    if(box.isEmpty)
      this.wait

    // While we can guarantee to be awaken, when notified the JVM guarantees
    // there's at least one item in the queue when we return
    val item = box.remove(NEXT)

    item
  }
}

