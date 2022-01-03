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
package parascale.thread.actorlike

import parascale.thread.actorlike.Constant._
import org.apache.log4j.Logger
import parascale.util._

/**
  * This object demonstrates how to dispatch tasks to workers.
  */
object Dispatcher extends App {
  val LOG =  Logger.getLogger(getClass)

  // Dispatch only as many workers as we have cores to avoid multiplexing.
  val numCores = Runtime.getRuntime.availableProcessors

  val numWorkers = getPropertyOrElse("workers", numCores)

  val workers = spawnWorkers(numWorkers)

  dispatch(workers)

  // Wait for all the workers
  join(workers)


  /**
    * Spawns workers.
    * @param n Number of workers
    * @return Spawned workers
    */
  def spawnWorkers(n: Int): Seq[Worker] = {
    for(id <- 0 until n) yield {
      val worker = new Worker(id)

      worker.start

      worker
    }
  }

  /**
    * Dispatches tasks to workers.
    * @param workers Workers
    */
  def dispatch(workers: Seq[Worker]): Unit = {
    val numTasks = getPropertyOrElse("tasks",Constant.NUM_TASKS)

    for(taskno <- 0 until numTasks) {
      val task = produce(taskno)

      val index = taskno % numWorkers

      val worker = workers(index)

      worker.send(task)
    }
  }

  /**
    * Waits for all the workers to finish.
    * @param workers Workers we join
    */
  def join(workers: Seq[Worker]): Unit = {
    workers.foreach { worker =>
      worker.send(DONE)
      worker.join
    }
  }

  /**
    * Produces a task.
    * @param num Task number
    * @return Task
    */
  def produce(num: Int): Task = {
    sleep(MAX_PRODUCING)

    LOG.debug("producing task "+num)
    Task(num, MAX_PRODUCING)
  }
}


