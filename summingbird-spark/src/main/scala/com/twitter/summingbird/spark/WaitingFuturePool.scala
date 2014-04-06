package com.twitter.summingbird.spark

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.util.{Future, Promise, FuturePool, ExecutorServiceFuturePool}
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}

/**
 * A [[FuturePool]] that queues up an unbounded number of tasks, but does not begin executing them
 * until start() is called. start() can only be called once.
 *
 * @param delegate the underlying future pool to delegate to
 * @author Alex Levenson
 */
class WaitingFuturePool(delegate: ExecutorServiceFuturePool) extends FuturePool {
  private[this] val lock = new Object

  private[this] val waitPromise = new Promise[Unit]

  // this is just an easy way to turn a Thread into a Future[Unit]
  private[this] val completionThread: ExecutorServiceFuturePool =
    FuturePool(Executors.newSingleThreadExecutor(
      // give the thread a name, and more importantly make it a daemon thread
      new NamedPoolThreadFactory("SparkPlatformCompletionPool", true)))

  // TODO: I don't really like this, but I can't think of a better way
  // count the number of tasks submitted, then countdown each time a task
  // completes, and when the count reaches zero we can shut down the thread pool
  private[this] val submittedTasksCount = new AtomicInteger(0)
  private[this] var unfinishedTasks: CountDownLatch = _
  private[this] val started = new AtomicBoolean(false)

  def apply[T](f: => T): Future[T] = {
    lock.synchronized {
      assertNotStarted()

      submittedTasksCount.addAndGet(1)

      // don't actually submit to delegate until waitPromise is fulfilled
      waitPromise.flatMap {
        _ => delegate(f)
      } ensure {
        // this task is done
        unfinishedTasks.countDown()
      }
    }
  }

  /**
   * Begin executing the previously submitted tasks.
   * Can only be called once.
   *
   * @return a Future[Unit] that will complete when all submitted tasks have completed.
   */
  def start(): Future[Unit] = {
    lock synchronized {
      assertNotStarted()

      started.set(true)
      unfinishedTasks = new CountDownLatch(submittedTasksCount.get)

      // start the future pool
      waitPromise.setValue(Unit)

      // create a Future for when the future pool has completed
      val shutdown: Future[Unit] = completionThread {
        unfinishedTasks.await()
        delegate.executor.shutdownNow()
        delegate.executor.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
      }

      shutdown ensure {
        completionThread.executor.shutdownNow()
      }

    }
  }

  private[this] def assertNotStarted() = if (started.get)
    throw new IllegalStateException("This WaitingFuturePool has already been started!")
}