/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.util.executor;

import alluxio.collections.ConcurrentHashSet;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * An extension of ThreadPoolExecutor which supports a timeout after which submitted tasks will
 * receive an interrupt.
 */
public final class TimeoutThreadPoolExecutor extends ThreadPoolExecutor {
  /** The period after which tasks should be interrupted. */
  private final long mTimeoutMs;
  /** The maximum amount of time to sleep before checking for expired tasks. */
  private final long mMaximumCheckIntervalMs;
  /**
   * A queue tracking when each task is slated to time out. The elements of this queue should be in
   * roughly sorted order, with the elements closest to timing out at the front. When tasks begin
   * they are added to the end of the queue. The {@link mInterruptorThread} processes the front of
   * the queue, interrupting any expired tasks.
   */
  private final ConcurrentLinkedQueue<TaskInfo> mTaskExpirationQueue;
  /**
   * A set of all currently-running tasks. Removal from this set at task completion is synchronized
   * on this object, such that synchronizing on {@link mCurrentlyRunning} prevents any tasks from
   * completing.
   */
  private final ConcurrentHashSet<Runnable> mCurrentlyRunning;
  /** A thread charged with interrupting timed out tasks. */
  private final Thread mInterruptorThread;

  /**
   * Creates a new {@code TimeoutThreadPoolExecutor} with the given initial parameters and default
   * thread factory and rejected execution handler. Threads created by this executor will be
   * interrupted after the specified timeout.
   *
   * @param timeoutMs tasks run by this executor will be interrupted after this many milliseconds
   * @param maximumCheckIntervalMs The maximum amount of time to sleep before checking for expired
   *        tasks
   * @param corePoolSize the number of threads to keep in the pool, even if they are idle
   * @param maximumPoolSize the maximum number of threads to allow in the pool
   * @throws IllegalArgumentException if one of the following holds:<br>
   *         {@code timeoutMs <= 0}<br>
   *         {@code maximumPoolSize <= 0}<br>
   *         {@code maximumPoolSize < corePoolSize}
   */
  public TimeoutThreadPoolExecutor(long timeoutMs, long maximumCheckIntervalMs, int corePoolSize,
      int maximumPoolSize) {
    super(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
    Preconditions.checkArgument(timeoutMs > 0, "timeoutMs must be positive");
    mTimeoutMs = timeoutMs;
    mMaximumCheckIntervalMs = maximumCheckIntervalMs;
    mTaskExpirationQueue = new ConcurrentLinkedQueue<>();
    mCurrentlyRunning = new ConcurrentHashSet<>();
    mInterruptorThread = new Thread(new Interruptor());
    mInterruptorThread.start();
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    mCurrentlyRunning.add(r);
    mTaskExpirationQueue.add(new TaskInfo(t, r));
    super.beforeExecute(t, r);
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    synchronized (mCurrentlyRunning) {
      mCurrentlyRunning.remove(r);
    }
  }

  /**
   * A Runnable which consumes the threads in mThreadQueue, sending them interrupts if they reach
   * their timeout. This Runnable alternates between sleeping and interrupting all timed out
   * threads. The length of time to sleep is until the next thread is scheduled to time out. If
   * there is no next thread, this Runnable will sleep for one second.
   *
   * This class assumes that it is the only thread removing elements from
   * {@link #mTaskExpirationQueue}.
   *
   * Threads may be reused to execute multiple Runnables. To ensure that the right Runnable is
   * canceled, the {@link #mCurrentlyRunning} set is referenced to
   */
  private class Interruptor implements Runnable {
    @Override
    public void run() {
      while (!Thread.interrupted()) {
        long now = System.currentTimeMillis();
        while (!mTaskExpirationQueue.isEmpty()
            && now >= mTaskExpirationQueue.peek().mTimeoutInstantMs) {
          TaskInfo info = mTaskExpirationQueue.remove();
          // Synchronize on mCurrentlyRunning to avoid a race condition where the thread being
          // interrupted finishes and takes on a new task before it is sent an interrupt. This is
          // enforced by also synchronizing on mCurrentlyRunning in afterExecute.
          synchronized (mCurrentlyRunning) {
            if (mCurrentlyRunning.contains(info.mRunnable)) {
              info.mThread.interrupt();
              mCurrentlyRunning.remove(info.mRunnable);
            }
          }
        }
        if (mTaskExpirationQueue.isEmpty()) {
          CommonUtils.sleepMs(mMaximumCheckIntervalMs);
        } else {
          long sleepTime = mTaskExpirationQueue.peek().mTimeoutInstantMs - System.currentTimeMillis();
          if (sleepTime > 0) {
            CommonUtils.sleepMs(sleepTime);
          }
        }
      }
    }
  }

  /**
   * Class for tracking information about a Runnable.
   */
  private class TaskInfo {
    /** The number of milliseconds past the epoch when the Runnable times out. */
    public final long mTimeoutInstantMs;
    /** The Runnable. */
    public final Runnable mRunnable;
    /** The thread running the Runnable. */
    public final Thread mThread;

    /**
     * @param thread the thread running the Runnable.
     * @param runnable the runnable
     */
    public TaskInfo(Thread thread, Runnable runnable) {
      mTimeoutInstantMs = System.currentTimeMillis() + mTimeoutMs;
      mRunnable = runnable;
      mThread = thread;
    }
  }
}
