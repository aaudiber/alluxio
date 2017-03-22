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

import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class TimeoutThreadPoolExecutor extends ThreadPoolExecutor {
  private final long mTimeoutMs;

  /**
   * Creates a new {@code TimeoutThreadPoolExecutor} with the given initial parameters and default
   * thread factory and rejected execution handler. Threads created by this executor will be
   * interrupted after the specified timeout.
   *
   * @param timeoutMs created threads will be interrupted after this many milliseconds
   * @param corePoolSize the number of threads to keep in the pool, even if they are idle, unless
   *        {@code allowCoreThreadTimeOut} is set
   * @param maximumPoolSize the maximum number of threads to allow in the pool
   * @throws IllegalArgumentException if one of the following holds:<br>
   *         {@code corePoolSize < 0}<br>
   *         {@code keepAliveTime < 0}<br>
   *         {@code maximumPoolSize <= 0}<br>
   *         {@code maximumPoolSize < corePoolSize}
   * @throws NullPointerException if {@code workQueue} is null
   */
  public TimeoutThreadPoolExecutor(long timeoutMs, int corePoolSize, int maximumPoolSize) {
    super(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
    mTimeoutMs = timeoutMs;
    mInterruptorService = Executors.newScheduledThreadPool(corePoolSize)
  }

  @Override
  public void execute(final Runnable command) {
    super.execute(command);
  }
}
