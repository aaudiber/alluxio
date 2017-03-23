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

import static org.junit.Assert.assertFalse;

import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Function;
import org.junit.Test;

/**
 * Unit tests for {@link TimeoutThreadPoolExecutor}.
 */
public final class TimeoutThreadPoolExecutorTest {

  @Test
  public void interruptTimedOutThread() {
    long timeoutMs = 50;
    long maxCheckInterval = 10;
    int numThreads = 1;
    TimeoutThreadPoolExecutor executor =
        new TimeoutThreadPoolExecutor(timeoutMs, maxCheckInterval, numThreads, numThreads);
    final InterruptionChecker checker = new InterruptionChecker(Integer.MAX_VALUE);
    executor.execute(checker);
    CommonUtils.waitFor("Thread to be interrupted", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return checker.isInterrupted();
      }
    }, WaitForOptions.defaults().setTimeout(200));
  }

  @Test
  public void doNotInterruptQuickThreads() throws Exception {
    long timeoutMs = 50;
    long maxCheckInterval = 10;
    int numThreads = 50;
    TimeoutThreadPoolExecutor executor =
        new TimeoutThreadPoolExecutor(timeoutMs, maxCheckInterval, numThreads, numThreads);
    for (int i = 0; i < 1000; i++) {
      InterruptionChecker checker = new InterruptionChecker(0);
      executor.submit(checker).get();
      assertFalse(checker.isInterrupted());
    }
  }

  @Test
  public void sustainHangingThreads() throws Exception {
    long timeoutMs = 50;
    long maxCheckInterval = 10;
    int numThreads = 100;
    TimeoutThreadPoolExecutor executor =
        new TimeoutThreadPoolExecutor(timeoutMs, maxCheckInterval, numThreads, numThreads);
    // Submit 25 batches of 20 threads which will hang. Sleep for half of the thread timeout so that
    // the thread pool has a chance to kill timed-out threads before running out of its 100 threads.
    for (int i = 0; i < 25; i++) {
      for (int j = 0; j < 20; j++) {
        executor.submit(new InterruptionChecker(Integer.MAX_VALUE));
      }
      CommonUtils.sleepMs(timeoutMs / 2);
    }
  }

  private class InterruptionChecker implements Runnable {
    private final long mSleepTimeMs;
    private boolean mInterrupted = false;

    /**
     * @param sleepTimeMs how long to sleep before terminating
     */
    public InterruptionChecker(long sleepTimeMs) {
      mSleepTimeMs = sleepTimeMs;
    }

    @Override
    public void run() {
      CommonUtils.sleepMs(mSleepTimeMs);
      if (Thread.interrupted()) {
        mInterrupted = true;
      }
    }

    /**
     * @return whether this Runnable was interrupted
     */
    public boolean isInterrupted() {
      return mInterrupted;
    }
  }
}
