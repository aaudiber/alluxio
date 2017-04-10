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

package alluxio;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.network.connection.ThriftClientPool;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The base class for clients that use {@link alluxio.network.connection.ThriftClientPool}.
 *
 * @param <C> the Alluxio service type
 */
public abstract class AbstractThriftClient<C extends AlluxioService.Client> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractThriftClient.class);

  protected static final int BASE_SLEEP_MS =
      Configuration.getInt(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS);
  protected static final int MAX_SLEEP_MS =
      Configuration.getInt(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);
  protected static final int RPC_MAX_NUM_RETRY =
      Configuration.getInt(PropertyKey.USER_RPC_RETRY_MAX_NUM_RETRY);

  /**
   * If the implementation of this function guarantees that the client returned will not
   * be returned to any other caller. Then this whole class is threadsafe.
   *
   * @return a Thrift service client
   */
  protected abstract C acquireClient() throws IOException;

  /**
   * @param client the client to release
   */
  protected abstract void releaseClient(C client);

  /**
   * The RPC to be executed in {@link #retryRPC(RpcCallable)}.
   *
   * @param <V> the return value of {@link #call(AlluxioService.Client)}
   * @param <C> the Alluxio service type
   */
  protected interface RpcCallable<V, C extends AlluxioService.Client> {
    /**
     * The task where RPC happens.
     *
     * @return RPC result
     * @throws TException when any exception defined in thrift happens
     */
    V call(C client) throws TException;
  }

  /**
   * Tries to execute an RPC defined as a {@link RpcCallable}.
   *
   * @param rpc the RPC call to be executed
   * @param <V> type of return value of the RPC call
   * @return the return value of the RPC call
   * @throws IOException when retries exceeds {@link #RPC_MAX_NUM_RETRY}
   */
  protected <V> V retryRPC(RpcCallable<V, C> rpc) throws IOException {
    TException exception;
    RetryPolicy retryPolicy =
        new ExponentialBackoffRetry(BASE_SLEEP_MS, MAX_SLEEP_MS, RPC_MAX_NUM_RETRY);
    do {
      C client = acquireClient();
      try {
        return rpc.call(client);
      } catch (AlluxioTException e) {
        AlluxioStatusException ase = AlluxioStatusException.fromThrift(e);
        processException(client, ase);
        exception = new TException(ase);
      } catch (TException e) {
        LOG.warn(e.getMessage());
        closeClient(client);
        exception = e;
      } finally {
        releaseClient(client);
      }
    } while (retryPolicy.attemptRetry());
    LOG.error("Failed after " + retryPolicy.getRetryCount() + " retries.");
    Preconditions.checkNotNull(exception);
    throw new IOException(exception);
  }

  /**
   * Do some processing based on the exception.
   *
   * @param client the client
   * @param e the exception
   * @throws E if the exception is not suppressed
   */
  protected <E extends Exception> void processException(C client, E e) throws E {
    throw e;
  }

  /**
   * Close the given client.
   *
   * @param client the client to close
   */
  private void closeClient(C client) {
    ThriftClientPool.closeThriftClient(client);
  }
}
