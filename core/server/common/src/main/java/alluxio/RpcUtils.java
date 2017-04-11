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

import alluxio.exception.AlluxioException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnknownException;
import alluxio.thrift.AlluxioTException;

import org.slf4j.Logger;

import java.io.IOException;

/**
 * Utilities for handling RPC calls.
 */
public final class RpcUtils {
  /**
   * Calls the given {@link RpcCallable} and handles any exceptions thrown.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param <T> the return type of the callable
   * @return the return value from calling the callable
   * @throws AlluxioTException if the callable throws an exception
   */
  public static <T> T call(Logger logger, RpcCallable<T> callable) throws AlluxioTException {
    AlluxioStatusException ase;
    try {
      return callable.call();
    } catch (AlluxioStatusException e) {
      ase = e;
    } catch (RuntimeException e) {
      ase = AlluxioStatusException.fromRuntimeException(e);
    } catch (AlluxioException e) {
      ase = AlluxioStatusException.fromAlluxioException(e);
    } catch (IOException e) {
      ase = AlluxioStatusException.fromIOException(e);
    } catch (Exception e) {
      ase = new UnknownException(e);
    }
    throw handleAlluxioStatusException(logger, callable, ase);
  }

  /**
   * Converts an {@link AlluxioStatusException} to an AlluxioTException and logs the appropriately
   * based on the type of exception.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param e an {@link AlluxioStatusException}
   * @return the corresponding AlluxioTException
   */
  public static AlluxioTException handleAlluxioStatusException(Logger logger,
      RpcCallable<?> callable, AlluxioStatusException e) {
    switch (e.getStatus()) {
      case INTERNAL:
      case DATA_LOSS:
        logger.error("{}", callable, e);
        return e.toThrift();
      case RESOURCE_EXHAUSTED:
        logger.warn("{}", callable, e);
        return e.toThrift();
      default:
        logger.debug("{}, Error={}", callable, e.getMessage());
        return e.toThrift();
    }
  }

  /**
   * Calls the given {@link RpcCallable} and handles any exceptions thrown. The callable should
   * implement a toString with the following format: "CallName: arg1=value1, arg2=value2,...".
   * The toString will be used to log enter and exit information with debug logging is enabled.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param <T> the return type of the callable
   * @return the return value from calling the callable
   * @throws AlluxioTException if the callable throws an Alluxio or runtime exception
   */
  public static <T> T callAndLog(Logger logger, RpcCallable<T> callable) throws AlluxioTException {
    logger.debug("Enter: {}", callable);
    try {
      T ret = call(logger, callable);
      logger.debug("Exit (OK): {}", callable);
      return ret;
    } catch (AlluxioTException e) {
      logger.debug("Exit (Error): {}, Error={}", callable, e.getMessage());
      throw e;
    }
  }

  /**
   * @param <T> the return type of the callable
   */
  public interface RpcCallable<T> {
    /**
     * The RPC implementation.
     *
     * @return the return value from the RPC
     * @throws AlluxioException if a checked Alluxio exception occurs
     * @throws IOException if an I/O exception occurs
     */
    T call() throws AlluxioException, IOException;
  }

  private RpcUtils() {} // prevent instantiation
}
