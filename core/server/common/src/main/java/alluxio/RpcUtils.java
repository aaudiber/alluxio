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

import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.DependencyDoesNotExistException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FailedToCheckpointException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.LineageDeletionException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.exception.NoWorkerException;
import alluxio.exception.UfsBlockAccessTokenUnavailableException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.DataLossException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.InternalException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.OutOfRangeException;
import alluxio.exception.status.PermissionDeniedException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.exception.status.UnavailableException;
import alluxio.exception.status.UnknownException;
import alluxio.thrift.AlluxioTException;

import org.slf4j.Logger;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.concurrent.RejectedExecutionException;

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
    try {
      return callable.call();
    } catch (AlluxioStatusException e) {
      throw handleAlluxioStatusException(logger, callable, e);
    } catch (RuntimeException e) {
      throw handleAlluxioStatusException(logger, callable, convertRuntimeException(e));
    } catch (AlluxioException e) {
      throw handleAlluxioStatusException(logger, callable, convertAlluxioException(e));
    } catch (Exception e) {
      throw new UnknownException(e).toThrift();
    }
  }

  /**
   * Converts an {@link AlluxioStatusException} to an AlluxioTException and logs the appropriately
   * based on the type of exception.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param ase an {@link AlluxioStatusException}
   * @return the corresponding AlluxioTException
   */
  public static <T> AlluxioTException handleAlluxioStatusException(Logger logger,
      RpcCallable<T> callable, AlluxioStatusException ase) throws AlluxioTException {
    try {
      throw ase;
    } catch (InternalException | DataLossException e) {
      logger.error("{}", callable, e);
      throw e.toThrift();
    } catch (ResourceExhaustedException e) {
      logger.warn("{}", callable, e);
      throw e.toThrift();
    } catch (AlluxioStatusException e) {
      logger.debug("{}, Error={}", callable, e.getMessage());
      throw e.toThrift();
    }
  }

  /**
   * Converts runtime exceptions to Alluxio status exceptions. Well-known runtime exceptions are
   * converted intelligently. Unrecognized runtime exceptions are converted to
   * {@link UnknownException}.
   *
   * @param re the runtime exception to convert
   * @return the converted {@link AlluxioStatusException}
   */
  public static AlluxioStatusException convertRuntimeException(RuntimeException re) {
    try {
      throw re;
    } catch (ArithmeticException | ClassCastException | ConcurrentModificationException
        | IllegalStateException | NoSuchElementException | NullPointerException
        | UnsupportedOperationException e) {
      return new InternalException(e);
    } catch (IllegalArgumentException e) {
      return new InvalidArgumentException(e);
    } catch (IndexOutOfBoundsException e) {
      return new OutOfRangeException(e);
    } catch (RejectedExecutionException e) {
      return new ResourceExhaustedException(e);
    } catch (RuntimeException e) {
      return new UnknownException(e);
    }
  }

  /**
   * Converts checked Alluxio exceptions to Alluxio status exceptions.
   *
   * @param ae the Alluxio exception to convert
   * @return the converted {@link AlluxioStatusException}
   */
  public static AlluxioStatusException convertAlluxioException(AlluxioException ae) {
    try {
      throw ae;
    } catch (AccessControlException e) {
      return new PermissionDeniedException(e);
    } catch (BlockAlreadyExistsException | FileAlreadyCompletedException
        | FileAlreadyExistsException e) {
      return new AlreadyExistsException(e);
    } catch (BlockDoesNotExistException | FileDoesNotExistException
        | LineageDoesNotExistException e) {
      return new NotFoundException(e);
    } catch (BlockInfoException | InvalidFileSizeException | InvalidPathException e) {
      return new InvalidArgumentException(e);
    } catch (ConnectionFailedException | FailedToCheckpointException | NoWorkerException
        | UfsBlockAccessTokenUnavailableException e) {
      return new UnavailableException(e);
    } catch (DependencyDoesNotExistException | DirectoryNotEmptyException
        | InvalidWorkerStateException | LineageDeletionException e) {
      return new FailedPreconditionException(e);
    } catch (WorkerOutOfSpaceException e) {
      return new ResourceExhaustedException(e);
    } catch (AlluxioException e) {
      return new UnknownException(e);
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
     */
    T call() throws AlluxioException;
  }

  private RpcUtils() {} // prevent instantiation
}
