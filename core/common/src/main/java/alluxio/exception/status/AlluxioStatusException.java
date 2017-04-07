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

package alluxio.exception.status;

import alluxio.thrift.AlluxioTException;

/**
 * An exception thrown by Alluxio. {@link #getStatus()} can be used to determine the represented
 * class of error.
 */
public class AlluxioStatusException extends RuntimeException {
  private static final long serialVersionUID = -7422144873058169662L;

  private final ExceptionStatus mStatus;

  /**
   * @param status the status code for this exception
   * @param message the exception message
   */
  public AlluxioStatusException(ExceptionStatus status, String message) {
    super(message);
    mStatus = status;
  }

  /**
   * @param status the status code for this exception
   * @param cause the cause of the exception
   */
  public AlluxioStatusException(ExceptionStatus status, Throwable cause) {
    this(status, cause.getMessage(), cause);
  }

  /**
   * @param status the status code for this exception
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public AlluxioStatusException(ExceptionStatus status, String message, Throwable cause) {
    super(message, cause);
    mStatus = status;
  }

  /**
   * @return the status code for this exception
   */
  public ExceptionStatus getStatus() {
    return mStatus;
  }

  public AlluxioTException toThrift() {
    return new AlluxioTException(getMessage(), ExceptionStatus.toThrift(mStatus));
  }

  /**
   * Converts an Alluxio exception from Thrift representation to native representation.
   *
   * @param e the Alluxio Thrift exception
   * @return the native Alluxio exception
   */
  public static AlluxioStatusException fromThrift(AlluxioTException e) {
    String m = e.getMessage();
    switch (e.getStatus()) {
      case CANCELED:
        return new CanceledException(m);
      case INVALID_ARGUMENT:
        return new InvalidArgumentException(m);
      case DEADLINE_EXCEEDED:
        return new DeadlineExceededException(m);
      case NOT_FOUND:
        return new NotFoundException(m);
      case ALREADY_EXISTS:
        return new AlreadyExistsException(m);
      case PERMISSION_DENIED:
        return new PermissionDeniedException(m);
      case UNAUTHENTICATED:
        return new UnauthenticatedException(m);
      case RESOURCE_EXHAUSTED:
        return new ResourceExhaustedException(m);
      case FAILED_PRECONDITION:
        return new FailedPreconditionException(m);
      case ABORTED:
        return new AbortedException(m);
      case OUT_OF_RANGE:
        return new OutOfRangeException(m);
      case UNIMPLEMENTED:
        return new UnimplementedException(m);
      case INTERNAL:
        return new InternalException(m);
      case UNAVAILABLE:
        return new UnavailableException(m);
      case DATA_LOSS:
        return new DataLossException(m);
      default:
        return new UnknownException(m);
    }
  }
}
