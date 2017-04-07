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

import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioTException;

/**
 * An exception thrown by Alluxio. {@link #getStatus()} can be used to determine the represented
 * class of error.
 */
public class AlluxioStatusException extends AlluxioException {
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

  /**
   * Converts an Alluxio exception from Thrift representation to native representation.
   *
   * @param e the Alluxio Thrift exception
   * @return the native Alluxio exception
   */
  public static AlluxioStatusException fromThrift(AlluxioTException e) {
    String m = e.getMessage();
    switch (e.getStatus()) {
      case Canceled:
        return new CanceledException(m);
      case InvalidArgument:
        return new InvalidArgumentException(m);
      case DeadlineExceeded:
        return new DeadlineExceededException(m);
      case NotFound:
        return new NotFoundException(m);
      case AlreadyExists:
        return new AlreadyExistsException(m);
      case PermissionDenied:
        return new PermissionDeniedException(m);
      case Unauthenticated:
        return new UnauthenticatedException(m);
      case ResourceExhausted:
        return new ResourceExhaustedException(m);
      case FailedPrecondition:
        return new FailedPreconditionException(m);
      case Aborted:
        return new AbortedException(m);
      case OutOfRange:
        return new OutOfRangeException(m);
      case Unimplemented:
        return new UnimplementedException(m);
      case Internal:
        return new InternalException(m);
      case Unavailable:
        return new UnavailableException(m);
      case DataLoss:
        return new DataLossException(m);
      default:
        return new UnknownException(m);
    }
  }
}
