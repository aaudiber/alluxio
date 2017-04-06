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

/**
 * Exception indicating that the operation was aborted, typically due to a concurrency issue like
 * sequencer check failures, transaction aborts, etc.
 *
 * See litmus test in {@link FailedPreconditionException} for deciding between
 * FailedPreconditionException, AbortedException, and UnavailableException.
 */
public class AbortedException extends AlluxioStatusException {
  private static final long serialVersionUID = -7705340466444818294L;
  private static final ExceptionStatus STATUS = ExceptionStatus.ABORTED;

  /**
   * @param message the exception message
   */
  public AbortedException(String message) {
    super(STATUS, message);
  }

  /**
   * @param cause the cause of the exception
   */
  public AbortedException(ExceptionStatus status, Throwable cause) {
    this(cause.getMessage(), cause);
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public AbortedException(String message, Throwable cause) {
    super(STATUS, message, cause);
  }
}
