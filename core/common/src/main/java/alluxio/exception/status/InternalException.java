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
 * Exception representing an internal error. This means some invariant expected by the underlying
 * system has been broken. If you see one of these errors, something is very broken.
 */
public class InternalException extends AlluxioStatusException {
  private static final long serialVersionUID = -515848781797481231L;
  private static final ExceptionStatus STATUS = ExceptionStatus.INTERNAL;

  /**
   * @param message the exception message
   */
  public InternalException(String message) {
    super(STATUS, message);
  }

  /**
   * @param cause the cause of the exception
   */
  public InternalException(Throwable cause) {
    this(cause.getMessage(), cause);
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public InternalException(String message, Throwable cause) {
    super(STATUS, message, cause);
  }
}
