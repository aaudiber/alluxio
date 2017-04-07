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

package alluxio.exception;

import javax.annotation.concurrent.ThreadSafe;

/**
 * All exceptions thrown from the Alluxio native client extend this type.
 */
@ThreadSafe
public class AlluxioException extends Exception {
  private static final long serialVersionUID = 2243833925609642384L;

  /**
   * @param message the exception message
   */
  public AlluxioException(String message) {
    super(message);
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public AlluxioException(String message, Throwable cause) {
    super(message, cause);
  }
}
