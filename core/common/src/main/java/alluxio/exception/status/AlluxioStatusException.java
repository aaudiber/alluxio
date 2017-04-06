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
 * An exception thrown by Alluxio. {@link #getStatus()} can be used to determine the represented
 * class of error.
 */
public class AlluxioStatusException extends RuntimeException {
  private static final long serialVersionUID = -7422144873058169662L;

  private final ExceptionStatus mStatus;

  /**
   * @param status the status code for this exception
   */
  public AlluxioStatusException(ExceptionStatus status) {
    mStatus = status;
  }

  /**
   * @return the status code for this exception
   */
  public ExceptionStatus getStatus() {
    return mStatus;
  }
}
