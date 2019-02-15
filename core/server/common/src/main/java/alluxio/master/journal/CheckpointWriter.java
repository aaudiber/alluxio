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

package alluxio.master.journal;

import java.io.Closeable;

/**
 * Writes Alluxio checkpoints. A checkpoint is an array of bytes. The first byte specifies the
 * checkpoint type, then the remaining bytes are interpreted according to the specific checkpoint
 * type.
 */
public interface CheckpointWriter extends Closeable {
}

