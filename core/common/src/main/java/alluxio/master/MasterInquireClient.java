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

package alluxio.master;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Client for determining the primary master.
 */
public interface MasterInquireClient {
  /**
   * @return the rpc address of the primary master, or null if none can be determined
   */
  @Nullable
  InetSocketAddress getPrimaryRpcAddress();

  /**
   * @return a list of all masters' RPC addresses
   */
  List<InetSocketAddress> getMasterRpcAddresses();

  /**
   * Factory for getting a master inquire client.
   */
  class Factory {
    public static MasterInquireClient create() {
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return ZkMasterInquireClient.getClient(Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS),
            Configuration.get(PropertyKey.ZOOKEEPER_ELECTION_PATH),
            Configuration.get(PropertyKey.ZOOKEEPER_LEADER_PATH));
      } else {
        return new SingleMasterInquireClient(
            NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));
      }
    }

    private Factory() {} // Not intended for instantiation.
  }
}