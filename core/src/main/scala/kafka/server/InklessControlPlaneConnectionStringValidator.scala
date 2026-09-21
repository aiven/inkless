/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.util
import org.apache.kafka.common.errors.InvalidConfigurationException

/**
 * Rejects an Inkless control plane connection string that embeds credentials or that targets a
 * specific broker, before the value ever reaches the metadata log.
 *
 * `ControllerConfigurationValidator` calls this from the `AlterConfigs`/`IncrementalAlterConfigs`
 * request path, before the controller appends the corresponding `ConfigRecord`. That is the only
 * point where rejecting the value is effective: `DynamicInklessControlPlaneConfig`, the
 * `BrokerReconfigurable` for these same keys, only sees the config after it has already been
 * committed and replicated.
 *
 * These keys must stay on the default broker resource. A per-broker override would let that one
 * broker point at a different control plane database than the rest of the cluster, so brokers
 * could assign conflicting offsets or see different topic metadata and retention state.
 */
object InklessControlPlaneConnectionStringValidator {
  // pgjdbc accepts credentials embedded as a `user=`/`password=` query parameter. That is the only
  // way a connection string can carry them, so this is what has to stay out of the metadata log.
  private val EmbeddedCredentialsPattern = java.util.regex.Pattern.compile("(?i)[?&](user|password)=")

  private def embedsCredentials(connectionString: String): Boolean =
    connectionString != null && EmbeddedCredentialsPattern.matcher(connectionString).find()

  def validate(resourceName: String, newConfigs: util.Map[String, String]): Unit = {
    DynamicInklessControlPlaneConfig.ReconfigurableConfigs.forEach { key =>
      val value = newConfigs.get(key)
      if (value != null) {
        // A per-broker override lets one broker point at a different control plane database than
        // the rest of the cluster, splitting offset assignment and topic metadata. Only the default
        // resource may set these keys.
        if (resourceName.nonEmpty) {
          throw new InvalidConfigurationException(
            s"$key can only be set on the default broker resource, not on broker $resourceName")
        }
        if (embedsCredentials(value)) {
          throw new InvalidConfigurationException(
            s"$key must not embed credentials in the connection string; configure username/password separately")
        }
      }
    }
  }
}
