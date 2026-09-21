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
import java.util.Locale
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.server.config.{InklessControlPlaneConfigs => JInklessControlPlaneConfigs}

/**
 * Guards every dynamic `inkless.control.plane.*` broker property, before the value ever reaches
 * the metadata log.
 *
 * `ControllerConfigurationValidator` calls this from the `AlterConfigs`/`IncrementalAlterConfigs`
 * request path, before the controller appends the corresponding `ConfigRecord`. That is the only
 * point where rejecting the value is effective: `DynamicInklessControlPlaneConfig`, the
 * `BrokerReconfigurable` for these same keys, only sees the config after it has already been
 * committed and replicated.
 *
 * Kafka's outer config schema has no idea these are Postgres connection settings: they are
 * undeclared custom broker properties, so anything under the `inkless.control.plane.` prefix
 * passes the generic dynamic-config checks unchecked. Without an allowlist here, a request could
 * set `inkless.control.plane.password` (or `.read.password`, `.write.password`) directly, and
 * that value would land in a plaintext `ConfigRecord`, exactly what rejecting embedded
 * credentials in the connection string is meant to prevent.
 *
 * These keys must also stay on the default broker resource. A per-broker override would let that
 * one broker point at a different control plane database than the rest of the cluster, so brokers
 * could assign conflicting offsets or see different topic metadata and retention state.
 */
object InklessControlPlaneConnectionStringValidator {
  // pgjdbc reads the username and password from the URL as `user`/`password` query parameters,
  // and reads the private key's password, when the key itself is encrypted, as `sslpassword`.
  // Those are the only ways a connection string can carry a secret, so they are what has to stay
  // out of the metadata log.
  private val SensitiveConnectionParams: Set[String] = Set("user", "password", "sslpassword")

  private def embedsCredentials(connectionString: String): Boolean = {
    if (connectionString == null) {
      return false
    }
    val queryStart = connectionString.indexOf('?')
    if (queryStart < 0) {
      return false
    }
    connectionString.substring(queryStart + 1).split('&').exists { param =>
      val name = param.indexOf('=') match {
        case -1 => param
        case eq => param.substring(0, eq)
      }
      SensitiveConnectionParams.contains(name.toLowerCase(Locale.ROOT))
    }
  }

  def validate(resourceName: String, newConfigs: util.Map[String, String]): Unit = {
    newConfigs.forEach { (key, value) =>
      if (key.startsWith(JInklessControlPlaneConfigs.PREFIX) && value != null) {
        // Only the three connection strings may be set dynamically. Every other nested Postgres
        // property, most importantly the direct username/password fields, must be configured
        // statically so it goes through the broker's own startup-time handling instead of a
        // plaintext ConfigRecord.
        if (!JInklessControlPlaneConfigs.RECONFIGURABLE_CONFIGS.contains(key)) {
          throw new InvalidConfigurationException(
            s"$key cannot be set dynamically; only the Inkless control plane connection strings " +
              "may be changed at runtime")
        }
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
