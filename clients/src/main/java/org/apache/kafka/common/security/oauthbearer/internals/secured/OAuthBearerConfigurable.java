/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.AppConfigurationEntry;

/**
 * Analogue to {@link Configurable} for OAuth-based authentication. This interface presents a similar
 * method signature as that of the {@link AuthenticateCallbackHandler} interface. However, this interface is
 * needed because {@link AuthenticateCallbackHandler} extends the JDK's {@link CallbackHandler} interface.
 *
 * <p/>
 *
 * <em>Note</em>:
 *
 * <ol>
 *   <li>
 *     Any class that <em>implements</em> this interface should initialize resources via
 *     {@link #configure(Map, String, List)} and release them via {@link #close()}.
 *   </li>
 *   <li>
 *     Any class that <em>instantiates</em> an object that implements {@code OAuthBearerConfigurable}
 *     must properly call that object's ({@link #configure(Map, String, List)} and {@link #close()}) methods
 *     so that the object can initialize and release resources.
 *   </li>
 * </ol>
 */
public interface OAuthBearerConfigurable extends Closeable {

    /**
     * Configures this object for the specified SASL mechanism.
     *
     * @param configs Key-value pairs containing the parsed configuration options of
     *        the client or broker. Note that these are the Kafka configuration options
     *        and not the JAAS configuration options. JAAS config options may be obtained
     *        from `jaasConfigEntries`. For configs that may be specified as both Kafka config
     *        as well as JAAS config (e.g. sasl.kerberos.service.name), the configuration
     *        is treated as invalid if conflicting values are provided.
     * @param saslMechanism Negotiated SASL mechanism. For clients, this is the SASL
     *        mechanism configured for the client. For brokers, this is the mechanism
     *        negotiated with the client and is one of the mechanisms enabled on the broker.
     * @param jaasConfigEntries JAAS configuration entries from the JAAS login context.
     *        This list contains a single entry for clients and may contain more than
     *        one entry for brokers if multiple mechanisms are enabled on a listener using
     *        static JAAS configuration where there is no mapping between mechanisms and
     *        login module entries. In this case, implementations can use the login module in
     *        `jaasConfigEntries` to identify the entry corresponding to `saslMechanism`.
     *        Alternatively, dynamic JAAS configuration option
     *        {@link org.apache.kafka.common.config.SaslConfigs#SASL_JAAS_CONFIG} may be
     *        configured on brokers with listener and mechanism prefix, in which case
     *        only the configuration entry corresponding to `saslMechanism` will be provided
     *        in `jaasConfigEntries`.
     */
    default void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {

    }

    /**
     * Closes any resources that were initialized by {@link #configure(Map, String, List)}.
     */
    default void close() throws IOException {
        // Do nothing...
    }
}