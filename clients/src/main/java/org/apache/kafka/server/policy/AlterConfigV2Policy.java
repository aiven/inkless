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
package org.apache.kafka.server.policy;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Map;
import java.util.Objects;

/**
 * An interface for enforcing a policy on alter configs requests with access to both
 * the existing and resulting configuration state.
 *
 * <p>If <code>alter.config.v2.policy.class.name</code> is defined, Kafka will create an instance
 * of the specified class using the default constructor and will then pass the broker configs to
 * its <code>configure()</code> method. During broker shutdown, the <code>close()</code> method will
 * be invoked so that resources can be released (if necessary).
 */
public interface AlterConfigV2Policy extends Configurable, AutoCloseable {

    /**
     * Class containing the alter config request parameters with before/after state.
     */
    class RequestMetadata {

        private final ConfigResource resource;
        private final Map<String, String> configsBefore;
        private final Map<String, String> configsAfter;

        /**
         * Create an instance of this class with the provided parameters.
         *
         * This constructor is public to make testing of <code>AlterConfigV2Policy</code>
         * implementations easier.
         */
        public RequestMetadata(ConfigResource resource,
                               Map<String, String> configsBefore,
                               Map<String, String> configsAfter) {
            this.resource = resource;
            this.configsBefore = configsBefore;
            this.configsAfter = configsAfter;
        }

        /**
         * Return the configs before the alteration.
         */
        public Map<String, String> configsBefore() {
            return configsBefore;
        }

        /**
         * Return the configs after the alteration is applied.
         */
        public Map<String, String> configsAfter() {
            return configsAfter;
        }

        /**
         * Return the resource being altered.
         */
        public ConfigResource resource() {
            return resource;
        }

        @Override
        public int hashCode() {
            return Objects.hash(resource, configsBefore, configsAfter);
        }

        @Override
        public boolean equals(Object o) {
            if ((o == null) || (!o.getClass().equals(getClass()))) return false;
            RequestMetadata other = (RequestMetadata) o;
            return resource.equals(other.resource) &&
                configsBefore.equals(other.configsBefore) &&
                configsAfter.equals(other.configsAfter);
        }

        @Override
        public String toString() {
            return "AlterConfigV2Policy.RequestMetadata(resource=" + resource +
                ", configsBefore=" + configsBefore +
                ", configsAfter=" + configsAfter + ")";
        }
    }

    /**
     * Validate the request parameters and throw a <code>PolicyViolationException</code>
     * with a suitable error message if the alter configs request parameters for the
     * provided resource do not satisfy this policy.
     *
     * @param requestMetadata the alter configs request parameters including before/after state
     *                        for the provided resource.
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validate(RequestMetadata requestMetadata) throws PolicyViolationException;
}
