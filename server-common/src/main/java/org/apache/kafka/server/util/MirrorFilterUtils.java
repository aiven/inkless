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
package org.apache.kafka.server.util;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourceType;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class MirrorFilterUtils {
    private MirrorFilterUtils() {}

    /**
     * Compiles a list of regex pattern strings into a single {@link Pattern} by joining them with {@code |}.
     *
     * @param patterns the list of regex pattern strings
     * @return a compiled Pattern that matches any of the given patterns, or null if no non-empty patterns
     */
    public static Pattern compilePatternList(List<String> patterns) {
        String combined = patterns.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining("|"));
        return combined.isEmpty() ? null : Pattern.compile("^(" + combined + ")$");
    }

    /**
     * Parses a list of ACL rule strings into a list of {@link AclRule} instances.
     *
     * @param rules the list of rule strings in semicolon-separated format
     * @return parsed list of AclRule instances
     */
    public static List<AclRule> parseAclRules(List<String> rules) {
        return rules.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(AclRule::parse)
                .toList();
    }

    /**
     * Represents an ACL include rule parsed from the semicolon-separated format:
     * resourceType;resourceName;operation;permissionType;principal
     *
     * Each field uses * as a wildcard (match all). The resourceName and principal
     * fields support regex patterns. Trailing wildcard fields can be omitted.
     *
     * Valid resourceType values: TOPIC, GROUP, CLUSTER, TRANSACTIONAL_ID, DELEGATION_TOKEN, USER.
     * Valid operation values: READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE, CLUSTER_ACTION,
     * DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, ALL, etc.
     * Valid permissionType values: ALLOW, DENY.
     *
     * Examples:
     * <pre>
     * *                                       - match all ACLs (default)
     * TOPIC;orders.*                          - all ACLs for topics matching orders.*
     * *;*;*;*;User:alice                      - all ACLs for principal User:alice
     * *;*;*;*;User:app-.*                     - all ACLs for principals matching User:app-.*
     * TOPIC;*;READ;ALLOW                      - all topic READ/ALLOW ACLs
     * GROUP;consumer-.*;READ;ALLOW;User:bob   - READ/ALLOW ACLs on groups matching consumer-.* for User:bob
     * </pre>
     *
     * Config usage example:
     * <pre>
     * mirror.acls.include=TOPIC;orders.*, *;*;*;*;User:alice
     * </pre>
     *
     * @param resourceType the resource type to match, or null for wildcard
     * @param resourceNamePattern regex pattern for the resource name, or null for wildcard
     * @param operation the ACL operation to match, or null for wildcard
     * @param permissionType the ACL permission type to match, or null for wildcard
     * @param principalPattern regex pattern for the principal, or null for wildcard
     */
    public record AclRule(
            ResourceType resourceType,
            Pattern resourceNamePattern,
            AclOperation operation,
            AclPermissionType permissionType,
            Pattern principalPattern
    ) {
        /**
         * Parses a semicolon-separated rule string into an AclRule.
         *
         * @param rule the rule string (e.g., "TOPIC;orders.*;READ;ALLOW;User:alice")
         * @return the parsed AclRule
         */
        public static AclRule parse(String rule) {
            String[] parts = rule.trim().split(";", -1);

            ResourceType resourceType = null;
            Pattern resourceNamePattern = null;
            AclOperation operation = null;
            AclPermissionType permissionType = null;
            Pattern principalPattern = null;

            if (parts.length >= 1 && !"*".equals(parts[0].trim())) {
                resourceType = ResourceType.valueOf(parts[0].trim().toUpperCase(Locale.ROOT));
            }
            if (parts.length >= 2 && !"*".equals(parts[1].trim())) {
                resourceNamePattern = Pattern.compile(parts[1].trim());
            }
            if (parts.length >= 3 && !"*".equals(parts[2].trim())) {
                operation = AclOperation.valueOf(parts[2].trim().toUpperCase(Locale.ROOT));
            }
            if (parts.length >= 4 && !"*".equals(parts[3].trim())) {
                permissionType = AclPermissionType.valueOf(parts[3].trim().toUpperCase(Locale.ROOT));
            }
            if (parts.length >= 5 && !"*".equals(parts[4].trim())) {
                principalPattern = Pattern.compile(parts[4].trim());
            }

            return new AclRule(resourceType, resourceNamePattern, operation, permissionType, principalPattern);
        }

        /**
         * Tests whether the given AclBinding matches this rule.
         * A null field acts as a wildcard and matches any value.
         *
         * @param binding the ACL binding to test
         * @return true if the binding matches all non-wildcard fields of this rule
         */
        public boolean matches(AclBinding binding) {
            if (resourceType != null && binding.pattern().resourceType() != resourceType) {
                return false;
            }
            if (resourceNamePattern != null && !resourceNamePattern.matcher(binding.pattern().name()).matches()) {
                return false;
            }
            if (operation != null && binding.entry().operation() != operation) {
                return false;
            }
            if (permissionType != null && binding.entry().permissionType() != permissionType) {
                return false;
            }
            if (principalPattern != null && !principalPattern.matcher(binding.entry().principal()).matches()) {
                return false;
            }
            return true;
        }
    }
}
