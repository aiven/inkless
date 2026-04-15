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

package org.apache.kafka.kip1134.demo;

import org.apache.kafka.clients.admin.Admin;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory session store (not suitable for production).
 */
final class SessionRegistry {

    static final class Session {
        final Admin admin;
        final Properties clientProps;

        Session(Admin admin, Properties clientProps) {
            this.admin = admin;
            this.clientProps = clientProps;
        }
    }

    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

    String createSession(Admin admin, Properties clientProps) {
        String token = UUID.randomUUID().toString();
        sessions.put(token, new Session(admin, clientProps));
        return token;
    }

    Session get(String token) {
        if (token == null) {
            return null;
        }
        return sessions.get(token);
    }

    void remove(String token) {
        if (token == null) {
            return;
        }
        Session removed = sessions.remove(token);
        if (removed != null) {
            removed.admin.close();
        }
    }
}
