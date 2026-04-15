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

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * Developer-only HTTP server for the KIP-1134 virtual cluster demo UI.
 */
public final class Kip1134DemoMain {

    static final String SESSION_COOKIE = "KIP1134_SESSION";

    private Kip1134DemoMain() {
    }

    public static void main(String[] args) throws IOException {
        int port = resolvePort(args);
        SessionRegistry sessions = new SessionRegistry();
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", new DemoHttpHandler(sessions));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("KIP-1134 demo UI listening on http://127.0.0.1:" + port + "/");
    }

    private static int resolvePort(String[] args) {
        if (args.length > 0) {
            return Integer.parseInt(args[0]);
        }
        return Integer.parseInt(System.getProperty("kip1134.demo.port", "8080"));
    }
}
