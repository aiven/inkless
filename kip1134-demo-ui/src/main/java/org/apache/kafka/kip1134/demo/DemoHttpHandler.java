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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterVirtualClustersOptions;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DescribeVirtualClustersOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListVirtualClustersOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.NewVirtualCluster;
import org.apache.kafka.clients.admin.VirtualClusterDescription;
import org.apache.kafka.clients.admin.VirtualClusterResourceAlteration;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

/**
 * Serves static assets and JSON API for the demo UI.
 */
final class DemoHttpHandler implements HttpHandler {

    private static final int MAX_BODY_BYTES = 512 * 1024;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SessionRegistry sessions;

    DemoHttpHandler(SessionRegistry sessions) {
        this.sessions = sessions;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if ("OPTIONS".equals(method)) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        String path = exchange.getRequestURI().getPath();
        if (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        try {
            if (path.startsWith("/api/")) {
                handleApi(exchange, method, path);
            } else {
                handleStatic(exchange, path);
            }
        } catch (Exception e) {
            sendJson(exchange, 500, errorJson(formatError(e)));
        }
    }

    private static void handleStatic(HttpExchange exchange, String path) throws IOException {
        if ("/".equals(path) || "/index.html".equals(path)) {
            sendResource(exchange, "static/index.html", "text/html; charset=utf-8");
            return;
        }
        if ("/app.js".equals(path)) {
            sendResource(exchange, "static/app.js", "application/javascript; charset=utf-8");
            return;
        }
        sendJson(exchange, 404, errorJson("Not found"));
    }

    private void handleApi(HttpExchange exchange, String method, String path) throws IOException {
        String route = method + " " + path;
        switch (route) {
            case "GET /api/session":
                handleSessionGet(exchange);
                return;
            case "POST /api/session":
                handleSessionPost(exchange);
                return;
            case "DELETE /api/session":
                handleSessionDelete(exchange);
                return;
            default:
                handleApiAuthenticated(exchange, route);
        }
    }

    private void handleApiAuthenticated(HttpExchange exchange, String route) throws IOException {
        SessionRegistry.Session session = requireSession(exchange);
        if (session == null) {
            return;
        }
        switch (route) {
            case "GET /api/topics":
                handleTopicsList(exchange, session);
                return;
            case "POST /api/topics":
                handleTopicsCreate(exchange, session);
                return;
            case "POST /api/topics/delete":
                handleTopicsDelete(exchange, session);
                return;
            case "GET /api/virtual-clusters":
                handleVcList(exchange, session);
                return;
            case "POST /api/virtual-clusters":
                handleVcCreate(exchange, session);
                return;
            case "POST /api/virtual-clusters/delete":
                handleVcDelete(exchange, session);
                return;
            case "POST /api/virtual-clusters/describe":
                handleVcDescribe(exchange, session);
                return;
            case "POST /api/virtual-clusters/alter":
                handleVcAlter(exchange, session);
                return;
            default:
                sendJson(exchange, 404, errorJson("Unknown API path"));
        }
    }

    private void handleSessionGet(HttpExchange exchange) throws IOException {
        String token = sessionCookie(exchange);
        SessionRegistry.Session s = token == null ? null : sessions.get(token);
        ObjectNode body = MAPPER.createObjectNode();
        body.put("connected", s != null);
        if (s != null) {
            ObjectNode props = MAPPER.createObjectNode();
            s.clientProps.stringPropertyNames().forEach(k -> props.put(k, s.clientProps.getProperty(k)));
            body.set("clientProps", props);
        }
        sendJson(exchange, 200, body);
    }

    private void handleSessionPost(HttpExchange exchange) throws IOException {
        JsonNode root = readJsonBody(exchange);
        String bootstrap = text(root, "bootstrapServers");
        if (bootstrap != null) {
            bootstrap = bootstrap.trim();
        }
        if (bootstrap == null || bootstrap.isEmpty()) {
            sendJson(exchange, 400, errorJson("bootstrapServers is required"));
            return;
        }

        String existing = sessionCookie(exchange);
        if (existing != null) {
            sessions.remove(existing);
        }

        Properties props = new Properties();
        JsonNode extra = root.path("clientProps");
        if (extra.isObject()) {
            extra.fieldNames().forEachRemaining(k -> props.put(k, extra.get(k).asText()));
        }
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

        Admin admin;
        try {
            admin = AdminClient.create(props);
        } catch (Exception e) {
            sendJson(exchange, 400, errorJson("Failed to create Admin client: " + formatError(e)));
            return;
        }

        String token = sessions.createSession(admin, copyProps(props));
        exchange.getResponseHeaders().add(
            "Set-Cookie",
            Kip1134DemoMain.SESSION_COOKIE + "=" + token + "; Path=/; HttpOnly; SameSite=Lax"
        );
        ObjectNode body = MAPPER.createObjectNode();
        body.put("connected", true);
        sendJson(exchange, 200, body);
    }

    private void handleSessionDelete(HttpExchange exchange) throws IOException {
        String token = sessionCookie(exchange);
        if (token != null) {
            sessions.remove(token);
        }
        exchange.getResponseHeaders().add("Set-Cookie", Kip1134DemoMain.SESSION_COOKIE + "=; Path=/; Max-Age=0");
        ObjectNode body = MAPPER.createObjectNode();
        body.put("connected", false);
        sendJson(exchange, 200, body);
    }

    private void handleTopicsList(HttpExchange exchange, SessionRegistry.Session session) throws IOException {
        try {
            TreeSet<String> names = new TreeSet<>(session.admin.listTopics(new ListTopicsOptions()).names().get());
            ArrayNode arr = MAPPER.createArrayNode();
            names.forEach(arr::add);
            ObjectNode body = MAPPER.createObjectNode();
            body.set("topics", arr);
            sendJson(exchange, 200, body);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendJson(exchange, 400, errorJson("Interrupted"));
        } catch (ExecutionException e) {
            sendJson(exchange, 400, errorJson(formatError(e)));
        }
    }

    private void handleTopicsCreate(HttpExchange exchange, SessionRegistry.Session session) throws IOException {
        JsonNode root = readJsonBody(exchange);
        String name = text(root, "name");
        if (name == null || name.isEmpty()) {
            sendJson(exchange, 400, errorJson("name is required"));
            return;
        }
        int partitions = root.path("partitions").asInt(1);
        short rf = (short) root.path("replicationFactor").asInt(1);
        try {
            session.admin.createTopics(
                List.of(new NewTopic(name, partitions, rf)),
                new CreateTopicsOptions()
            ).all().get();
            sendJson(exchange, 200, MAPPER.createObjectNode().put("ok", true));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendJson(exchange, 400, errorJson("Interrupted"));
        } catch (ExecutionException e) {
            sendJson(exchange, 400, errorJson(formatError(e)));
        }
    }

    private void handleTopicsDelete(HttpExchange exchange, SessionRegistry.Session session) throws IOException {
        JsonNode root = readJsonBody(exchange);
        JsonNode namesNode = root.path("names");
        if (!namesNode.isArray() || namesNode.isEmpty()) {
            sendJson(exchange, 400, errorJson("names must be a non-empty array"));
            return;
        }
        List<String> names = new ArrayList<>();
        namesNode.forEach(n -> names.add(n.asText()));
        try {
            session.admin.deleteTopics(TopicCollection.ofTopicNames(names), new DeleteTopicsOptions()).all().get();
            sendJson(exchange, 200, MAPPER.createObjectNode().put("ok", true));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendJson(exchange, 400, errorJson("Interrupted"));
        } catch (ExecutionException e) {
            sendJson(exchange, 400, errorJson(formatError(e)));
        }
    }

    private void handleVcList(HttpExchange exchange, SessionRegistry.Session session) throws IOException {
        try {
            Collection<String> list = session.admin.listVirtualClusters(new ListVirtualClustersOptions()).names().get();
            ArrayNode arr = MAPPER.createArrayNode();
            new TreeSet<>(list).forEach(arr::add);
            ObjectNode body = MAPPER.createObjectNode();
            body.set("virtualClusters", arr);
            sendJson(exchange, 200, body);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendJson(exchange, 400, errorJson("Interrupted"));
        } catch (ExecutionException e) {
            sendJson(exchange, 400, errorJson(formatError(e)));
        }
    }

    private void handleVcCreate(HttpExchange exchange, SessionRegistry.Session session) throws IOException {
        JsonNode root = readJsonBody(exchange);
        String name = text(root, "name");
        if (name == null || name.isEmpty()) {
            sendJson(exchange, 400, errorJson("name is required"));
            return;
        }
        try {
            session.admin.createVirtualClusters(List.of(new NewVirtualCluster(name))).all().get();
            sendJson(exchange, 200, MAPPER.createObjectNode().put("ok", true));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendJson(exchange, 400, errorJson("Interrupted"));
        } catch (ExecutionException e) {
            sendJson(exchange, 400, errorJson(formatError(e)));
        }
    }

    private void handleVcDelete(HttpExchange exchange, SessionRegistry.Session session) throws IOException {
        JsonNode root = readJsonBody(exchange);
        JsonNode namesNode = root.path("names");
        if (!namesNode.isArray() || namesNode.isEmpty()) {
            sendJson(exchange, 400, errorJson("names must be a non-empty array"));
            return;
        }
        List<String> names = new ArrayList<>();
        namesNode.forEach(n -> names.add(n.asText()));
        try {
            session.admin.deleteVirtualClusters(names).all().get();
            sendJson(exchange, 200, MAPPER.createObjectNode().put("ok", true));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendJson(exchange, 400, errorJson("Interrupted"));
        } catch (ExecutionException e) {
            sendJson(exchange, 400, errorJson(formatError(e)));
        }
    }

    private void handleVcDescribe(HttpExchange exchange, SessionRegistry.Session session) throws IOException {
        JsonNode root = readJsonBody(exchange);
        JsonNode namesNode = root.path("names");
        if (!namesNode.isArray() || namesNode.isEmpty()) {
            sendJson(exchange, 400, errorJson("names must be a non-empty array"));
            return;
        }
        List<String> names = new ArrayList<>();
        namesNode.forEach(n -> names.add(n.asText()));
        try {
            Map<String, KafkaFuture<VirtualClusterDescription>> futures =
                session.admin.describeVirtualClusters(names, new DescribeVirtualClustersOptions()).values();
            ObjectNode out = MAPPER.createObjectNode();
            for (String name : names) {
                KafkaFuture<VirtualClusterDescription> f = futures.get(name);
                if (f == null) {
                    continue;
                }
                VirtualClusterDescription d = f.get();
                ObjectNode vc = MAPPER.createObjectNode();
                vc.put("name", d.name());
                ObjectNode links = MAPPER.createObjectNode();
                d.topicLinks().forEach(links::put);
                vc.set("topicLinks", links);
                ArrayNode users = MAPPER.createArrayNode();
                d.users().forEach(users::add);
                vc.set("users", users);
                ArrayNode groups = MAPPER.createArrayNode();
                d.consumerGroups().forEach(groups::add);
                vc.set("consumerGroups", groups);
                out.set(name, vc);
            }
            ObjectNode body = MAPPER.createObjectNode();
            body.set("descriptions", out);
            sendJson(exchange, 200, body);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendJson(exchange, 400, errorJson("Interrupted"));
        } catch (ExecutionException e) {
            sendJson(exchange, 400, errorJson(formatError(e)));
        }
    }

    private void handleVcAlter(HttpExchange exchange, SessionRegistry.Session session) throws IOException {
        JsonNode root = readJsonBody(exchange);
        String vcName = text(root, "vcName");
        if (vcName == null || vcName.isEmpty()) {
            sendJson(exchange, 400, errorJson("vcName is required"));
            return;
        }
        JsonNode changes = root.path("changes");
        if (!changes.isArray()) {
            sendJson(exchange, 400, errorJson("changes must be an array"));
            return;
        }
        List<VirtualClusterResourceAlteration> alts = new ArrayList<>();
        try {
            for (JsonNode ch : changes) {
                String type = text(ch, "type");
                String op = text(ch, "op");
                String name = text(ch, "name");
                if (type == null || op == null || name == null) {
                    sendJson(exchange, 400, errorJson("Each change needs type, op, and name"));
                    return;
                }
                byte resourceType = parseResourceType(type);
                byte resourceOp = parseResourceOp(op);
                String physical = ch.has("physicalTopic") && !ch.get("physicalTopic").isNull()
                    ? ch.get("physicalTopic").asText(null)
                    : null;
                alts.add(new VirtualClusterResourceAlteration(resourceType, resourceOp, name, physical));
            }
        } catch (IllegalArgumentException e) {
            sendJson(exchange, 400, errorJson(e.getMessage()));
            return;
        }
        try {
            session.admin.alterVirtualClusters(Map.of(vcName, alts), new AlterVirtualClustersOptions()).all().get();
            sendJson(exchange, 200, MAPPER.createObjectNode().put("ok", true));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendJson(exchange, 400, errorJson("Interrupted"));
        } catch (ExecutionException e) {
            sendJson(exchange, 400, errorJson(formatError(e)));
        }
    }

    private static byte parseResourceType(String type) {
        switch (type) {
            case "user":
                return 0;
            case "topicLink":
                return 1;
            case "group":
                return 2;
            default:
                throw new IllegalArgumentException("Unknown resource type: " + type);
        }
    }

    private static byte parseResourceOp(String op) {
        switch (op) {
            case "add":
                return 0;
            case "remove":
                return 1;
            default:
                throw new IllegalArgumentException("Unknown operation: " + op);
        }
    }

    private SessionRegistry.Session requireSession(HttpExchange exchange) throws IOException {
        String token = sessionCookie(exchange);
        SessionRegistry.Session s = token == null ? null : sessions.get(token);
        if (s == null) {
            sendJson(exchange, 401, errorJson("Not connected; POST /api/session first"));
            return null;
        }
        return s;
    }

    private static String sessionCookie(HttpExchange exchange) {
        String raw = exchange.getRequestHeaders().getFirst("Cookie");
        if (raw == null) {
            return null;
        }
        String prefix = Kip1134DemoMain.SESSION_COOKIE + "=";
        for (String part : raw.split(";")) {
            String t = part.trim();
            if (t.startsWith(prefix)) {
                return t.substring(prefix.length());
            }
        }
        return null;
    }

    private static JsonNode readJsonBody(HttpExchange exchange) throws IOException {
        byte[] buf = readBody(exchange);
        if (buf.length == 0) {
            return MAPPER.createObjectNode();
        }
        return MAPPER.readTree(buf);
    }

    private static byte[] readBody(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            byte[] buf = in.readAllBytes();
            if (buf.length > MAX_BODY_BYTES) {
                throw new IOException("Request body too large");
            }
            return buf;
        }
    }

    private static String text(JsonNode node, String field) {
        if (node == null || !node.has(field) || node.get(field).isNull()) {
            return null;
        }
        String v = node.get(field).asText();
        return v == null || v.isEmpty() ? null : v;
    }

    private static Properties copyProps(Properties p) {
        Properties copy = new Properties();
        copy.putAll(p);
        return copy;
    }

    private static ObjectNode errorJson(String message) {
        return MAPPER.createObjectNode().put("error", message);
    }

    private static String formatError(Throwable t) {
        Throwable c = t instanceof ExecutionException ? t.getCause() : t;
        String msg = c.getMessage();
        return msg != null ? msg : c.toString();
    }

    private static void sendJson(HttpExchange exchange, int status, ObjectNode json) throws IOException {
        byte[] data = MAPPER.writeValueAsBytes(json);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }

    private static void sendResource(HttpExchange exchange, String classpath, String contentType) throws IOException {
        try (InputStream in = DemoHttpHandler.class.getClassLoader().getResourceAsStream(classpath)) {
            if (in == null) {
                sendJson(exchange, 404, errorJson("Missing resource: " + classpath));
                return;
            }
            byte[] data = in.readAllBytes();
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.sendResponseHeaders(200, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        }
    }
}
