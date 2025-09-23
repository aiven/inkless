package io.aiven.inkless.storage_backend.gcs.nettyhttpclient;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;

import java.io.IOException;
import java.net.URI;

import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpHeaderNames;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

public final class ReactorNettyTransport extends HttpTransport {

    private static ReactorNettyTransport instance;

    private final URI uri;
    private final HttpClient client;
    private final LoopResources clientEventLoopGroup;
    private final ConnectionProvider connectionProvider;
    private boolean shutdown = false;

    private ReactorNettyTransport(final String endpoint) {
        uri = URI.create(endpoint);
        final int port;
        if (uri.getPort() == -1) {
            // No port defined, select by the scheme
            port = "https://".equals(uri.getScheme()) ? 80 : 443;
        } else {
            port = uri.getPort();
        }

        connectionProvider = ConnectionProvider.builder("custom")
                .maxConnections(96)
                .build();

        clientEventLoopGroup = LoopResources.create("gcs-netty-http", 32, true);

        client = HttpClient.create(connectionProvider)
                .runOn(clientEventLoopGroup)
                .host(uri.getHost())
                .port(uri.getPort())
                .keepAlive(true)
                .followRedirect(false)
                .compress(false)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .headers(cons -> cons.set(HttpHeaderNames.HOST, String.format("%s:%s", uri.getHost(), port)));
        client.warmup().block();
    }

    @Override
    protected LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
        return new ReactorNettyRequest(client, method, url);
    }

    @Override
    public void shutdown() throws IOException {
        if (!shutdown) {
            connectionProvider.disposeLater().block();
            clientEventLoopGroup.disposeLater().block();
            shutdown = true;
        }
    }

    /**
     * Returns whether the transport is shutdown or not.
     *
     * @return true if the transport is shutdown.
     * @since 1.44.0
     */
    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    public synchronized static ReactorNettyTransport get(String gcsUri) {
        if (instance == null) {
            instance = new ReactorNettyTransport(gcsUri);
        }
        return instance;
    }

}
