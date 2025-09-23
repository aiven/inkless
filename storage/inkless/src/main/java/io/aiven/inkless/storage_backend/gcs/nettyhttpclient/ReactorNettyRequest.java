package io.aiven.inkless.storage_backend.gcs.nettyhttpclient;

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;

import java.io.IOException;
import java.net.URI;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import reactor.core.publisher.Flux;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;

import static io.netty.buffer.Unpooled.wrappedBuffer;

public class ReactorNettyRequest  extends LowLevelHttpRequest {

    private HttpClient client;
    private final String method;
    private final URI uri;
    private final HttpHeaders headers = new DefaultHttpHeaders();

    public ReactorNettyRequest(HttpClient client, String method, String url) {
        this.client = client;
        this.method = method;
        this.uri = URI.create(url);
    }

    @Override
    public void addHeader(String name, String value) {
        headers.add(name, value);
    }

    @Override
    public LowLevelHttpResponse execute() throws IOException {
        final HttpClient.ResponseReceiver<?> receiver;
        final ByteBuf buffer;
        switch (method) {
            case "POST":
            case "PUT":
            case "DELETE":
                if (getStreamingContent() != null) {
                    long contentLength = getContentLength();
                    buffer = wrappedBuffer(new byte[Math.toIntExact(contentLength)]);
                    buffer.resetWriterIndex();
                    try (ByteBufOutputStream out = new ByteBufOutputStream(buffer)) {
                        getStreamingContent().writeTo(out);
                    }

                    String contentType = getContentType();
                    String contentEncoding = getContentEncoding();

                    if (contentType != null) {
                        headers.set(HttpHeaderNames.CONTENT_TYPE.toString(), contentType);
                    }
                    if (contentEncoding != null) {
                        headers.set(HttpHeaderNames.CONTENT_ENCODING.toString(), contentEncoding);
                    }
                    if (contentLength >= 0) {
                        headers.set(HttpHeaderNames.CONTENT_LENGTH.toString(), Math.toIntExact(contentLength));
                    }
                } else {
                    buffer = wrappedBuffer(new byte[0]);
                }

                client = client.headers(cons -> {
                    cons.add(headers);
                });

                HttpClient.RequestSender sender;
                switch (method) {
                    case "POST":
                        sender = client.post();
                        break;
                    case "PUT":
                        sender = client.put();
                        break;
                    case "DELETE":
                        sender = client.delete();
                        break;
                    default:
                        throw new RuntimeException("unknown method");
                }
                receiver = sender.uri(uri).send(ByteBufFlux.fromInbound(Flux.just(buffer)));
                break;
            case "GET":
                client = client.headers(cons -> {
                    cons.add(headers);
                });
                receiver = client.get().uri(uri);
                break;
            default:
                throw new RuntimeException("Unsupported method " + method);
        }

        final ReactorNettyResponse block = receiver.responseSingle((response, content) -> {
            return content.map(bb -> {
                // Buffer is directly allocated, copy bytes to heap and
                // allow the Reactor framework to release the direct buffer
                // and free the memory for reuse.
                final byte[] clone = new byte[bb.readableBytes()];
                bb.readBytes(clone);
                return new ReactorNettyResponse(response, clone);
            }).defaultIfEmpty(new ReactorNettyResponse(response, null));
        }).block();
        if (block == null) {
            throw new RuntimeException("No response received.");
        }
        return block;
    }
}
