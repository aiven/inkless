package io.aiven.inkless.storage_backend.gcs.nettyhttpclient;

import com.google.api.client.http.LowLevelHttpResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import io.netty.handler.codec.http.HttpHeaderNames;
import reactor.netty.http.client.HttpClientResponse;

public class ReactorNettyResponse extends LowLevelHttpResponse {

    private final HttpClientResponse response;
    private final byte[] content;

    public ReactorNettyResponse(HttpClientResponse response, byte[] content) {
        this.response = response;
        if (content == null) {
            this.content = new byte[0];
        } else {
            this.content = content;
        }
    }

    @Override
    public InputStream getContent() throws IOException {
        return new ByteArrayInputStream(content);
    }

    @Override
    public String getContentEncoding() throws IOException {
        return response.responseHeaders().get(HttpHeaderNames.CONTENT_ENCODING);
    }

    @Override
    public long getContentLength() throws IOException {
        return content.length;
    }

    @Override
    public String getContentType() throws IOException {
        return response.responseHeaders().get(HttpHeaderNames.CONTENT_TYPE);
    }

    @Override
    public String getStatusLine() throws IOException {
        final StringBuilder buf = new StringBuilder();

        buf.append(response.version()).append(" ").append(getStatusCode()).append(" ");
        if (this.getReasonPhrase() != null) {
            buf.append(getReasonPhrase());
        }
        return buf.toString();
    }

    @Override
    public int getStatusCode() throws IOException {
        return response.status().code();
    }

    @Override
    public String getReasonPhrase() throws IOException {
        return response.status().reasonPhrase();
    }

    @Override
    public int getHeaderCount() throws IOException {
        return response.responseHeaders().size();
    }

    @Override
    public String getHeaderName(int index) throws IOException {
        return response.responseHeaders().entries().get(index).getKey();
    }

    @Override
    public String getHeaderValue(int index) throws IOException {
        return response.responseHeaders().entries().get(index).getValue();
    }
}
