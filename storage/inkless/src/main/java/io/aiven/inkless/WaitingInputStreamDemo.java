package io.aiven.inkless;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.Executors;

public class WaitingInputStreamDemo {
    public static void main(String[] args) throws IOException, InterruptedException {
        final PipedInputStream in = new PipedInputStream();
        final PipedOutputStream out = new PipedOutputStream(in);

        Executors.newSingleThreadExecutor().submit(() -> {
            int read;
            byte[] bytes = new byte[1024];
            try {
                while ((read = in.read(bytes)) >= 0) {
                    System.out.printf("Read %d bytes%n", read);
                }
            } catch (final IOException e) {
                System.out.println("Error: " + e);
                throw new RuntimeException(e);
            }
            System.out.println("Done");
        });

        for (int i = 0; i < 100; i++) {
            out.write(new byte[i % 20]);
            Thread.sleep(100);
        }
        out.close();
    }
}
