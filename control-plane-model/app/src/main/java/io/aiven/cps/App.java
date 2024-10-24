package io.aiven.cps;

import java.io.IOException;

import io.aiven.cps.broker.BatchBuffer;
import io.aiven.cps.common.FindBatchRequest;
import io.aiven.cps.common.FindBatchResponse;
import io.aiven.cps.control_plane.ControlPlane;

public class App {
    public static void main(String[] args) throws IOException {
        ControlPlane controlPlane = new ControlPlane();

        BatchBuffer file1 = new BatchBuffer("file1");
        file1.addBatch("topic1", 0, 10, new byte[100]);
        file1.addBatch("topic1", 1, 10, new byte[110]);
        file1.addBatch("topic2", 0, 10, new byte[200]);
        file1.addBatch("topic2", 1, 10, new byte[210]);
        file1.addBatch("topic1", 0, 20, new byte[100]);
        file1.addBatch("topic1", 1, 20, new byte[110]);
        file1.addBatch("topic2", 0, 20, new byte[200]);
        file1.addBatch("topic2", 1, 20, new byte[210]);
        BatchBuffer.CloseResult file1CloseResult = file1.close();

        // Step 1: Upload file1CloseResult.data to S3

        // Step 2: Commit
        controlPlane.commitFile(file1CloseResult.commitFileRequest);

        // Step 3: Response to producers with the assigned offsets.

        // Add some more data ...
        BatchBuffer file2 = new BatchBuffer("file2");
        file2.addBatch("topic1", 0, 1, new byte[100]);
        file2.addBatch("topic1", 1, 2, new byte[110]);
        file2.addBatch("topic2", 0, 3, new byte[200]);
        file2.addBatch("topic2", 1, 4, new byte[210]);
        BatchBuffer.CloseResult file2CloseResult = file2.close();
        controlPlane.commitFile(file2CloseResult.commitFileRequest);

        // Step 4: Find batch for the target offset.
        FindBatchResponse topic1 = controlPlane.findBatch(new FindBatchRequest("topic1", 0, 22));
        System.out.println("File: " + topic1.fileName);
        System.out.println("Byte offset: " + topic1.byteOffset);
        System.out.println("Batch size in bytes: " + topic1.byteSize);
        System.out.println("Batch base Kafka offset: " + topic1.batchBaseOffset);
        System.out.println("Number of records: " + topic1.numberOfRecords);

        // Step 5: Fetch from S3.
    }
}
