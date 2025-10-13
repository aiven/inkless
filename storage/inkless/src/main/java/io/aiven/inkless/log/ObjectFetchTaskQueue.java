/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.log;

import java.util.concurrent.LinkedBlockingQueue;

class ObjectFetchTaskQueue {
    private final LinkedBlockingQueue<ObjectFetchTask> queue = new LinkedBlockingQueue<>();
    private long totalByteSize = 0;

    void add(final ObjectFetchTask task) {
        queue.add(task);
        totalByteSize += task.batchInfo().metadata().byteSize();
    }

    ObjectFetchTask peek() {
        return queue.peek();
    }

    ObjectFetchTask poll() {
        final ObjectFetchTask task = queue.poll();
        if (task != null) {
            totalByteSize -= task.batchInfo().metadata().byteSize();
        }
        return task;
    }

    int size() {
        return queue.size();
    }

    long byteSize() {
        return totalByteSize;
    }
}
