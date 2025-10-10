package io.aiven.inkless.common.cache;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.generated.CacheKey;

public final class CacheKeyCreator {

    /**
     * Create cache key from the object key and byte range.
     * The produce side cache job and consume side cache fetch requires
     * matching keys. Consolidates the key creation to a class for this
     * purpose.
     * @param object The blob storage object key.
     * @param byteRange The byte range of the cached portion of data.
     * @return
     */
    public static CacheKey create(ObjectKey object, ByteRange byteRange) {
        return new CacheKey().
                setObject(object.value())
                .setRange(new CacheKey.ByteRange()
                        .setOffset(byteRange.offset())
                        .setLength(byteRange.size()));
    }

}
