package io.aiven.inkless.common.cache;

import org.junit.jupiter.api.Test;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.generated.CacheKey;

import static org.assertj.core.api.Assertions.assertThat;

public class CacheKeyCreatorTest {

    private static final ObjectKeyCreator objectKeyCreator = ObjectKey.creator("prefix", false);

    @Test
    public void testCacheKeyCreate() {
        final CacheKey key = CacheKeyCreator.create(objectKeyCreator.create("test-key"), new ByteRange(12, 13));
        assertThat(key.object()).isEqualTo("prefix/test-key");
        assertThat(key.range()).isEqualTo(new CacheKey.ByteRange().setOffset(12).setLength(13));

        final CacheKey equalKey = CacheKeyCreator.create(objectKeyCreator.create("test-key"), new ByteRange(12, 13));
        assertThat(key).isEqualTo(equalKey);
    }

    @Test
    public void testCreatedCacheKeyEquality() {
        final CacheKey key = CacheKeyCreator.create(objectKeyCreator.create("test-key"), new ByteRange(12, 13));
        final CacheKey equalKey = CacheKeyCreator.create(objectKeyCreator.create("test-key"), new ByteRange(12, 13));
        final CacheKey unequalKey = CacheKeyCreator.create(objectKeyCreator.create("test-key"), new ByteRange(12, 12));
        assertThat(key).isEqualTo(equalKey);
        assertThat(key).isNotEqualTo(unequalKey);
    }

}