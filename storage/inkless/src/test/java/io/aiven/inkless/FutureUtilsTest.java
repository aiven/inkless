package io.aiven.inkless;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static io.aiven.inkless.FutureUtils.combineMapOfFutures;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FutureUtilsTest {

    @Test
    void testCombineMap_successful() {
        final var future1 = CompletableFuture.completedFuture("value1");
        final var future2 = CompletableFuture.completedFuture("value2");
        final var result = combineMapOfFutures(Map.of("key1", future1, "key2", future2));
        assertThat(result).isCompletedWithValue(Map.of("key1", "value1", "key2", "value2"));
    }

    @Test
    void testCombineMap_empty() {
        final var result = combineMapOfFutures(Map.of());
        assertThat(result).isCompletedWithValue(Map.of());
    }

    @Test
    void testCombineMap_failed() {
        final var future1 = CompletableFuture.completedFuture("value1");
        final var future2 = new CompletableFuture<String>();
        final var result = combineMapOfFutures(Map.of("key1", future1, "key2", future2));
        future2.completeExceptionally(new RuntimeException("failed"));
        assertThat(result).isCompletedExceptionally();
    }

    @Test
    void testCombineMap_nullMap() {
        assertThatThrownBy(() -> combineMapOfFutures(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testCombineMap_nullKey() {
        Map<String, CompletableFuture<Integer>> input = new HashMap<>();
        input.put(null, CompletableFuture.completedFuture(1));

        assertThatThrownBy(() -> combineMapOfFutures(input))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testCombineMap_nullFuture() {
        Map<String, CompletableFuture<Integer>> input = new HashMap<>();
        input.put("a", null);

        assertThatThrownBy(() -> combineMapOfFutures(input))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testCombineMap_nullResult() {
        Map<String, CompletableFuture<Integer>> input = Map.of(
            "a", CompletableFuture.completedFuture(null)
        );

        CompletableFuture<Map<String, Integer>> future = combineMapOfFutures(input);

        assertThatThrownBy(future::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NullPointerException.class);
    }

    @Test
    void testCombineMap_delayedCompletion() throws Exception {
        CompletableFuture<Integer> delayed = new CompletableFuture<>();
        Map<String, CompletableFuture<Integer>> input = Map.of(
            "a", delayed
        );

        final CompletableFuture<Map<String, Integer>> future = combineMapOfFutures(input);
        assertThat(future).isNotCompleted();

        delayed.complete(42);
        Map<String, Integer> result = future.get(1, TimeUnit.SECONDS);

        assertThat(result.get("a")).isEqualTo(42);
    }

    @Test
    void testCombineMap_multipleDelayedCompletions() {
        CompletableFuture<Integer> future1 = new CompletableFuture<>();
        CompletableFuture<Integer> future2 = new CompletableFuture<>();

        Map<String, CompletableFuture<Integer>> input = Map.of(
            "a", future1,
            "b", future2
        );

        CompletableFuture<Map<String, Integer>> combined = combineMapOfFutures(input);

        future2.complete(2);  // Complete second future first
        assertThat(combined).isNotCompleted();

        future1.complete(1);  // Complete first future last
        Map<String, Integer> result = combined.join();

        assertThat(result).hasSize(2).contains(
            entry("a", 1),
            entry("b", 2)
        );
    }
}