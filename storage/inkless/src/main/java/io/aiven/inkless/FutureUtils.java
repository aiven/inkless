// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

public class FutureUtils {

    public static <K, V> CompletableFuture<Map<K, V>> combineMapOfFutures(Map<K, CompletableFuture<V>> futuresMap) {
        Objects.requireNonNull(futuresMap, "futuresMap cannot be null");

        // Validate no null keys or futures
        if (futuresMap.entrySet().stream().anyMatch(e -> e.getKey() == null || e.getValue() == null)) {
            throw new NullPointerException("Map cannot contain null keys or futures");
        }

        // Handle empty map case
        if (futuresMap.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        List<CompletableFuture<Map.Entry<K, V>>> entryFutures = futuresMap.entrySet()
            .stream()
            .map(entry ->
                entry.getValue().thenApply((value) -> Map.entry(entry.getKey(), value)))
            .toList();

        return CompletableFuture
            .allOf(entryFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> entryFutures.stream()
                .map(future -> {
                    try {
                        return future.join();
                    } catch (CompletionException e) {
                        throw new CompletionException("Error combining futures", e);
                    }
                })
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (v1, v2) -> {
                        throw new IllegalStateException("Duplicate key found: " + v1);
                    }
                )));
    }
}
