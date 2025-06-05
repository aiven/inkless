package io.aiven.inkless.cache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


/**
 * A data structure that maps keys to LinkedLists of values.
 * Eviction is done only at the item level within each LinkedList:
 * - When adding an item to a LinkedList associated with a key,
 * if the list exceeds its max size, items are removed from the
 * beginning of that specific list.
 * - There is no eviction policy for the keys themselves; the number of
 * keys can grow indefinitely.
 *
 * @param <K> The type of the keys.
 * @param <V> The type of the elements within the LinkedList values.
 */
public class ItemLevelEvictionCache<K, V> {

    private final Map<K, LinkedList<V>> store;
    private final int maxListSizePerKey; // Maximum number of items in each LinkedList value

    public ItemLevelEvictionCache(int maxListSizePerKey) {
        if (maxListSizePerKey <= 0) {
            throw new IllegalArgumentException("maxListSizePerKey must be positive.");
        }
        this.store = new HashMap<>();
        this.maxListSizePerKey = maxListSizePerKey;
    }

    /**
     * Adds a value to the LinkedList associated with the given key.
     * If the key does not exist, a new LinkedList is created for it.
     * If adding the value makes the LinkedList exceed its maxListSizePerKey,
     * items are removed from the beginning of that list until it meets the size constraint.
     *
     * @param key   The key.
     * @param value The value to add to the key's LinkedList.
     */
    public void addValue(K key, V value) {
        // Get the list for the key. If key doesn't exist, create a new LinkedList.
        LinkedList<V> list = store.computeIfAbsent(key, k -> new LinkedList<>());

        // Add the new value to the end of the list
        list.addLast(value);

        // Enforce max size for this specific LinkedList (item-level eviction)
        while (list.size() > maxListSizePerKey) {
            list.removeFirst(); // Remove from the beginning of THIS list
        }
    }

    /**
     * Retrieves the LinkedList associated with the given key.
     *
     * @param key The key.
     * @return The LinkedList of values, or null if the key is not found in the store.
     */
    public LinkedList<V> getList(K key) {
        return store.get(key);
    }

    /**
     * Removes a key and its associated LinkedList from the store.
     *
     * @param key The key to remove.
     * @return The LinkedList previously associated with the key, or null if not found.
     */
    public LinkedList<V> removeKey(K key) {
        return store.remove(key);
    }

    /**
     * Returns the current number of keys in the store.
     *
     * @return The number of keys.
     */
    public int numberOfKeys() {
        return store.size();
    }

    /**
     * Clears all entries from the store.
     */
    public void clear() {
        store.clear();
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ItemLevelEvictionCache (Keys: " + numberOfKeys() + "):\n");
        store.forEach((key, list) -> {
            sb.append("  Key: ").append(key)
                .append(" -> Values (Size: ").append(list.size()).append("/").append(maxListSizePerKey).append("): [");
            String delimiter = "";
            for (V val : list) {
                sb.append(delimiter).append(val);
                delimiter = ", ";
            }
            sb.append("]\n");
        });
        return sb.toString();
    }

}