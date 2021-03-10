package org.opencb.opencga.storage.core.metadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Created by jacobo on 27/02/19.
 */
public class MetadataCache<ID, R> {

    private final ConcurrentHashMap<String, R> cache;
    private final BiFunction<Integer, ID, R> function;

    public MetadataCache(BiFunction<Integer, ID, R> function) {
        this(function, null);
    }

    public MetadataCache(BiFunction<Integer, ID, R> function, Predicate<R> reuseValue) {
        this.cache = new ConcurrentHashMap<>();
        if (reuseValue != null) {
            // Try to reuse the value from another already existing in the cache.
            // Allows to avoid duplicated values and lets the GC to claim the unused values.
            this.function = function.andThen(value -> {
                // Once we have the value, try to find an equal value in the cache
                if (value != null && reuseValue.test(value)) {
                    for (R cachedValue : cache.values()) {
                        if (cachedValue.equals(value)) {
                            // Use the cached value. The GC will claim the 'value'. Multiple entries will point to the same value.
                            return cachedValue;
                        }
                    }
                }
                return value;
            });
        } else {
            this.function = function;
        }
    }

    /**
     * Get a cached value.
     *
     * @param studyId Study id
     * @param id      Resource id
     * @return        Value
     */
    public R get(int studyId, ID id) {
        String key = getKey(studyId, id);

        // Avoid unnecessary synchronize blocks.
        // ConcurrentHashMap::get does not have any synchronization block
        R r = cache.get(key);
        if (r != null) {
            return r;
        } else {
            // Use this method to avoid executing the function twice
            return cache.computeIfAbsent(key, k -> this.function.apply(studyId, id));
        }
    }

    /**
     * Get a cached value. Recompute if the stored value is equals to the given invalid value.
     *
     * @param studyId      Study id
     * @param id           Resource id
     * @param invalidValue Invalid value
     * @return Value
     */
    public R get(int studyId, ID id, R invalidValue) {
        String key = getKey(studyId, id);

        // Avoid unnecessary synchronize blocks.
        // ConcurrentHashMap::get does not have any synchronization block
        R r = cache.get(key);
        if (r == null || r.equals(invalidValue)) {
            // Use this method to avoid executing the function twice
            return cache.compute(key, (k, v) -> {
                if (v == null || v.equals(invalidValue)) {
                    return this.function.apply(studyId, id);
                } else {
                    return v;
                }
            });
        } else {
            return r;
        }
    }

    public void put(int studyId, ID id, R value) {
        cache.put(getKey(studyId, id), value);
    }

    public void clear() {
        cache.clear();
    }

    private String getKey(int studyId, ID id) {
        return studyId + "_" + id;
    }

}
