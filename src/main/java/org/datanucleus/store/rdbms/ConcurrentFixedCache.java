package org.datanucleus.store.rdbms;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * When we know that there is a finite number of keys, so we get just "cache" everything.
 * <p>
 * {@link #factory} may be called multiple times in case multiple threads try to init the same
 * key at once.
 */
public final class ConcurrentFixedCache<K, V>
{
    private final Function<K, V> factory;
    private volatile Map<K, V> immutable = Collections.emptyMap();

    public ConcurrentFixedCache(Function<K, V> factory)
    {
        this.factory = factory;
    }

    public V get(K key)
    {
        V value = immutable.get(key);
        if (value == null) {
            Map<K, V> copy = new HashMap<>(immutable);
            value = factory.apply(key);
            copy.put(key, value);
            immutable = copy;
        }
        return value;
    }
}
