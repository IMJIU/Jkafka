package kafka.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.common.KafkaException;
import kafka.func.Handler;
import kafka.func.Tuple;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Pool<K, V> implements Iterable<Tuple<K, V>> {
    private ConcurrentHashMap<K, V> pool = new ConcurrentHashMap<K, V>();
    private Object createLock = new Object();
    public Optional<Handler<K, V>> valueFactory;
    public Pool() {
        this.valueFactory = Optional.empty();
    }
    public Pool(Optional<Handler<K, V>> valueFactory) {
        this.valueFactory = valueFactory;
    }

    public Pool(Map<K, V> m) {
        m.forEach((k, v) -> pool.put(k, v));
    }

    public V put(K k, V v) {
        return pool.put(k, v);
    }

    public void putIfNotExists(K k, V v) {
        pool.putIfAbsent(k, v);
    }


    public List<Tuple<K,V>>list(){
        List list = Lists.newArrayList();
        Iterator<Tuple<K, V>> it = iterator();
        while(it.hasNext()){
            list.add(it.next());
        }
        return list;
    }

    /**
     * Gets the value associated with the given key. If there is no associated
     * value, then create the value using the pool's value factory and return the
     * value associated with the key. The user should declare the factory method
     * as lazy if its side-effects need to be avoided.
     *
     * @param key The key to lookup.
     * @return The final value associated with the key. This may be different from
     * the value created by the factory if another thread successfully
     * put a value.
     */
    public V getAndMaybePut(K key) {
        if (!valueFactory.isPresent())
            throw new KafkaException("Empty value factory in pool.");
        V curr = pool.get(key);
        if (curr == null) {
            synchronized (createLock) {
                curr = pool.get(key);
                if (curr == null) {
                    pool.put(key, valueFactory.get().handle(key));
                }
                return pool.get(key);
            }
        } else {
            return curr;
        }
    }

    public void contains(K id) {
        pool.containsKey(id);
    }

    public V get(K key) {
        return pool.get(key);
    }

    public V remove(K key) {
        return pool.remove(key);
    }

    public Set<K> keys() {
        return pool.keySet();
    }

    public Iterable<V> values() {
        return new ArrayList<>(pool.values());
    }

    public void clear() {
        pool.clear();
    }

    public Integer getSize() {
        return pool.size();
    }


    @Override
    public Iterator<Tuple<K, V>> iterator() {
        return new Iterator<Tuple<K, V>>() {

            private Iterator<Map.Entry<K, V>> iter = pool.entrySet().iterator();

            public boolean hasNext() {
                return iter.hasNext();
            }

            public Tuple next() {
                Map.Entry<K, V> entry = iter.next();
                return new Tuple(entry.getKey(), entry.getValue());
            }
        };
    }

    public Map<K,V> toMap() {
        Map<K,V>map = Maps.newHashMap();
        map.putAll(pool);
        return map;
    }
}
