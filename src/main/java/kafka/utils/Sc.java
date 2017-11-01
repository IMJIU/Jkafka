package kafka.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.func.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zhoulf
 * @create 2017-10-27 14:07
 **/
public class Sc {

    public static <T> int count(Collection<T> it, Handler<T, Boolean> handler) {
        int count = 0;
        for (T t : it) {
            if (handler.handle(t)) {
                count++;
            }
        }
        return count;
    }

    public static <T> void loop(Iterator<T> it, ActionP<T> action) {
        while (it.hasNext()) {
            action.invoke(it.next());
        }
    }

    public static <T> List<T> filter(Iterator<T> it, Handler<T, Boolean> action) {
        List<T> list = Lists.newArrayList();
        while (it.hasNext()) {
            T t = it.next();
            if (action.handle(t)) {
                list.add(t);
            }
        }
        return list;
    }

    public static <K, V> void foreach(Map<K, V> map, ActionP2<K, V> action) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            action.invoke(entry.getKey(), entry.getValue());
        }
    }

    public static <V> void foreach(Optional<V> opt, ActionP<V> action) {
        if (opt.isPresent()) {
            action.invoke(opt.get());
        }
    }

    public static <T, K> Map<K, List<T>> groupBy(Iterable<T> it, Handler<T, K> handler) {
        Map<K, List<T>> result = Maps.newHashMap();
        for (T t : it) {
            K key = handler.handle(t);
            List<T> itemList = result.getOrDefault(key, Lists.newArrayList());
            itemList.add(t);
            result.put(key, itemList);
        }
        return result;
    }

    public static <K, V, V2> Map<V2, Map<K, V>> groupBy(Map<K, V> map, Handler2<K, V, V2> handler) {
        Map<V2, Map<K, V>> maps = Maps.newHashMap();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            V2 tag = handler.handle(entry.getKey(), entry.getValue());
            Map<K, V> m = maps.getOrDefault(tag, Maps.newHashMap());
            m.put(entry.getKey(), entry.getValue());
            maps.put(tag, m);
        }
        return maps;
    }

    public static <K, V, V2> Map<V2, List<V>> groupByToList(Map<K, V> map, Handler2<K, V, V2> handler) {
        Map<V2, List<V>> maps = Maps.newHashMap();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            V2 tag = handler.handle(entry.getKey(), entry.getValue());
            List<V> list = maps.getOrDefault(tag, Lists.newArrayList());
            list.add(entry.getValue());
            maps.put(tag, list);
        }
        return maps;
    }

    public static <K, V, V2> Map<V2, Map<K, V>> groupByValue(Map<K, V> map, Handler<V, V2> handler) {
        Map<V2, Map<K, V>> maps = Maps.newHashMap();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            V2 tag = handler.handle(entry.getValue());
            Map<K, V> m = maps.getOrDefault(tag, Maps.newHashMap());
            m.put(entry.getKey(), entry.getValue());
            maps.put(tag, m);
        }
        return maps;
    }

    public static <K, V, K2> Map<K2, Map<K, V>> groupByKey(Map<K, V> map, Handler<K, K2> handler) {
        Map<K2, Map<K, V>> maps = Maps.newHashMap();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            K2 tag = handler.handle(entry.getKey());
            Map<K, V> m = maps.getOrDefault(tag, Maps.newHashMap());
            m.put(entry.getKey(), entry.getValue());
            maps.put(tag, m);
        }
        return maps;
    }

    public static <V, RESULT> Optional<RESULT> map(Optional<V> it, Handler<V, RESULT> handler) {
        if (it.isPresent()) {
            return Optional.of(handler.handle(it.get()));
        }
        return Optional.empty();
    }

    public static <V, RESULT> List<RESULT> map(Iterable<V> it, Handler<V, RESULT> handler) {
        List<RESULT> list = Lists.newArrayList();
        Iterator<V> itor = it.iterator();
        while (itor.hasNext()) {
            list.add(handler.handle(itor.next()));
        }
        return list;
    }

    public static <V, RESULT> List<RESULT> map(Collection<V> set, Handler<V, RESULT> handler) {
        List<RESULT> list = Lists.newArrayList();
        if (set != null) {
            for (V entry : set) {
                list.add(handler.handle(entry));
            }
        }
        return list;
    }

    public static <V, RESULT> Set<RESULT> map(Set<V> set, Handler<V, RESULT> handler) {
        Set<RESULT> list = Sets.newHashSet();
        if (set != null) {
            for (V entry : set) {
                list.add(handler.handle(entry));
            }
        }
        return list;
    }

    public static <K, V, RESULT> List<RESULT> map(Map<K, V> map, Handler<Map.Entry<K, V>, RESULT> handler) {
        List<RESULT> list = Lists.newArrayList();
        if (map != null) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                list.add(handler.handle(entry));
            }
        }
        return list;
    }

    public static <K, V, RESULT> List<RESULT> map(Map<K, V> map, Handler2<K, V, RESULT> handler) {
        List<RESULT> list = Lists.newArrayList();
        if (map != null) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                list.add(handler.handle(entry.getKey(), entry.getValue()));
            }
        }
        return list;
    }

    public static <K, V, V2> Map<K, V2> mapValue(Map<K, V> map, Handler<V, V2> handler) {
        Map<K, V2> result = Maps.newHashMap();
        if (map != null) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                result.put(entry.getKey(), handler.handle(entry.getValue()));
            }
        }
        return result;
    }

    public static <K, V, V2> Map<K, V2> map2(Map<K, V> map, Handler<Map.Entry<K, V>, V2> handler) {
        Map<K, V2> result = Maps.newHashMap();
        if (map != null) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                result.put(entry.getKey(), handler.handle(entry));
            }
        }
        return result;
    }

    public static <K, V, K2> Map<K2, V> mapKey(Map<K, V> map, Handler<K, K2> handler) {
        Map<K2, V> result = Maps.newHashMap();
        if (map != null) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                result.put(handler.handle(entry.getKey()), entry.getValue());
            }
        }
        return result;
    }

    public static <T> int size(Iterable<T> iterable) {
        Iterator<T> it = iterable.iterator();
        int size = 0;
        while (it.hasNext()) {
            it.next();
            size++;
        }
        return size;
    }

    public static <K, V, R> List<R> flatMap(Map<K, V> map, Handler2<K, V, Stream<R>> handler2) {
        Stream<R> stream = null;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            Stream<R> s = handler2.handle(entry.getKey(), entry.getValue());
            if (stream == null) {
                stream = s;
            } else {
                Stream.concat(stream, s);
            }
        }
        return stream.collect(Collectors.toList());
    }

    public static <K, V> Map<K, V> toMap(Collection<Tuple<K, V>> list) {
        Map<K, V> map = Maps.newHashMap();
        for (Tuple<K, V> kv : list) {
            map.put(kv.v1, kv.v2);
        }
        return map;
    }


    public static <T> Set<T> filterNot(Set<T> it, Handler<T, Boolean> handler) {
        Iterator<T> iterator = it.iterator();
        Set<T> set = Sets.newHashSet();
        while (iterator.hasNext()) {
            T t = iterator.next();
            if (!handler.handle(t)) {
                set.add(t);
            }
        }
        return set;
    }

    public static <T> List<T> filterNot(Iterable<T> it, Handler<T, Boolean> handler) {
        Iterator<T> iterator = it.iterator();
        List<T> list = Lists.newArrayList();
        while (iterator.hasNext()) {
            T t = iterator.next();
            if (!handler.handle(t)) {
                list.add(t);
            }
        }
        return list;
    }

    public static <T> List<T> filter(Iterable<T> it, Handler<T, Boolean> handler) {
        Iterator<T> iterator = it.iterator();
        List<T> list = Lists.newArrayList();
        while (iterator.hasNext()) {
            T t = iterator.next();
            if (handler.handle(t)) {
                list.add(t);
            }
        }
        return list;
    }

    public static <T> Set<T> filter(Set<T> it, Handler<T, Boolean> handler) {
        Set<T> result = Sets.newHashSet();
        for (T t : it) {
            if (handler.handle(t)) {
                result.add(t);
            }
        }
        return result;
    }

    public static void it(int i, int limit, ActionP<Integer> actionP) {
        Stream.iterate(i, n -> n + 1).limit(limit).forEach(n -> actionP.invoke(n));
    }

    public static <T> List<T> itToList(int i, int limit, Handler<Integer, T> handler) {
        return Stream.iterate(i, n -> n + 1).limit(limit).map(n -> handler.handle(n)).collect(Collectors.toList());
    }

    public static <T> List<T> itFlatToList(int i, int limit, Handler<Integer, Stream<T>> handler) {
        return Stream.iterate(i, n -> n + 1).limit(limit).flatMap(n -> handler.handle(n)).collect(Collectors.toList());
    }


    public static <T> Boolean exists(Collection<T> list, Handler<T, Boolean> handler) {
        for (T t : list) {
            if (handler.handle(t)) {
                return true;
            }
        }
        return false;
    }

    public static <K, V> Boolean exists(Map<K, V> map, Handler2<K, V, Boolean> handler) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (handler.handle(entry.getKey(), entry.getValue())) {
                return true;
            }
        }
        return false;
    }

    public static <T> Optional<T> lastOption(Iterator<T> it) {
        T last = null;
        while (it.hasNext()) {
            last = it.next();
        }
        if (last != null) {
            Optional.of(last);
        }
        return Optional.empty();
    }

    public static <T> T last(Iterator<T> it) {
        T last = null;
        while (it.hasNext()) {
            last = it.next();
        }
        return last;
    }

    public static <T> List<T> yield(int i, int numAliveBrokers, Fun<T> fun) {
        List<T> list = new ArrayList<>(numAliveBrokers);
        for (; i < numAliveBrokers; i++) {
            list.add(fun.invoke());
        }
        return list;
    }

    public static <T> Set<T> yieldSet(int i, int numAliveBrokers, Fun<T> fun) {
        Set<T> list = new HashSet<>(numAliveBrokers);
        for (; i < numAliveBrokers; i++) {
            list.add(fun.invoke());
        }
        return list;
    }

    public static <T> Set<T> toSet(Iterable<T> iterable) {
        Set<T> set = Sets.newHashSet();
        Iterator<T> it = iterable.iterator();
        while (it.hasNext()) {
            set.add(it.next());
        }
        return set;
    }

    public static <T> List<T> toList(Collection<T> list) {
        List<T> result = new ArrayList<T>(list.size());
        for (T t : list) {
            result.add(t);
        }
        return result;
    }

    public static <T> T find(Collection<T> list, Handler<T, Boolean> handler) {
        for (T t : list) {
            if (handler.handle(t)) {
                return t;
            }
        }
        return null;
    }

    public static <K, V> Tuple<K, V> find(Map<K, V> map, Handler2<K, V, Boolean> handler) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            K k = entry.getKey();
            V v = entry.getValue();
            if (handler.handle(k, v)) {
                return Tuple.of(k, v);
            }
        }
        return Tuple.EMPTY;
    }

    public static <T extends Number> Long sum(Collection<T> list) {
        return list.stream().mapToLong(n -> n.longValue()).sum();
    }

    public static <T> int sum(Collection<T> list, Handler<T, Integer> handler) {
        IntCount size = IntCount.of(0);
        list.forEach(t -> size.add(handler.handle(t)));
        return size.get();
    }

    public static <K, V> Map<K, V> filter(Map<K, V> map, Handler2<K, V, Boolean> handler2) {
        Map<K, V> result = Maps.newHashMap();
        for (Map.Entry<K, V> en : map.entrySet()) {
            K k = en.getKey();
            V v = en.getValue();
            if (handler2.handle(k, v)) {
                result.put(k, v);
            }
        }
        return result;
    }

    public static <T, V> V match(Optional<T> opt, Handler<T, V> handler, Fun<V> fun) {
        if (opt.isPresent()) {
            return handler.handle(opt.get());
        }
        return fun.invoke();
    }
}
