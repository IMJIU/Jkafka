package kafka.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.api.PartitionStateInfo;
import kafka.func.*;
import kafka.log.TopicAndPartition;
import org.apache.commons.collections.map.HashedMap;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zhoulf
 * @create 2017-10-27 14:07
 **/
public class Sc {

    public static <T> int count(Iterable<T> it, Handler<T, Boolean> handler) {
        int count = 0;
        for (T t : it) {
            if (handler.handle(t)) {
                count++;
            }
        }
        return count;
    }

    public static <K, V> int count(Map<K, V> map, Handler2<K, V, Boolean> handler) {
        int count = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (handler.handle(entry.getKey(), entry.getValue())) {
                count++;
            }
        }
        return count;
    }

    public static <T> void foreach(Iterator<T> it, ActionP<T> action) {
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

    public static <K, V> void foreach(Iterable<Tuple<K, V>> it, ActionP2<K, V> action) {
        for (Tuple<K, V> tuple : it) {
            action.invoke(tuple.v1, tuple.v2);
        }
    }

    public static <T> void foreach(T[] it, ActionP<T> action) {
        for (T tuple : it) {
            action.invoke(tuple);
        }
    }

    public static <K, V> void foreach(Map<K, V> map, ActionP2<K, V> action) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            action.invoke(entry.getKey(), entry.getValue());
        }
    }

    public static <K, V> boolean foreach(Map<K, V> map, Handler2<K, V, Boolean> action) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            boolean isStop = action.handle(entry.getKey(), entry.getValue());
            if (isStop) {
                return true;
            }
        }
        return false;
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


    public static <K, V, V2> Map<V2, Map<K, V>> groupBy(List<Tuple<K, V>> list, Handler2<K, V, V2> handler) {
        Map<V2, Map<K, V>> maps = Maps.newHashMap();
        for (Tuple<K, V> entry : list) {
            V2 tag = handler.handle(entry.v1, entry.v2);
            Map<K, V> m = maps.getOrDefault(tag, Maps.newHashMap());
            m.put(entry.v1, entry.v2);
            maps.put(tag, m);
        }
        return maps;
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

    public static <K, V, K2> Map<K2, Map<K, V>> groupByKey(List<Tuple<K, V>> list, Handler<K, K2> handler) {
        Map<K2, Map<K, V>> maps = Maps.newHashMap();
        for (Tuple<K, V> entry : list) {
            K2 tag = handler.handle(entry.v1);
            Map<K, V> m = maps.getOrDefault(tag, Maps.newHashMap());
            m.put(entry.v1, entry.v2);
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

    public static <V> V getOrElse(Optional<V> opt, V v) {
        if (opt.isPresent()) {
            return opt.get();
        }
        return v;
    }

    public static <T, V> List<V> map(T[] it, Handler<T, V> action) {
        List<V> result = new ArrayList<>(it.length);
        for (T tuple : it) {
            result.add(action.handle(tuple));
        }
        return result;
    }

    public static <V, RESULT> Optional<RESULT> map(Optional<V> it, Handler<V, RESULT> handler) {
        if (it.isPresent()) {
            return Optional.of(handler.handle(it.get()));
        }
        return Optional.empty();
    }

    public static <V, V2> List<V2> map(Iterable<V> it, Handler<V, V2> handler) {
        return map(it.iterator(), handler);
    }

    public static <V, V2> List<V2> map(Iterator<V> it, Handler<V, V2> handler) {
        List<V2> list = Lists.newArrayList();
        while (it.hasNext()) {
            list.add(handler.handle(it.next()));
        }
        return list;
    }

    public static <K, V, V2> List<V2> map(Collection<Tuple<K, V>> it, Handler2<K, V, V2> handler) {
        List<V2> list = new ArrayList<>(it.size());
        for (Tuple<K, V> tuple : it) {
            list.add(handler.handle(tuple.v1, tuple.v2));
        }
        return list;
    }

    public static <V, V2> List<V2> map(Collection<V> set, Handler<V, V2> handler) {
        List<V2> list = Lists.newArrayList();
        if (set != null) {
            for (V entry : set) {
                list.add(handler.handle(entry));
            }
        }
        return list;
    }

    public static <V, K, V2> Map<K, V2> mapToMap(Collection<V> set, Handler<V, Tuple<K, V2>> handler) {
        Map<K, V2> result = new HashMap<>(set.size());
        if (set != null) {
            for (V entry : set) {
                Tuple<K, V2> tuple = handler.handle(entry);
                result.put(tuple.v1, tuple.v2);
            }
        }
        return result;
    }

    public static <V, V2> Set<V2> mapToSet(Collection<V> list, Handler<V, V2> handler) {
        Set<V2> set = Sets.newHashSet();
        if (set != null) {
            for (V entry : list) {
                set.add(handler.handle(entry));
            }
        }
        return set;
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

    public static <V1, V2, V3, V4> Tuple<V3, V4> map(Tuple<V1, V2> tuple, Handler2<V1, V2, Tuple<V3, V4>> handler) {
        Tuple<V3, V4> result = handler.handle(tuple.v1, tuple.v2);
        return result;
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

    public static <T> List<T> flatten(Iterable<Iterable<T>> it) {
        List<T> result = Lists.newArrayList();
        Iterator<Iterable<T>> iterator = it.iterator();
        while (iterator.hasNext()) {
            Iterable<T> subIt = iterator.next();
            Iterator<T> subItor = subIt.iterator();
            while (subItor.hasNext()) {
                result.add(subItor.next());
            }
        }
        return result;
    }

    public static <T> List<T> flatten2(Collection<Collection<T>> it) {
        List<T> result = Lists.newArrayList();
        for (Collection<T> collection : it) {
            for (T i : collection) {
                result.add(i);
            }
        }
        return result;
    }

    public static <T> List<T> flatMap(List<T> it, Handler<T, Collection<T>> handler) {
        List<T> list = Lists.newArrayList();
        Iterator<T> iterator = it.iterator();
        while (iterator.hasNext()) {
            T t = iterator.next();
            Collection<T> collection = handler.handle(t);
            if (collection.size() > 0) {
                list.addAll(collection);
            }
        }
        return list;
    }

    public static <T, V> List<V> flatMap(Iterable<T> it, Handler<T, Collection<V>> handler) {
        List<V> list = Lists.newArrayList();
        Iterator<T> iterator = it.iterator();
        while (iterator.hasNext()) {
            T t = iterator.next();
            Collection<V> collection = handler.handle(t);
            if (collection.size() > 0) {
                list.addAll(collection);
            }
        }
        return list;
    }


    public static <T, V> Set<V> flatMap(Set<T> it, Handler<T, Collection<V>> handler) {
        Set<V> set = Sets.newHashSet();
        Iterator<T> iterator = it.iterator();
        while (iterator.hasNext()) {
            T t = iterator.next();
            Collection<V> collection = handler.handle(t);
            if (collection.size() > 0) {
                set.addAll(collection);
            }
        }
        return set;
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

    public static <K, V> Map<K, V> filterNot(Map<K, V> map, Handler2<K, V, Boolean> handler2) {
        Map<K, V> result = Maps.newHashMap();
        for (Map.Entry<K, V> en : map.entrySet()) {
            K k = en.getKey();
            V v = en.getValue();
            if (!handler2.handle(k, v)) {
                result.put(k, v);
            }
        }
        return result;
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

    public static <T> List<T> itToList(int i, int step, int limit, Handler<Integer, T> handler) {
        return Stream.iterate(i, n -> n + step).limit(limit).map(n -> handler.handle(n)).collect(Collectors.toList());
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

    public static <T> T last(Iterator<T> itor) {
        T last = null;
        while (itor.hasNext()) {
            last = itor.next();
        }
        return last;
    }

    public static <T> T last(Iterable<T> it) {
        T last = null;
        Iterator<T> itor = it.iterator();
        while (itor.hasNext()) {
            last = itor.next();
        }
        return last;
    }

    public static <T> List<T> yield(int i, int num, Fun<T> fun) {
        List<T> list = new ArrayList<>(num);
        for (; i < num; i++) {
            list.add(fun.invoke());
        }
        return list;
    }

    public static <T> Set<T> yieldSet(int i, int num, Fun<T> fun) {
        Set<T> list = new HashSet<>(num);
        for (; i < num; i++) {
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

    public static <T> T find(Iterable<T> list, Handler<T, Boolean> handler) {
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


    public static <T, V> V match(Optional<T> opt, Handler<T, V> handler, Fun<V> fun) {
        if (opt.isPresent()) {
            return handler.handle(opt.get());
        }
        return fun.invoke();
    }

    public static <T> void match(Optional<T> opt, ActionP<T> handler, Action action) {
        if (opt.isPresent()) {
            handler.invoke(opt.get());
        } else {
            action.invoke();
        }
    }

    public static <K, V> Tuple<K, V> head(Map<K, V> topMap) {
        if (!topMap.isEmpty()) {
            for (Map.Entry<K, V> e : topMap.entrySet()) {
                return Tuple.of(e.getKey(), e.getValue());
            }
        }
        return Tuple.EMPTY;
    }

    public static <T> T head(Iterable<T> iterable) {
        Iterator<T> it = iterable.iterator();
        while (it.hasNext()) {
            return it.next();
        }
        return null;
    }

    public static <T> Set<T> subtract(Set<T> s1, Set<T> s2) {
        Set<T> result = Sets.newHashSet(s1);
        for (T t : s1) {
            if (s2.contains(t)) {
                result.remove(t);
            }
        }
        return result;
    }

    public static <T> Set<T> and(Set<T> s1, Set<T> s2) {
        Set<T> result = Sets.newHashSet(s1);
        for (T t : s1) {
            if (!s2.contains(t)) {
                result.remove(t);
            }
        }
        for (T t : result) {
            if (!s2.contains(t)) {
                result.remove(t);
            }
        }
        return result;
    }

    public static <T> Set<T> add(Set<T> newReplicas, Collection<T> c) {
        Set<T> result = Sets.newHashSet(newReplicas);
        for (T t : c) {
            result.add(t);
        }
        return result;
    }

    public static boolean equals(List<Integer> aliveNewReplicas, List<Integer> newReplicas) {
        if (aliveNewReplicas.size() != newReplicas.size()) {
            return false;
        }
        for (int i = 0; i < aliveNewReplicas.size(); i++) {
            if (!aliveNewReplicas.get(i).equals(newReplicas.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static <T> Set<T> add(Set<T>... sets) {
        int size = 0;
        for (Set<T> t : sets) {
            size += t.size();
        }
        Set<T> result = new HashSet<T>(size);
        for (Set<T> t : sets) {
            result.addAll(t);
        }
        return result;
    }

    public static <K, V> Map<K, V> add(Map<K, V> m1, Map<K, V> m2) {
        Map<K, V> result = new HashedMap(m1.size() + m2.size());
        m1.forEach((k, v) -> result.put(k, v));
        m2.forEach((k, v) -> result.put(k, v));
        return result;
    }

    public static <T> boolean foldBooleanOr(Iterable<T> it, boolean b, Handler<T, Boolean> handler) {
        for (T t : it) {
            b = b || handler.handle(t);
        }
        return b;
    }

    public static <T> Boolean foldBooleanAll(Collection<T> it, boolean b, Handler<T, Boolean> handler) {
        for (T t : it) {
            b = b && handler.handle(t);
        }
        return b;
    }

    public static <T> List<T> toList(T[] a) {
        List<T> list = new ArrayList<T>(a.length);
        for (T t : a) {
            list.add(t);
        }
        return list;
    }

    public static <T> Tuple<List<T>, List<T>> partition(List<T> list, Handler<T, Boolean> handler) {
        List<T> list1 = Lists.newArrayList();
        List<T> list2 = Lists.newArrayList();
        for (T t : list) {
            if (handler.handle(t)) {
                list1.add(t);
            } else {
                list2.add(t);
            }
        }
        return Tuple.of(list1, list2);
    }


    public static <T> List<T> takeRight(Collection<T> it, int index) {
        int i = 0;
        Iterator<T> iterator = it.iterator();
        List<T> result = Lists.newArrayList();
        while (iterator.hasNext()) {
            if (i++ >= index) {
                result.add(iterator.next());
            }
        }
        return result;
    }

    public static <T> boolean subsetOf(Set<T> sub, Set<T> parent) {
        boolean result = true;
        for (T t : sub) {
            if (!parent.contains(t)) {
                result = false;
                break;
            }
        }
        return result;
    }

    public static Map toMap(Properties config) {
        return config;
    }

    public static <K, V> List<Tuple<K, V>> toList(Map<K, V> map) {
        List<Tuple<K, V>> result = new ArrayList(map.size());
        map.forEach((k, v) -> result.add(Tuple.of(k, v)));
        return result;
    }

    public static <T> List<T> sortWith(List<T> list, Handler2<T, T, Boolean> handler2) {
        List<T> result = new ArrayList<>(list);
        for (int i = 0; i < result.size() - 1; i++) {
            T temp = result.get(i);
            for (int j = i + 1; j < list.size(); j++) {
                T next = result.get(j);
                if (handler2.handle(next, temp)) {
                    result.set(i, next);
                    result.set(j, temp);
                    temp = next;
                }
            }
        }
        return result;
    }

    public static String mkString(Collection collection, String sep) {
        StringBuilder sb = new StringBuilder();
        Iterator iterator = collection.iterator();
        while (iterator.hasNext()) {
            sb.append(iterator.next()).append(sep);
        }
        if (collection.size() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    public static <T> Map<T, Integer> zipWithIndex(List<T> list) {
        Map<T, Integer> result = new HashMap<>(list.size());
        Integer i = 0;
        for (T e : list) {
            result.put(e, i++);
        }
        return result;
    }

    public static <V1, V2> List zip(List<V1> list1, List<V2> list2) {
        List<Tuple<V1, V2>> result = new ArrayList<>(list1.size());
        for (int i = 0; i < list1.size(); i++) {
            result.add(Tuple.of(list1.get(i), list2.get(i)));
        }
        return result;
    }

    public static <T extends Comparable> Iterable<T> sortIterable(List<T> consumerThreadIds) {
        Collections.sort(consumerThreadIds);
        return consumerThreadIds;
    }

    public static <T extends Comparable> List<T> sorted(List<T> list) {
        Collections.sort(list);
        return list;
    }

    public static <V> V getOrElseUpdate(Map map, Object key, V value) {
        Object result = map.get(key);
        if (result == null) {
            map.put(key, value);
        }
        return value;
    }

    public static <K, V> Map<K, V> toMap(Object... objs) {
        Map<K, V> ret = Maps.newHashMap();
        if (objs.length == 0) {
            return ret;
        }
        if (objs.length % 2 != 0) {
            throw new RuntimeException("Sc.toMap parameters must be 2*n");
        }
        for (int i = 0; i < objs.length; i += 2) {
            ret.put((K) objs[i], (V) objs[i + 1]);
        }
        return ret;
    }
}
