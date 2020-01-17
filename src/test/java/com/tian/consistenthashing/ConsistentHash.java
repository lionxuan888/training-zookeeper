package com.tian.consistenthashing;

import com.google.common.collect.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsistentHash<S> {

    /**
     * virtual nodes
     */
    private TreeMap<Long, S> nodes;

    /**
     * servers
     */
    private List<S> servers;

    /**
     * the server and nodes mapping
     */
    private Multimap<S, Long> serverNodes;

    /**
     * virtual node num for every server
     */
    private final int nodeNum = 125;

    private int defaultWeight = 1;

    public ConsistentHash(List<S> servers) {
        this.servers = servers;
        init();
    }

    /**
     * init hash ring
     */
    private void init() {
        nodes = new TreeMap<Long, S>();
        serverNodes = ArrayListMultimap.create();
        for (int i = 0; i != servers.size(); ++i) {
            final S server = servers.get(i);

            for (int n = 0; n < nodeNum * defaultWeight; n++) {
                Long hash = hash("Server-" + server + "-node-" + n);
                nodes.put(hash, server);
                System.out.println("Server-" + server + "-node-" + n + ", " + hash);
                serverNodes.put(server, hash);
            }
        }
    }

    public S getServer(String key) {
        SortedMap<Long, S> tail = nodes.tailMap(hash(key));
        if (tail.size() == 0) {
            return nodes.get(nodes.firstKey());
        }
        return tail.get(tail.firstKey());
    }


    public void removeServer(S server) {
        List<Long> list = Lists.newArrayList(serverNodes.get(server));

        for (Long nodeHash : list) {
            nodes.remove(nodeHash);
        }
        serverNodes.removeAll(server);
    }

    /**
     * MurMurHash算法
     */
    private Long hash(String key) {
        long h = MurmurHash.hash(key.getBytes(), 0);
        if (h < 0) {
            h = h & MurmurHash.MASK;
        }
        return h;
    }

    public static void main(String[] args) {
        List<String> servers = Lists.newArrayList();
        servers.add("172.16.3.115:31779");
        servers.add("172.16.3.116:31339");

        //servers.add("172.168.112.12");
//        servers.add("172.168.112.13");
//        servers.add("172.168.112.14");
//        servers.add("172.168.112.15");
//        servers.add("172.168.112.16");
        ConsistentHash<String> consistent = new ConsistentHash<String>(servers);


        System.out.println("===============");
        testDataBalance(consistent);
        consistent.removeServer("172.168.112.10");
        testDataBalance(consistent);
    }

    private static void testDataBalance(ConsistentHash<String> consistent) {
        String jobClass = "com.icms.job.TestJob_";
        Map<String, AtomicInteger> map = Maps.newHashMap();
        for (int i = 1; i < 24; i++) {
            String key = jobClass + i;
            String server = consistent.getServer(key);
            AtomicInteger atomicInteger = map.get(server);
            if (atomicInteger == null) {
                atomicInteger = new AtomicInteger(0);
                map.put(server, atomicInteger);
            }
            atomicInteger.getAndAdd(1);
        }
        for (String server : consistent.serverNodes.keySet()) {
            System.out.println(server + ": " + consistent.serverNodes.get(server).size());
        }
        System.out.println(map);
    }
}
