package com.tian;

import com.google.common.collect.Lists;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class LeaderSelect2 {


    @Test
    public void test() {
        List<Integer> list = Lists.newArrayList();
        for (int i = 0; i < 2658; i++) {
            list.add(i);
        }
        List<List<Integer>> parts = Lists.partition(list, 10);
        parts.stream().forEach(aa -> {
            System.out.println(aa);
        });


    }



    public static void main(String[] args) throws Exception{
        String master_path = "/masterSelect";
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .namespace("tian")
                .retryPolicy(new RetryPolicy() {
                    public boolean allowRetry(int i, long l, RetrySleeper retrySleeper) {
                        return false;
                    }
                })
                .build();
        client.start();
        String path = "/aa";
//        client.create().withMode(CreateMode.PERSISTENT).forPath(path, new String("fff").getBytes());

        LeaderSelector leaderSelector = new LeaderSelector(client, master_path, new LeaderSelectorListener() {
            public void takeLeadership(CuratorFramework client) throws Exception {


                System.out.println("server2 : 成为Master角色");
                Thread.sleep(Integer.MAX_VALUE);

                System.out.println("server2 :完成Master操作, 释放Master权利");
            }

            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                System.out.println("server2 :stateChanged");
            }
        });
        leaderSelector.autoRequeue();
        leaderSelector.start();
        Thread.sleep(Integer.MAX_VALUE);
    }

}
