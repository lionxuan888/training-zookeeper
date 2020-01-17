package com.tian;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

import java.util.concurrent.CountDownLatch;

public class LeaderSelect {

    public static void main(String[] args) throws Exception {

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
        stopGracefully(client);
        LeaderSelector leaderSelector = new LeaderSelector(client, master_path, new LeaderSelectorListener() {
            public void takeLeadership(CuratorFramework client) throws Exception {
                System.out.println("server1: 成为Master角色");

                Thread.sleep(Integer.MAX_VALUE);

                System.out.println("server1: 完成Master操作, 释放Master权利");
            }

            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                System.out.println("server1: stateChanged");
            }
        });
        leaderSelector.autoRequeue();
        leaderSelector.start();
        Thread.sleep(Integer.MAX_VALUE);
    }


    public static void  stopGracefully(CuratorFramework client) {

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("优雅关闭");
                client.close();
            }
        });

    }
}
