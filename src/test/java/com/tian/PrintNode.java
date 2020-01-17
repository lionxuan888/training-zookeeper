package com.tian;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.junit.Test;

import java.util.List;

/**
 * Created by logan on 18/6/25.
 */
public class PrintNode {


    @Test
    public void testPrint() throws Exception {
        String connectString = "172.16.3.117:40000";
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .namespace("ats")
                .retryPolicy(new RetryPolicy() {
                    public boolean allowRetry(int i, long l, RetrySleeper retrySleeper) {
                        return false;
                    }
                })
                .build();
        client.start();
        List<String> strings = client.getChildren().forPath("/cluster/clients/monkey");
        List<String> servers = client.getChildren().forPath("/cluster/servers");
        System.out.println(connectString + "---> " + strings);
        System.out.println(connectString + "---> " + servers);
    }

    @Test
    public void testPrint_1() throws Exception {
        String connectString = "172.16.3.117:40001";
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .namespace("ats")
                .retryPolicy(new RetryPolicy() {
                    public boolean allowRetry(int i, long l, RetrySleeper retrySleeper) {
                        return false;
                    }
                })
                .build();
        client.start();
        List<String> strings = client.getChildren().forPath("/cluster/clients/monkey");
        List<String> servers = client.getChildren().forPath("/cluster/servers");
        System.out.println(connectString + "---> " + strings);
        System.out.println(connectString + "---> " + servers);
    }

    @Test
    public void testPrint_2() throws Exception {
        String connectString = "172.16.3.117:40002";
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .namespace("ats")
                .retryPolicy(new RetryPolicy() {
                    public boolean allowRetry(int i, long l, RetrySleeper retrySleeper) {
                        return false;
                    }
                })
                .build();
        client.start();
        List<String> strings = client.getChildren().forPath("/cluster/clients/monkey");
        List<String> servers = client.getChildren().forPath("/cluster/servers");
        System.out.println(connectString + "---> " + strings);
        System.out.println(connectString + "---> " + servers);
    }
}
