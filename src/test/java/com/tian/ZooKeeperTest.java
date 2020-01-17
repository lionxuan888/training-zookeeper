package com.tian;

import com.google.common.base.Strings;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

public class ZooKeeperTest {



    @Test
    public void testAlive() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("172.16.3.117:40000,172.16.3.117:40001,172.16.3.117:40002")
                .namespace("tian")
                .retryPolicy(new RetryPolicy() {
                    public boolean allowRetry(int i, long l, RetrySleeper retrySleeper) {
                        return false;
                    }
                })
                .build();
        client.start();
        creatingParentContainersIfNeeded(client, "/app/hx");
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/app/hx", false);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                PathChildrenCacheEvent.Type eventType = event.getType();
                ChildData childData = event.getData();

                if (childData == null){
                    return;
                }
                String path = childData.getPath();

                switch (eventType) {
                    case CHILD_ADDED:
                        System.out.println("the client is join.... " + path);
                         break;
                    case CHILD_REMOVED:
                        System.out.println("the client is leave.... " + path);
                         break;
                    case CHILD_UPDATED:
                         break;
                    case CONNECTION_RECONNECTED:

                    default:
                        break;
                }
            }
        });
        pathChildrenCache.start();
        Thread.sleep(Integer.MAX_VALUE);
    }

    private void creatingParentContainersIfNeeded(CuratorFramework client, String path) throws Exception {
        Stat pathStat = client.checkExists().forPath(path);
        if (pathStat == null) {
            String nodePath = client.create().creatingParentsIfNeeded().forPath(path, "hello".getBytes());
        }
    }


    @Test
    public void online() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("172.16.3.117:40000,172.16.3.117:40001,172.16.3.117:40002")
                .namespace("tian")
                .retryPolicy(new RetryPolicy() {
                    public boolean allowRetry(int i, long l, RetrySleeper retrySleeper) {
                        return false;
                    }
                })
                .build();
        client.start();
        client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/app/hx/192.168.1.199");
    }

    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1")
                .namespace("tian")
                .retryPolicy(new RetryPolicy() {
                    public boolean allowRetry(int i, long l, RetrySleeper retrySleeper) {
                        return false;
                    }
                })
                .build();
        client.start();

        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/tomason", true);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                switch (pathChildrenCacheEvent.getType()) {
                    case CHILD_ADDED:
                        ChildData data = pathChildrenCacheEvent.getData();
                        System.out.println("event: added :"  + data.getPath() + "===" + new String(data.getData()));
                        break;
                    case CHILD_UPDATED:
                        System.out.println("event: CHILD_UPDATED");
                        break;
                    case CHILD_REMOVED:
                        System.out.println("event: CHILD_REMOVED");
                        break;
                    case CONNECTION_SUSPENDED:
                        System.out.println("event: CONNECTION_SUSPENDED");
                        break;
                    case CONNECTION_RECONNECTED:
                        System.out.println("event: CONNECTION_RECONNECTED");
                        break;
                    case CONNECTION_LOST:
                        System.out.println("event: CONNECTION_LOST");
                        break;
                    case INITIALIZED:
                        System.out.println("event: INITIALIZED");
                        break;
                }
            }
        });
        pathChildrenCache.start();
        Thread.sleep(1000 * 2);
        client.create().forPath("/tomason/fff");
        Thread.sleep(1000 * 3);
        System.out.println("done");

    }

   @Test
    public void test() throws Exception{
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1")
                .namespace("tian")
                .retryPolicy(new RetryPolicy() {
                    public boolean allowRetry(int i, long l, RetrySleeper retrySleeper) {
                        return false;
                    }
                })
                .build();
        client.start();

//        String path = "/jobs/clientProxy/register/iqianjin.icms.doFlashScreenJob";
       String path = "/jobs/a/b/d";
        Stat pathStat = client.checkExists().forPath(path);

        if (pathStat == null){
            String s = client.create().creatingParentContainersIfNeeded().forPath(path);
            System.out.println("[[[ " + s);
//            client.create().forPath(path);
           // String nodePath = client.create().forPath(path, null);

        }
    }

}
