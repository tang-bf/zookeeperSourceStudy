package com.tbh.client.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.common.Time;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *github上下载源码  ant eclipse编译为eclispe项目，开始data proto包源码都没有，去maven仓库下载源码包,编译后发现generated包下有这些
 * 所以又手动删除，不然会报重复的类；运行后又没有Info类,发现version.util包有个vergen类
 * 3.4.13 1 2020-09-14 把自动编译去掉  z真坑。。。
 * 后来发现data proto 都是由jute自动生成的；jute主要用了jacc;
 * jute组件来实现序列反序列化,客户端服务端之间相互通信通过TCP/IP 怎么解决粘包问题？类似序列化还有json ,xml,protobuff,thrift
 * dubbo用的hessian2
 */
public class ZookeeperClientConnectionTest {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 默认的watch
        ZooKeeper client = new ZooKeeper("localhost:2181", 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("默认的watch:" + event.getType());
            }
        });


        client.create("/luban-e", "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        doTask(client);

        System.in.read();
    }

    private static void doTask(ZooKeeper client) {
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(1);
                System.out.println(client.getChildren("/",false));
            } catch (InterruptedException e) {
//                e.printStackTrace();
            } catch (KeeperException e) {
//                e.printStackTrace();
            }


        }
    }
}
