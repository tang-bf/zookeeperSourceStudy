/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.management.JMException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * This class starts and runs a standalone ZooKeeperServer.
 */
@InterfaceAudience.Public
public class ZooKeeperServerMain {
    // //单机server
    private static final Logger LOG =
        LoggerFactory.getLogger(ZooKeeperServerMain.class);

    private static final String USAGE =
        "Usage: ZooKeeperServerMain configfile | port datadir [ticktime] [maxcnxns]";

    private ServerCnxnFactory cnxnFactory;

    /*
     * Start up the ZooKeeper server.
     *
     * @param args the configfile or the port datadir [ticktime]
     */
    public static void main(String[] args) {
        ZooKeeperServerMain main = new ZooKeeperServerMain();
        try {
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            System.exit(2);
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            System.exit(2);
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            System.exit(1);
        }
        LOG.info("Exiting normally");
        System.exit(0);
    }

    protected void initializeAndRun(String[] args)
        throws ConfigException, IOException
    {
        try {
            ManagedUtil.registerLog4jMBeans();
        } catch (JMException e) {
            LOG.warn("Unable to register log4j JMX control", e);
        }

        ServerConfig config = new ServerConfig();
        if (args.length == 1) {
            config.parse(args[0]);
        } else {
            config.parse(args);
        }

        runFromConfig(config);
    }

    /**
     * Run from a ServerConfig.
     * @param config ServerConfig to use.
     * @throws IOException
     */
    public void runFromConfig(ServerConfig config) throws IOException {
        LOG.info("Starting server");
        FileTxnSnapLog txnLog = null;
        try {
            // Note that this thread isn't going to be doing anything else,
            // so rather than spawning another thread, we will just call
            // run() in this thread.
            // create a file logger url from the command line args
            final ZooKeeperServer zkServer = new ZooKeeperServer();
            // Registers shutdown handler which will be used to know the
            // server error or shutdown state changes.
            final CountDownLatch shutdownLatch = new CountDownLatch(1);
            zkServer.registerServerShutdownHandler(
                    new ZooKeeperServerShutdownHandler(shutdownLatch));
            //日志目录  数据目录  工具类
            txnLog = new FileTxnSnapLog(new File(config.dataLogDir), new File(
                    config.dataDir));
            //3.6版本上加了 jvmpausemonitor（hadoop2.7）监控jvm暂停时间   GarbageCollectorMXBean（jdk原生获得gc的信息）
            // 3.5adminserver factory.createadminserver 用的是jetty
            /**
             *  adminServer = AdminServerFactory.createAdminServer();
             *             adminServer.setZooKeeperServer(zkServer);
             *             adminServer.start();
             *             Class.forName("org.apache.zookeeper.server.admin.JettyAdminServer");
             */
            txnLog.setServerStats(zkServer.serverStats());
            zkServer.setTxnLogFactory(txnLog);
            zkServer.setTickTime(config.tickTime);
            zkServer.setMinSessionTimeout(config.minSessionTimeout);
            zkServer.setMaxSessionTimeout(config.maxSessionTimeout);
            // 获取建立socket工厂，工厂方法模式 m默认 nio  可配置为netty
            cnxnFactory = ServerCnxnFactory.createFactory();
            // 建立socket,默认是NIOServerCnxnFactory（是一个线程）
            //配置并绑定端口
            cnxnFactory.configure(config.getClientPortAddress(),
                    config.getMaxClientCnxns());
            //加载数据  初始化处理请求链
            cnxnFactory.startup(zkServer);
            /**3.5后
             * containerManager = new ContainerManager(zkServer.getZKDatabase(), zkServer.firstProcessor,
             *                     Integer.getInteger("znode.container.checkIntervalMs", (int) TimeUnit.MINUTES.toMillis(1)),
             *                     Integer.getInteger("znode.container.maxPerMinute", 10000)
             *             );
             *             containerManager.start();
             *             开启timertask  删除容器节点  容器节点  ttl节点
             * protected Collection<String> getCandidates() {
             *         Set<String> candidates = new HashSet<String>();
             *         for (String containerPath : zkDb.getDataTree().getContainers()) {
             *             DataNode node = zkDb.getDataTree().getNode(containerPath);
             *             /*
             *                 cversion > 0: keep newly created containers from being deleted
             *                 before any children have been added. If you were to create the
             *                 container just before a container cleaning period the container
             *                 would be immediately be deleted.
             *              */
//             *if ((node != null) && (node.stat.getCversion() > 0) &&
//             *(node.getChildren().size() == 0)){
//             *candidates.add(containerPath);
//             *}
//             *}
//             *for (String ttlPath : zkDb.getDataTree().getTtls()) {
//             *DataNode node = zkDb.getDataTree().getNode(ttlPath);
//             *if (node != null) {
//             *Set<String> children = node.getChildren();
//             *if ((children == null) || (children.size() == 0)) {
//             *if (EphemeralType.get(node.stat.getEphemeralOwner()) == EphemeralType.TTL) {
//             *long elapsed = getElapsed(node);
//             *long ttl = EphemeralType.TTL.getValue(node.stat.getEphemeralOwner());
//             *if ((ttl != 0) && (getElapsed(node) > ttl)) {
//             *candidates.add(ttlPath);
//             *}
//             *}
//             *}
//             *}
//             *}
//             *return candidates;
//             *}

            // Watch status of ZooKeeper server. It will do a graceful shutdown
            // if the server is not running or hits an internal error.
            shutdownLatch.await();
            shutdown();

            cnxnFactory.join();
            if (zkServer.canShutdown()) {
                zkServer.shutdown(true);
            }
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Server interrupted", e);
        } finally {
            if (txnLog != null) {
                txnLog.close();
            }
        }
    }

    /**
     * Shutdown the serving instance
     */
    protected void shutdown() {
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }
    }

    // VisibleForTesting
    ServerCnxnFactory getCnxnFactory() {
        return cnxnFactory;
    }
}
