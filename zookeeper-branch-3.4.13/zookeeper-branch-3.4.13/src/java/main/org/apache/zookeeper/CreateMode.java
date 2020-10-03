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
package org.apache.zookeeper;

import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;

/***
 *  CreateMode value determines how the znode is created on ZooKeeper.
 */
@InterfaceAudience.Public
public enum CreateMode {
    //3.5.3版本增加的，3.5.4/3.6.0版本并不支持，所以在3.5.4/3.6.0等其他版本还需设置另外一个java系统属性：
    // Dzookeeper.emulate353TTLNodes
    //3.5.3后版本已经有七种节点   container 容器节点  当节点的最后一个子节点被删除后，容器节点会自动删除
    // （会有延迟，开启了一个timertask）
    // 一个容器节点只有创建过子节点才会被删除 containermanager 类  首先判断cversion>0 才会加到待删除的列表中
    //  PERSISTENT_WITHTTL TTL节点  客户端断开连接后不会自动删除Znode，
    //  如果该Znode没有子Znode且在给定TTL时间内无修改，该Znode将会被删除；
    //  TTL单位是毫秒，必须大于0且小于或等于 EphemeralType.MAX_TTL
    // PERSISTENT_SEQUENTIAL_WITH_TTL TTL顺序节点  同PERSISTENT_WITH_TTL，且Znode命名末尾自动添加递增编号；
    //  3.4  watch机制只能触发一次，源码可以看到在客户端被remove掉，可以用curator框架解决
    //新版后增加了新的方法  zookeeper.addwatch 针地某个添加监听器，并且是持久化的
    //有两种，特殊的有种是递归的 表示子节点的数据变化也会触发，子节点的子节点变化也会触发
    // 集群管理  kafka
    // 注册中心  dubbo
    // 分布式配置中心 disconf(配置中心还是有一些开源实现的。像百度的Disconf,阿里的Diamond,
    // 携程的Apollo,还有基于Github的pull模式来实现。)
    // 分布式锁  curator 读写锁(自己原生实现分布式锁  临时顺序节点，watch监听上一节点防止惊群效应，判断当前节点是不是最小的
    // ，拿到他前面的上一节点，监听删除节点事件。由于监听事件是异步的，需要阻塞住，可以用countdownlatch await住，在触发事件的逻辑
    // 里面countDown)
    // curator实现
//    InterProcessMutex：分布式可重入排它锁
//    InterProcessSemaphoreMutex：分布式排它锁
//    InterProcessReadWriteLock：分布式读写锁
//    InterProcessMultiLock：将多个锁作为单个实体管理的容器
    /**
     * The znode will not be automatically deleted upon client's disconnect.
     */
    PERSISTENT (0, false, false),
    /**
    * The znode will not be automatically deleted upon client's disconnect,
    * and its name will be appended with a monotonically increasing number.
    */
    PERSISTENT_SEQUENTIAL (2, false, true),
    /**
     * The znode will be deleted upon the client's disconnect.
     */
    EPHEMERAL (1, true, false),
    /**
     * The znode will be deleted upon the client's disconnect, and its name
     * will be appended with a monotonically increasing number.
     */
    EPHEMERAL_SEQUENTIAL (3, true, true);

    private static final Logger LOG = LoggerFactory.getLogger(CreateMode.class);

    private boolean ephemeral;
    private boolean sequential;
    private int flag;

    CreateMode(int flag, boolean ephemeral, boolean sequential) {
        this.flag = flag;
        this.ephemeral = ephemeral;
        this.sequential = sequential;
    }

    public boolean isEphemeral() { 
        return ephemeral;
    }

    public boolean isSequential() { 
        return sequential;
    }

    public int toFlag() {
        return flag;
    }

    /**
     * Map an integer value to a CreateMode value
     */
    static public CreateMode fromFlag(int flag) throws KeeperException {
        switch(flag) {
        case 0: return CreateMode.PERSISTENT;

        case 1: return CreateMode.EPHEMERAL;

        case 2: return CreateMode.PERSISTENT_SEQUENTIAL;

        case 3: return CreateMode.EPHEMERAL_SEQUENTIAL ;

        default:
            String errMsg = "Received an invalid flag value: " + flag
                    + " to convert to a CreateMode";
            LOG.error(errMsg);
            throw new KeeperException.BadArgumentsException(errMsg);
        }
    }
}
