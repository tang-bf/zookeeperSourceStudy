package com.tbh.client.zkclient;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;

import java.io.IOException;

/**
 *
 */
public class ZkoClientWatchTest {

    public static void main(String[] args) throws IOException {
        ZkClient zk = new ZkClient("localhost:2181",10000, 10000, new SerializableSerializer());

        zk.writeData("/zkclient", "123");
    }
}
