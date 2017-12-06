package com.neoace.kafka.topic;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * Created by liufeng on 2017/12/6.
 */
public class Topic {

    /**
     * 从zookeeper中获取主题列表
     * @param zookeeperServer
     * @return
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static List<String> listTopicsFromZookeeper(String zookeeperServer) throws IOException, KeeperException, InterruptedException {

        int sessionTimeout = 4000;
        Watcher watcher = new Watcher() {
            public void process(WatchedEvent event) {
            }
        };
        ZooKeeper zooKeeper = new ZooKeeper(zookeeperServer, sessionTimeout, watcher);
        List<String> topics = zooKeeper.getChildren("/brokers/topics", false);
        //去掉kafka默认主题
        topics.remove("__consumer_offsets");

        return topics;
    }

}
                                                  