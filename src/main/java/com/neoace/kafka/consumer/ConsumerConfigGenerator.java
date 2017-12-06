package com.neoace.kafka.consumer;

import java.util.Properties;

/**
 * Created by liufeng on 2017/12/6.
 */
public class ConsumerConfigGenerator {

    /**
     * 生成consumer配置
     * 默认连接本地服务器，以后确认需要的参数
     * @return
     */
    public static Properties createConsumerConfig(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.41:9092,192.168.5.41:9093,192.168.5.41:9094");
        props.put("group.id", "demo");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

}
