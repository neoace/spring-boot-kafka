package com.neoace.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by liufeng on 2017/12/6.
 */
public class Consumer implements Runnable {

    /**
     * 保存每个主题的线程池
     */
    public  static final Map<String, Object> poolCache = new ConcurrentHashMap<String,Object>();

    /**
     * 消费者
     */
    private KafkaConsumer consumer;

    /**
     * 主题
     */
    private String topic;


    /**
     * 线程数量，一般就是Topic的分区数量
     */
    private int numThreads;


    @Override
    public void run() {

        this.consumer.subscribe(Arrays.asList(this.topic));

        while(true) {
            //1. 获取待消费数据
            ConsumerRecords<String, String> records = this.consumer.poll(numThreads);

            // 2. 创建线程池
            ExecutorService executorService = (ExecutorService) poolCache.get(this.topic);
            if(null != executorService){
            }else{
                executorService = Executors.newFixedThreadPool(this.numThreads);
                poolCache.put(this.getTopic(), executorService);
            }

            // 3. 构建数据输出对象
            int threadNumber = 0;
            for (final ConsumerRecord<String, String> record : records) {
                executorService.submit(new ConsumerRecordProcesser(record, threadNumber, this.topic));
                threadNumber++;
            }
        }
    }

    public Consumer() {
    }

    public Consumer(String topic, Properties consumerConfig, int numThreads) {
        this.topic = topic;
        this.numThreads = numThreads;
        this.consumer = new KafkaConsumer(consumerConfig);
    }


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

}
