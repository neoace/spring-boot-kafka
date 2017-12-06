package com.neoace.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by liufeng on 2017/12/6.
 */
public class ConsumerRecordProcesser implements Runnable {

    private ConsumerRecord record;
    private int threadNumber;
    private String topic;

    @Override
    public void run() {
        System.out.println(Thread.currentThread()+"_____"+this.threadNumber + "____" + this.record.offset() +"_____"+ topic + "____" + this.record.value());
    }

    public ConsumerRecordProcesser(ConsumerRecord record, int threadNumber, String topic) {
        this.record = record;
        this.threadNumber = threadNumber;
        this.topic = topic;
    }

    public ConsumerRecord getRecord() {
        return record;
    }

    public void setRecord(ConsumerRecord record) {
        this.record = record;
    }

    public int getThreadNumber() {
        return threadNumber;
    }

    public void setThreadNumber(int threadNumber) {
        this.threadNumber = threadNumber;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
