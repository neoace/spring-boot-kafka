package com.neoace.kafka;

import com.neoace.kafka.consumer.Consumer;
import com.neoace.kafka.consumer.ConsumerConfigGenerator;
import com.neoace.kafka.topic.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.messaging.support.MessageBuilder;

import java.util.List;
import java.util.Properties;


@SpringBootApplication
public class SpringKafkaApplication {
    private static final Logger logger = LoggerFactory.getLogger(SpringKafkaApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaApplication.class, args);
    }


    @Bean
    public ApplicationRunner runner(KafkaTemplate<Object, Object> template) {
        return args -> {

            /*
            // 发送int型数据，需要定义相应的序列化类和解析器
            template.send(MessageBuilder.withPayload(42)
                    .setHeader(KafkaHeaders.TOPIC, "demo")
                    .build());
            */

            template.send(MessageBuilder.withPayload("43")
                    .setHeader(KafkaHeaders.TOPIC, "demo")
                    .build());
            Thread.sleep(5000);


            /* 从zookeeper 获取topic 列表 */
            String connectString = "192.168.5.41:2181";
            List<String> topics = Topic.listTopicsFromZookeeper(connectString);

            for(String topic : topics){
                //生成consumer配置
                Properties props = ConsumerConfigGenerator.createConsumerConfig();

                Consumer consumer = new Consumer(topic, props, 5);
                (new Thread(consumer)).start();
            }

            Thread.sleep(50000);
        };
    }

    @Bean
    public StringJsonMessageConverter converter() {
        return new StringJsonMessageConverter();
    }

//    @Component
//    @KafkaListener(groupId = "demo", topics = "demo")
//    public static class Listener {
//
//        @KafkaHandler
//        public void intListener(Integer in) {
//            System.out.println("Got an int: " + in);
//        }
//
//        @KafkaHandler
//        public void stringListener(String in) {
//            System.out.println("Got a string: " + in);
//        }
//
//    }
}
