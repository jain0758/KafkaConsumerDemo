package com.kafkaexample.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaexample.config.MyKafkaJsonConfig;
import com.kafkaexample.dto.Player;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.Properties;

@Component(value = "jsonConsumer")
public class JsonConsumer implements ConsumerInterface {

    @Autowired
    private MyKafkaJsonConfig myKafkaConfig;

    private KafkaConsumer consumer;

    @PostConstruct
    public void init() {
        consumer = new KafkaConsumer(getConfig());
        consumer.subscribe(Collections.singletonList(myKafkaConfig.getTopicName()));
    }

    public void consume() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            System.out.println(" ######### Started Reading ######## ");
            ConsumerRecords<Integer, JsonNode> records = consumer.poll(1000);
            if (records.count() > 0) {
                for (ConsumerRecord<Integer, JsonNode> record : records) {
                    JsonNode jsonNode = record.value();
                    System.out.println(jsonNode);// prints JSON
                    System.out.println(mapper.treeToValue(jsonNode, Player.class));// prints Object representation of JSON
                }
            }
            System.out.println(" ######### Completed Reading ######## ");
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private Properties getConfig() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, myKafkaConfig.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, getClass().getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, myKafkaConfig.getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, myKafkaConfig.getValueDeserializer());
        return props;
    }
}
