package com.kafkaexample.consumer;

import com.kafkaexample.config.MyKafkaAvroConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Component(value = "avroConsumer")
public class AvroConsumer implements ConsumerInterface {

    @Autowired
    private MyKafkaAvroConfig myKafkaConfig;

    private KafkaConsumer consumer;

    private DatumReader<GenericRecord> datumReader;

	@PostConstruct
    public void init() {
        consumer = new KafkaConsumer(getConfig());
        consumer.subscribe(Collections.singletonList(myKafkaConfig.getTopicName()));
        datumReader = new GenericDatumReader<>(getSchema());
    }

    @Override
    public void consume() {
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(1000);
            System.out.println(" RECORDS FOUND :: " + records.count());
            if (records.count() > 0) {
                System.out.println(" ######### Started Reading ######## ");
                for (ConsumerRecord<String, byte[]> record : records) {
                    byte[] data = record.value();
                    SeekableByteArrayInput input = new SeekableByteArrayInput(data);
                    try {
						DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input, datumReader);
                        System.out.println(parseGenericRecord(dataFileReader).get(0));
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
                System.out.println(" ######### Completed Reading ######## ");
            }
        }
    }

    private List<String> parseGenericRecord(DataFileReader<GenericRecord> dataFileReader) {
        List<String> list = new ArrayList<>();
        while (dataFileReader.hasNext()) {
            GenericRecord user = (GenericRecord) dataFileReader.next();
            StringBuilder builder = new StringBuilder("[ Name :: " + user.get("name"));
            builder.append(", Sport Name :: " + user.get("sportName"));
            builder.append(", Jersey Number :: " + user.get("jerseyNumber"));
            builder.append(", Age :: " + user.get("age"));
            builder.append(", Height :: " + user.get("height") + " ]");
            list.add(builder.toString());
        }
        return list;
    }

    private Schema getSchema() {
        File file;
        Schema schema = null;
        try {
            file = ResourceUtils.getFile("classpath:player.avsc");
            schema = new Schema.Parser().parse(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
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
