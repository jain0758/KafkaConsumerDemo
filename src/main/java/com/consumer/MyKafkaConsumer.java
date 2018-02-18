package com.consumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MyKafkaConsumer
{
	KafkaConsumer<Long, String> consumer;
	private final static String TOPIC = "anshumanTopic";
	private final static String BOOTSTRAP_SERVER = "localhost:9092";

	private void consumeData()
	{
		consumer = new KafkaConsumer<Long, String>(getProperties());
		consumer.subscribe(Collections.singletonList(TOPIC));
		int recordReadCount = 0;
		while (true)
		{
			ConsumerRecords<Long, String> records = consumer.poll(1000);
			if(records.count() == 0) {
				recordReadCount++;
				if(recordReadCount > 100)
					break;
				else
					continue;
			}
			for(ConsumerRecord<Long, String> record : records) {
				System.out.println("KEY : "+record.key()+ " VALUE : "+record.value());
			}
			consumer.commitAsync();
		}
		consumer.close();
        System.out.println("DONE");
	}

	private Properties getProperties()
	{
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, getClass().getName());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		return props;
	}

	public static void main(String[] args)
	{
		MyKafkaConsumer consumer = new MyKafkaConsumer();
		consumer.consumeData();
	}
}
