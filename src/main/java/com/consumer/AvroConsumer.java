package com.consumer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class AvroConsumer implements com.consumer.KafkaConsumer
{
	private final String TOPIC_NAME = "anshumanTopic";
	
	KafkaConsumer<String, byte[]> consumer;
	
	@Override
	public void consume()
	{
		consumer = new KafkaConsumer<String, byte[]>(getConfig());
		consumer.subscribe(Arrays.asList(TOPIC_NAME));
		ConsumerRecords<String, byte[]> records = consumer.poll(1000);
		System.out.println(" RECORDS FOUND :: "+records.count());
		if (records.count() > 0)
		{
			System.out.println(" ######### Started Reading ######## ");
			for (ConsumerRecord<String, byte[]> record : records)
			{
				byte [] jsonNode = record.value();
								
			}
			System.out.println(" ######### Completed Reading ######## ");
		}
	}
		
	public Schema getSchema() {
		String filePath = "G:\\Workspaces\\newWorkspace\\KafkaConsumerDemo\\src\\main\\java\\com\\consumer\\player.avsc";
		Schema schema = null;
		try
		{
			schema = new Schema.Parser().parse(new File(filePath));
		} 
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return schema;
	}
	
	private Properties getConfig()
	{
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, getClass().getName());
		props.put("deserializer.class", "kafka.serializer.StringDecoder");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		return props;
	}
}
