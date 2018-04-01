package com.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.dto.Player;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonConsumer implements com.consumer.KafkaConsumer
{
	private final String TOPIC_NAME = "anshumanTopic";
	
	KafkaConsumer<Integer, JsonNode> consumer;

	public void consume()
	{
		consumer = new KafkaConsumer<Integer, JsonNode>(getConfig());
		consumer.subscribe(Arrays.asList(TOPIC_NAME));
		ObjectMapper mapper = new ObjectMapper();
		try
		{
			System.out.println(" ######### Started Reading ######## ");
			ConsumerRecords<Integer, JsonNode> records = consumer.poll(1000);
			if (records.count() > 0)
			{
				for (ConsumerRecord<Integer, JsonNode> record : records)
				{
					JsonNode jsonNode = record.value();
					// prints JSON
					System.out.println(jsonNode);
					// prints Object representation of JSON
					System.out.println(mapper.treeToValue(jsonNode, Player.class));
				}
			}
			System.out.println(" ######### Completed Reading ######## ");
		} 
		catch (JsonProcessingException e) {
			e.printStackTrace();
		} 
		finally {
			consumer.close();
		}
	}

	private Properties getConfig()
	{
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, getClass().getName());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
		return props;
	}
}
