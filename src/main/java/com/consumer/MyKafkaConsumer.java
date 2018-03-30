package com.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.dto.Player;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.utils.ConsumerConstants;
import com.utils.Utils;

public class MyKafkaConsumer
{
	KafkaConsumer<Integer, JsonNode> consumer;

	public void consumeData()
	{
		consumer = new KafkaConsumer<Integer, JsonNode>(getConfig());
		consumer.subscribe(Arrays.asList(ConsumerConstants.TOPIC));
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
		System.out.println("DONE");
	}

	private Properties getConfig()
	{
		return Utils.getConfig(getClass().getName());
	}
}
