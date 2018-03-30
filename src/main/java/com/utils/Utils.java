package com.utils;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public class Utils
{		
	public static Properties getConfig(String clientId)
	{
		/*Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConstants.BOOTSTRAP);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ConsumerConstants.KEY_SERIALIZER);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConsumerConstants.VALUE_SERIALIZER);*/
		
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConstants.BOOTSTRAP);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, clientId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConsumerConstants.KEY_SERIALIZER);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConsumerConstants.VALUE_SERIALIZER);
		return props;
	}
}
