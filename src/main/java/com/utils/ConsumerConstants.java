package com.utils;

public class ConsumerConstants
{
	public static String BOOTSTRAP = "localhost:9092";

	public static String KEY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

	public static String VALUE_SERIALIZER = "org.apache.kafka.connect.json.JsonDeserializer";
	
	public static String TOPIC = "anshumanTopic";
}
