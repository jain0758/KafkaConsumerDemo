package com.consumer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

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

public class AvroConsumer implements com.consumer.KafkaConsumer
{
	private final String TOPIC_NAME = "anshumanTopic";

	private KafkaConsumer<String, byte[]> consumer;
	
	private DatumReader<GenericRecord> datumReader;
	
	private DataFileReader<GenericRecord> dataFileReader;

	public AvroConsumer()
	{
		consumer = new KafkaConsumer<String, byte[]>(getConfig());
		consumer.subscribe(Arrays.asList(TOPIC_NAME));
		datumReader = new GenericDatumReader<GenericRecord>(getSchema());
	}

	@Override
	public void consume()
	{
		while (true)
		{
			ConsumerRecords<String, byte[]> records = consumer.poll(1000);
			System.out.println(" RECORDS FOUND :: " + records.count());
			if (records.count() > 0)
			{
				System.out.println(" ######### Started Reading ######## ");
				for (ConsumerRecord<String, byte[]> record : records)
				{
					byte[] data = record.value();
					SeekableByteArrayInput input = new SeekableByteArrayInput(data);
					try
					{
						dataFileReader = new DataFileReader<GenericRecord>(input, datumReader);
						System.out.println(parseGenericRecord(dataFileReader).get(0));
					} catch (IOException e1)
					{
						e1.printStackTrace();
					}
				}
				System.out.println(" ######### Completed Reading ######## ");
			}
		}
	}
	
	private List<String> parseGenericRecord(DataFileReader<GenericRecord> dataFileReader) {
		List<String> list = new ArrayList<String>();
		while (dataFileReader.hasNext())
		{
			GenericRecord user = (GenericRecord) dataFileReader.next();
			StringBuilder builder = new StringBuilder("[ Name :: "+user.get("name"));
			builder.append(", Sport Name :: "+user.get("sportName"));
			builder.append(", Jersey Number :: "+user.get("jerseyNumber"));
			builder.append(", Age :: "+user.get("age"));
			builder.append(", Height :: "+user.get("height")+" ]");
			list.add(builder.toString());
		}
		return list;
	}

	private Schema getSchema()
	{
		String filePath = "G:\\Workspaces\\newWorkspace\\KafkaConsumerDemo\\src\\main\\java\\com\\consumer\\player.avsc";
		Schema schema = null;
		try
		{
			schema = new Schema.Parser().parse(new File(filePath));
		} catch (IOException e)
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
		//props.put("deserializer.class", "kafka.serializer.StringDecoder");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		return props;
	}
}
