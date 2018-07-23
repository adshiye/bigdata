package com.thinkive.bigdata.wangwei.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.thinkive.base.jdbc.DataRow;
import com.thinkive.base.util.JsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaClient
{
	
	private static Logger logger = LoggerFactory.getLogger(KafkaClient.class);
	
	private Producer<String, String> producer = null;
	
	private static KafkaClient client = null;
	
	private final static String DEFAULT_TOPIC = "savekudutest";
	
	private final static String PROPERTY_FILE_NAME = "kafka.properties";
	
	private KafkaClient() throws Exception
	{
		InputStream in = KafkaClient.class.getClassLoader().getResourceAsStream(PROPERTY_FILE_NAME);
		Properties props = new Properties();
		props.load(in);
		producer = new KafkaProducer<String, String>(props);
	}
	
	public static KafkaClient getInstance() throws Exception
	{
		if (client == null)
		{
			client = new KafkaClient();
		}
		return client;
	}
	
	/**
	 * @param topic ����
	 * @param msg ��Ϣ
	 * @return
	 */
	public void send(String topic, String msg) throws Exception
	{
		client.producer.send(new ProducerRecord<String, String>(topic, msg)).get(10, TimeUnit.SECONDS);
	}
	
	/**
	 * @param topic ����
	 * @param msg ��Ϣ
	 * @return
	 */
	public void send(String topic, DataRow message) throws Exception
	{
		String msg = JsonHelper.getJSONString(message);
		send(topic, msg);
	}
	
	public DataRow receive()
	{
		return null;
	}
	
	public DataRow receive(String name)
	{
		return null;
	}
	
	public void send(DataRow message) throws Exception
	{
		send(DEFAULT_TOPIC, message);
	}
}
