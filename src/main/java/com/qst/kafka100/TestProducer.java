package com.qst.kafka100;

import java.util.Properties;

import org.junit.Test;

import java.util.Date;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer {

	public static void main(String[] args) {
		// 初始化参数
		Properties props = new Properties();
		props.put("metadata.broker.list", "s202:9092, s203:9092,s204:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer.class", "kafka.serializer.IntegerEncoder");
		//props.put("partitioner.class", "com.qst.kafka100.SimplePartition");
		props.put("producer.type", "sync");
		//props.put("batch.num.messages", "1000");
		props.put("request.required.acks", "1");

		// 将java的配置转为kafka配置
		ProducerConfig config = new ProducerConfig(props);
		// 使用kafka配置初始化producer
		Producer<Integer, String> producer = new Producer<Integer, String>(config);

		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			String runtime = new Date().toString();
			String msg = "Message Publishing Time - " + runtime + "_" + i;
			//System.out.println(msg);
			// 初始化message，将topic和msg作为参数传到message中
			KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>("t4", i, msg);
			// 使用producer的send方法，发送消息
			producer.send(data);
		}
		producer.close();
		System.out.println(System.currentTimeMillis() -start);
	}

}
