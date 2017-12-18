package com.qst.kafka100;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class NewProducer {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "s202:9092,s203:9092,s204:9092");
		props.put("acks", "0");
		props.put("batch.size", 16384);
		props.put("linger.ms", 500);
		props.put("buffer.memory", 33554432);
		props.put("partitioner.class", "com.qst.kafka100.NewPartition");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 默认异步
		final Producer<String , String> producer = new KafkaProducer<String , String>(props);
		
		String[] arr = {"a","b","c","d","e"};
		Random r = new Random();
		
		for(int j = 0; j < 100 ; j++){
			//3
			int i = r.nextInt(5);
			//c
			String key = arr[i];
			//c1
			String msg = key+j;
			ProducerRecord<String , String> record = new ProducerRecord<String , String>("t8", key, msg);
			
			producer.send(record);
			
		}
		producer.close();
		
		

	}
}
