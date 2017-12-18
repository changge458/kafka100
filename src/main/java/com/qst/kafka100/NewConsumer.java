package com.qst.kafka100;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class NewConsumer {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("bootstrap.servers", "s202:9092,s203:9092,s204:9092");
		props.put("group.id", "g1");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		// 分区
		List<TopicPartition> partitions = new ArrayList<TopicPartition>();
		TopicPartition par = new TopicPartition("t8", 0);
		partitions.add(par);
		consumer.assign(partitions);

		// 手动控制偏移量
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
		offsets.put(par, new OffsetAndMetadata(0));
		consumer.commitSync(offsets);
		
		while(true){
			ConsumerRecords<String, String> records = consumer.poll(100);
			for(ConsumerRecord<String, String> record : records){
				if(record.value().startsWith("a2")){
					long offset = record.offset();
					consumer.seek(par, offset);
					while(true){
						ConsumerRecords<String, String> records2 = consumer.poll(100);
						for(ConsumerRecord<String, String> record2 : records2){
							System.out.println("offset= " + record2.offset() + ", key= " + record2.key()+ ", value= "+ record2.value());
						}
					}
				}
			}
		}
		
	}
}
