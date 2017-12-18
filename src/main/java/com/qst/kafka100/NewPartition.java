package com.qst.kafka100;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class NewPartition implements Partitioner {

	int i;
	
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		i++;
		String key0 = (String) key;
		if(key0.startsWith("a")){
			return 0;
		}
		if(key0.startsWith("b")){
			return 1;
		}
		return 2;
		
		
	}

	public void close() {
		System.out.println(i);
		
		
	}
	
	

}
