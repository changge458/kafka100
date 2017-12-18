package com.qst.kafka100;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartition implements Partitioner {
	public SimplePartition (VerifiableProperties props) {
	}

	public int partition(Object preKey, int numPartitions) {

		Integer key = (Integer) preKey;

		return key % numPartitions;

	}

}
