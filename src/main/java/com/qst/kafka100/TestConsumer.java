package com.qst.kafka100;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class TestConsumer {

	@SuppressWarnings("deprecation")
	@Test
	public void TestConsumer() throws Exception {
		Properties props = new Properties();
		props.put("zookeeper.connect", "s202:2181,s203:2181,s204:2181");
		props.put("group.id", "g1");
		ConsumerConfig conf = new ConsumerConfig(props);

		final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(conf);

		final Map<String, Integer> topicMap = new HashMap<String, Integer>();

		final String topic = "t4";

		// Define single thread for topic
		topicMap.put(topic, 1);
		// 通过consumer获取流映射(topic,list<stream>)
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);
		// 通过topic获取数据流
		List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);
		// 通过for打印出消息
		for (final KafkaStream<byte[], byte[]> stream : streamList) {
			new Thread() {
				public void run() {
					ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
					while (consumerIte.hasNext())
						System.out.println(
								Thread.currentThread().getName() + "::" + new String(consumerIte.next().message()));

					if (consumer != null)
						consumer.shutdown();

				}
			}.start();
		}
		while(true){
			Thread.sleep(2000);
		}
	}
}
