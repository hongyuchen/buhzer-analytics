package com.mj;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.*;
import java.util.*;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.xml.bind.DatatypeConverter; 

/** 
 * The KafkaMultiThreadedConsumer class takes incoming requests from 
 * a producer and processes the information. Debugging results are
 * shown in the terminal
 * @author hchen
 */

public class KafkaMultiThreadedConsumer {
	
	
	public static class KafkaPartitionConsumer implements Runnable {

		private int tnum ;
		private KafkaStream kfs ;
		
		public KafkaPartitionConsumer(int id, KafkaStream ks) {
			tnum = id ;
			kfs = ks ;
		}
		
		
		public void run() {
			System.out.println("This is thread " + tnum) ;

			try {

			BuhzerAnalytics ba = new BuhzerAnalytics();//new
			
			ConsumerIterator<byte[], byte[]> it = kfs.iterator();
				int i = 1 ;
	        	while (it.hasNext()) {
							try {
								String s = new String(it.next().message());
								Message m = (Message) Serializer.anyDeserialize(s);
								ba.updateDB(m);
								for (int wi = 0; wi < 3; wi++) {
									System.out.println("Estimated wait time for line " + wi + " - " + ba.estimateAsString(wi));
								}
								System.out.println(tnum + " " + i + ": " + m.toString());
								++i ;
							}
							catch (Exception e) {
								e.printStackTrace();
							}
	        	}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}

	
	
	public static class MultiKafka {
		
		public void run() {
			
			
			
			
			
		}
		
	}
	
	
	public static void main(String[] args) throws Exception{
		
		Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "test_group");
        props.put("zookeeper.session.timeout.ms", "413");
        props.put("zookeeper.sync.time.ms", "203");
        props.put("auto.commit.interval.ms", "1000");
        // props.put("auto.offset.reset", "smallest"); 
        
      
		
        ConsumerConfig cf = new ConsumerConfig(props) ;
        
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(cf) ;
        
        String topic = "mytopic" ;
        
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(3));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
	
        ExecutorService executor = Executors.newFixedThreadPool(3); ;
        
        int threadnum = 0 ;
        
        for(KafkaStream<byte[],byte[]> stream  : streams) {
        	
        	executor.execute(new KafkaPartitionConsumer(threadnum,stream));
        	++threadnum ;
        }
        
        
        
        
        // consumer.shutdown(); 
	}

	
	

}


