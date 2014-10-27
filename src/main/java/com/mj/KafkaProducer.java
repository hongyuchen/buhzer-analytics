package com.mj;


import java.util.*;
import java.io.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import javax.xml.bind.DatatypeConverter; 

enum WaitlistAction {
	ADD,
	REMOVE
}

class Message implements Serializable{
	public int userID;
	public int waitlistID;
	public WaitlistAction action;
	public Message(int u, int w, WaitlistAction a) {
		userID = u;
		waitlistID = w;
		action = a;
	}
	public String toString() {
		return "USER_ID:" + userID + "|WAITLIST_ID:" + waitlistID;
	}
}

public class KafkaProducer {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		Properties props = new Properties();
		 
		props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");
		 
		ProducerConfig config = new ProducerConfig(props);
		
		Producer<String, String> producer = new Producer<String, String>(config);
		
		String date = "04092014" ;
		// String topic = "my-replicated-topic" ;
		String topic = "mytopic" ;


		ArrayList<Integer> rids = new ArrayList<Integer>();
		ArrayList<Integer> cids = new ArrayList<Integer>();

		Random random = new Random();
		int iteration = 0;

		while (true) {
			try {
				    Thread.sleep(1000);                 //1000 milliseconds is one second.
			} catch(InterruptedException ex) {
				    Thread.currentThread().interrupt();
			}

			iteration++;

			int rand = random.nextInt(3);

			if (rids.size() > 0 && random.nextInt(4) == 0) {
				int rmidx = random.nextInt(rids.size());
				int rid = rids.get(rmidx);
				int cid = cids.get(rmidx);
				rids.remove(rmidx);
				cids.remove(rmidx);
				Message m = new Message(
						cid,
						rid,
						WaitlistAction.REMOVE);
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, String.valueOf(iteration), anySerialize(m));
				producer.send(data);
				System.out.println(iteration + "|" + rid + "|" + cid);
				 
			}
			else {
				int nrid = random.nextInt(3);
				int ncid = random.nextInt(1000);
				rids.add(nrid);
				cids.add(ncid);
				Message m = new Message(
						ncid,
						nrid,
						WaitlistAction.ADD);
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, String.valueOf(iteration), anySerialize(m));
				producer.send(data);
				System.out.println(iteration + "|" + nrid + "|" + ncid);

			}
				


		}
		
		
		//producer.close();
		

	}

public static String anySerialize(Object o) throws IOException { 
                ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
                ObjectOutputStream oos = new ObjectOutputStream(baos); 
                oos.writeObject(o); 
                oos.close(); 
                return DatatypeConverter.printBase64Binary(baos.toByteArray()); 
        } 
        public static Object anyDeserialize(String s) throws IOException, 
ClassNotFoundException { 
                ByteArrayInputStream bais = new 
ByteArrayInputStream(DatatypeConverter.parseBase64Binary(s)); 
                ObjectInputStream ois = new ObjectInputStream(bais); 
                Object o = ois.readObject(); 
                ois.close(); 
                return o; 
        } 

}
