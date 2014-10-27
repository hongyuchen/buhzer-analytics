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
			
			ConsumerIterator<byte[], byte[]> it = kfs.iterator();
				int i = 1 ;
	        	while (it.hasNext()) {
							try {
								String s = new String(it.next().message());
								Message m = (Message) anyDeserialize(s);
								updateDB(m);
								for (int wi = 0; wi < 3; wi++) {
									System.out.println("LINE " + wi + " has " + getCountFromRestaurantID(wi) + " people.");
								}
								System.out.println(tnum + " " + i + ": " + m.toString());
								++i ;
							}
							catch (Exception e) {
								e.printStackTrace();
							}
	        	}
			
		}

		public static int getCountFromRestaurantID(int rid) throws SQLException{

			String JDBC_DRIVER = "com.mysql.jdbc.Driver";
			String DB_URL = "jdbc:mysql://localhost/buhzer";

			String USER = "root";
			String PASS = "1234";
			
			Connection conn = null;
			PreparedStatement stmt = null;
			ResultSet rs = null;


			try {
				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection(DB_URL,USER,PASS);
				System.out.println("Creating statement...");
				stmt = conn.prepareStatement("SELECT COUNT(*) AS COUNT FROM queues WHERE RestaurantID = ?");
				stmt.setInt(1, rid);
				rs = stmt.executeQuery();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
				if (rs.next()) {
					return rs.getInt("COUNT");
				}
				else {
					return -1;
				}
		}




		
		public static int getTimeDiffFromLeave(int cid, int rid) throws SQLException {
			String JDBC_DRIVER = "com.mysql.jdbc.Driver";
			String DB_URL = "jdbc:mysql://localhost/buhzer";

			String USER = "root";
			String PASS = "1234";
			
			Connection conn = null;
			PreparedStatement stmt = null;
			ResultSet rs = null;


			try {
				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection(DB_URL,USER,PASS);
				System.out.println("Creating statement...");
				stmt = conn.prepareStatement("SELECT TIME_TO_SEC(TIMEDIFF(now(), created)) as SECONDS from queues where restaurantId = ? and userId = ?;");
				stmt.setInt(1, rid);
				stmt.setInt(2, cid);
				rs = stmt.executeQuery();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
				if (rs.next()) {
					return rs.getInt("SECONDS");
				}
				else {
					return -1;
				}
		}

	  public static void updateDB(Message m) throws SQLException{

			String JDBC_DRIVER = "com.mysql.jdbc.Driver";
			String DB_URL = "jdbc:mysql://localhost/buhzer";

			String USER = "root";
			String PASS = "1234";
			
			Connection conn = null;
			PreparedStatement stmt = null;

			try {

				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection(DB_URL,USER,PASS);
				System.out.println("Creating statement...");

				switch (m.action) {
					case ADD:
						stmt = conn.prepareStatement("INSERT INTO queues (restaurantID, userId) values (?, ?)", Statement.RETURN_GENERATED_KEYS);
						stmt.setInt(1, m.waitlistID);
						stmt.setString(2, "" + m.userID);
						stmt.executeUpdate();
						break;
					case REMOVE:
						System.out.println("SECONDS: " + getTimeDiffFromLeave(m.userID, m.waitlistID));
						stmt = conn.prepareStatement("DELETE FROM queues WHERE restaurantId = ? AND userId = ?");
						stmt.setInt(1, m.waitlistID);
						stmt.setString(2, "" + m.userID);
						stmt.executeUpdate();
						break;
					default:
						System.out.println("Unknown command.");
						System.out.println(m.action);

				}
			 }catch(SQLException se){
					se.printStackTrace();
			 }catch(Exception e){
					e.printStackTrace();
			 }finally{
					try{
						 if(stmt!=null)
								stmt.close();
					}catch(SQLException se2){
					}
					try{
						 if(conn!=null)
								conn.close();
					}catch(SQLException se){
						 se.printStackTrace();
					}
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


