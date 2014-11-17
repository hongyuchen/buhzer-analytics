package com.mj;

import java.util.*;
import java.io.*;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import javax.xml.bind.DatatypeConverter; 

/**
 * The BuhzerAnalytics class handles the computation for Buhzer and connects with the
 * MySQL database.
 * @author hchen
 */

public class BuhzerAnalytics {

	 private Producer<String, String> producer;
	 private String date;
	 private String topic;
	 private int iteration;

	 /** Access information for mysql database. */
	 private final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	 private final String DB_URL = "jdbc:mysql://localhost/buhzer";
	 private final String USER = "root";
	 private final String PASS = "1234";
	 
	 private Connection conn;

	 /** Initializes BuhzerAnalytics class. */
   public BuhzerAnalytics() throws Exception{
			Properties props = new Properties();
			 
			props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			props.put("request.required.acks", "1");
			 
			ProducerConfig config = new ProducerConfig(props);
			
			producer = new Producer<String, String>(config);
			date = "04092014";
			topic = "mytopic";
			iteration = 0;

			
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DB_URL,USER,PASS);
	 }

	 /** Closes the connection. */
	 public void close() {
				try{
					 if(conn!=null)
							conn.close();
				}catch(SQLException se){
					 se.printStackTrace();
				}
	 }

	 
	 /** Sends an incoming request to Kafka. Requires restaurantID, userID, and Add/Remove command*/
   public void send(int rid, int cid, boolean remove) throws Exception{

		Message m = new Message(
			cid,
			rid,
			remove ? WaitlistAction.REMOVE : WaitlistAction.ADD);

		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, String.valueOf(iteration), Serializer.anySerialize(m));
		producer.send(data); 
		System.out.println(iteration + "|" + rid + "|" + cid);
		iteration += 1;
	 }

	 /** Estimates wait time given a restaurantID. */
	 public int estimate(int rid) throws SQLException {
			int line_ct = getCountFromRestaurantID(rid);
			int avg_time = getWaittimeFromRestaurantID(rid);
			int total_time = line_ct * avg_time;
			return total_time;
	 }

	 /** Outputs time estimate as a string in HH:MM:SS format. */
	 public String estimateAsString(int rid) throws SQLException{
			int total_time = estimate(rid);
			int hours = total_time / 3600;
			int rmdr = total_time % 3600;
			int mins = rmdr / 60;
			int secs = rmdr % 60;
			return hours + ":" + mins + ":" + secs;
	 }


	 
	 /** Outputs average wait time from restaurantID. */
	public int getWaittimeFromRestaurantID(int rid) throws SQLException {

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement("SELECT AVG(timediff) AS AVG FROM timediffs WHERE RestaurantID = ?");
			stmt.setInt(1, rid);
			rs = stmt.executeQuery();
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		if (rs.next()) {
			return rs.getInt("AVG");
		}
		else {
			return -1;
		}
	}

	/** Gets the number of people in a restaurant line */
	public int getCountFromRestaurantID(int rid) throws SQLException{

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
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

	/** Gets the time that a user spent in a restaurant when that person leaves. */
	public int getTimeDiffFromLeave(int cid, int rid) throws SQLException {

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
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

	/** Updates time differences when a person leaves. */
	public void updateTimeDiff(int userId, int waitlistId, int timeDiff) throws SQLException {
		PreparedStatement stmt = null;

		try {

			stmt = conn.prepareStatement("INSERT INTO timediffs (restaurantID, userId, timediff) values (?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
			stmt.setInt(1, waitlistId);
			stmt.setString(2, "" + userId);
			stmt.setInt(3, timeDiff);
			stmt.executeUpdate();

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
		 }
		
	}

	/** Updates the database when an incoming request is handled. */
	public void updateDB(Message m) throws SQLException {

		PreparedStatement stmt = null;

		try {

			switch (m.action) {
				case ADD:
					stmt = conn.prepareStatement("INSERT INTO queues (restaurantID, userId) values (?, ?)", Statement.RETURN_GENERATED_KEYS);
					stmt.setInt(1, m.waitlistID);
					stmt.setString(2, "" + m.userID);
					stmt.executeUpdate();
					break;
				case REMOVE:
					int timeDiff = getTimeDiffFromLeave(m.userID, m.waitlistID);
					updateTimeDiff(m.userID, m.waitlistID, timeDiff);
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
		 }
		
	}
	
}



