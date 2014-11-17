package com.mj;


import java.util.*;
import java.io.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import javax.xml.bind.DatatypeConverter; 

/** 
 * The KafkaProducer class sends random queries to the consumer on a
 * periodic basis. Used for testing purposes only.
 * @author hchen
 */

public class KafkaProducer {

	public static void main(String[] args) throws Exception{
		
		ArrayList<Integer> rids = new ArrayList<Integer>();
		ArrayList<Integer> cids = new ArrayList<Integer>();
		BuhzerAnalytics ba = new BuhzerAnalytics(); 

		Random random = new Random();
		int iteration = 0;

		while (true) {
			try {
				    Thread.sleep(1000);                 
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
				ba.send(rid, cid, true);
				 
			}
			else {
				int nrid = random.nextInt(3);
				int ncid = random.nextInt(1000);
				boolean dupflag = false;
				for (int i = 0; i < rids.size(); i++) {
					if (rids.get(i) == nrid && cids.get(i) == ncid) {
						dupflag = true;
					}
				}
				if (!dupflag) {
					rids.add(nrid);
					cids.add(ncid);
					ba.send(nrid, ncid, false); 
				}
			}
				


		}
		
		
		//producer.close();
		

	}


}
