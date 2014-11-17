package com.mj;

import java.util.*;
import java.io.*;

import javax.xml.bind.DatatypeConverter;

/**
 * The Serializer Class takes incoming objects and serializes them into
 * a bitstring to be sent over Kafka. It can also be used to deseralize
 * the bitstring back into an object.
 * @author hchen
 */

public class Serializer {

	      /** Takes an object and serializes it. */
				public static String anySerialize(Object o) throws IOException { 
                ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
                ObjectOutputStream oos = new ObjectOutputStream(baos); 
                oos.writeObject(o); 
                oos.close(); 
                return DatatypeConverter.printBase64Binary(baos.toByteArray()); 
        } 

				/** Takes a string and deseralizes it. */
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
