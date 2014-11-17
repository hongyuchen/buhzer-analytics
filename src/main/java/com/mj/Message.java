package com.mj;

import java.util.*;
import java.io.*;

/**
 * The Message class represents a possible Buhzer request. Each request
 * is represented by a userID for an incoming customer, a waitlistID for
 * identifying an existing line, and an action (either add or remove).
 * @author hchen
 */

public class Message implements Serializable{

	public int userID;
	public int waitlistID;
	public WaitlistAction action;

	/** Initializes a message. */
	public Message(int u, int w, WaitlistAction a) {
		userID = u;
		waitlistID = w;
		action = a;
	}

	/** Displays the current message as a string. */
	public String toString() {
		return "USER_ID:" + userID + "|WAITLIST_ID:" + waitlistID;
	}
}

