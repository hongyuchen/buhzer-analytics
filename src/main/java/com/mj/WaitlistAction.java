package com.mj;

/**
 * Instructions for possible incoming actions to the queue.
 * @author hchen
 */

public enum WaitlistAction {
  /** Adding a new customer to a queue.*/
	ADD,

	/** Deleting an existing customer from a queue.*/
	REMOVE
}
