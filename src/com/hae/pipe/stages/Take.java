package com.hae.pipe.stages;

import com.hae.pipe.*;

/**
 *          ┌─FIRST──┐  ┌─1──────┐   
 *  ──TAKE──┼────────┼──┼────────┼──┬───────┬──
 *          └─LAST───┘  ├─number─┤  └─BYTES─┘
 *                      └─*──────┘
 */
public class Take extends Stage {
	public int execute(String args) throws PipeException {
		signalOnError();
		try {
			commit(-2);
			
			boolean first = true;
			int takeCount = 1;
			boolean bytes = false;
			PipeArgs pa = new PipeArgs(args);
			
			String word = scanWord(pa);
			if (Syntax.abbrev("LAST", word, 4)) {
				first = false;
				word = scanWord(pa);
			}
			
			if (Syntax.abbrev("FIRST", word, 4)) {
				first = true;
				word = scanWord(pa);
			}
			
			if (Syntax.isNumberOrStar(word, true)) {
				if (Syntax.isSignedNumber(word))
					takeCount = PipeUtil.makeInt(word);
				else
					takeCount = Integer.MAX_VALUE;
				// can't be negative
				if (takeCount < 0)
					return exitCommand(-287, word);
				word = scanWord(pa);
			}
			
			if (Syntax.abbrev("BYTES", word, 5)) {
				bytes = true;
				word = scanWord(pa);
			}
			
			if (!"".equals(word)) {
				// extra parameters
				return exitCommand(-112, word);
			}

			// make sure that the primary input stream is connected
			if (streamState(INPUT, 0) != 0)
				return exitCommand(-102, "0");
			
			// make sure no other input streams are connected
			int maxStream = maxStream(INPUT);
			for (int i = 1; i <= maxStream; i++) {
				streamState(INPUT, i);
				if (RC >= 0 && RC <= 8)
					return exitCommand(-264, ""+i);
			}
				
			commit(0);

			int taken = 0;
			String spare = "";
			if (first) {
				while (taken < takeCount) {
					String s = peekto();
					if (bytes) {
						// will this string throw us over the edge?
						if (taken + s.length() > takeCount) {
							// yup, so only output the bit that gets us to the limit
							output(s.substring(0, takeCount - taken));
							// and hang on to the rest to be written to the output stream
							spare = s.substring(takeCount - taken);  // hang on to the rest
						}
						else {
							// nope... still under the limit. output the whole record
							output(s);
						}
						taken += s.length();
					}
					else {
						// if we're counting records we just output the 
						// whole record (we couldn't be in this clause if 
						// we have already written takeCount records)
						output(s);
						taken += 1;
					}
					
					// now consume it...
					readto();
					
					// only go again if we haven't hit the limit
					if (taken < takeCount)
						s = peekto();
				}
				
				// now lets send the rest to the secondary output stream
				select(OUTPUT, 1);
				// first, though we need to check to see if it is connected
				if (RC == 0) {
					// is there anything left over if we were processing bytes?
					if (spare.length() != 0)
						output(spare);
					shortStreams();
				}
			}
			else {
				// TAKE LAST
				// LAST records are delayed (primary output). 
				// Non-LAST records are not delayed (secondary output). 

				// Create a FIFO queue to hold the last N input bytes/records
				LastQueue lastQueue = new LastQueue(takeCount, bytes);

				// SECONDARY output for records that aren't part of LAST N
				boolean secondaryConnected = (streamState(OUTPUT, 1) == 0);
				if (secondaryConnected) {
					select(OUTPUT, 1);
				}

				// Read record, push it, write overflow to SECONDARY, till EOFException
				try {
					while (true) {

						String s = peekto();

						// Push record onto the end of queue, and get overflow
						ArrayList<String> overflow = lastQueue.push(s);

						// If secondary output connected, write overflow
						if (secondaryConnected  &&  ! overflow.isEmpty()) {
							for (String ovrec : overflow) {
								output(ovrec);
							}
						}

						readto();
					}
				}
				catch (EOFException e) {

					// BYTES processing only:
					// Retrieve the "held" overflow record ('Ra').
					// It is the last record on SECONDARY output.
					if (bytes  &&  secondaryConnected) {
						String secrec = lastQueue.getHeldOverflow();
						if (secrec != null)
							output(secrec);
					}
					// Finished with SECONDARY output

					// Send the queued last N records/bytes to PRIMARY output.
					select(OUTPUT, 0);
					ListIterator<String> it = lastQueue.getIterator();
					while (it.hasNext()) {
						output(it.next());
					}
				}
			}
		}
		catch(EOFException e) {
		}
		return 0;
	}

	/**
	 * A FIFO queue with MAX size of N records/bytes.
	 * <p>
	 * "push" records onto the END of the queue. When a push causes the  
	 * queue to exceed MAX size, entry(s) are "popped" off the FRONT of the
	 * queue until it's back to MAX. The "popped" entries are returned to 
	 * caller as overflow records from the push.
	 * <p>
	 * After all records are pushed onto the queue (at EOF), those that remain
	 * in the queue are the desired last N records/bytes, in original order.
	 * <p>
	 * Note for BYTES processing:
	 * Assume Z bytes in input stream. User wants last N bytes.
	 * The input stream is divided into two pieces:
	 *    the first Z-N bytes
	 *    the last  N bytes
	 * The point where Z-N bytes is reached will probably be in the middle of 
	 * some record 'R'.
	 * <p>
	 * All records before 'R' will go to the SECONDARY output unmodified.
	 * All records after 'R' will go to the PRIMARY output unmodified.
	 * <p>
	 * Record 'R' itself is split in two: 'Ra' and 'Rb'.
	 * Ra is the final overflow record (final SECONDARY output rec) 
	 * Rb is the first PRIMARY output rec, it's stored on the front of the queue.
	 * <p>
	 * The correct record 'R' isn't determined until all input records have 
	 * been pushed. So we are constantly re-choosing this record 'R' as each 
	 * push causes an overflow. At EOF, the last one split is the correct record 'R'.
	 * <p>
	 * THE CALLING CODE IS RESPONSIBLE for retrieving that held record 'Ra' 
	 * at the appropriate time (at EOF). Use provided method to retrieve it.
	 */
	public class LastQueue {

		// inner class member variables start with "_"
		private int _maxQueueSize;
		private int _currentQueueSize; 
		private boolean _bytes;  // true = BYTES, false = RECORDS
		private ArrayList<String> _overflow;
		private String _holdRa;

		private LinkedList<String> _FIFOQueue;  // the real queue

		/**
		 * LastQueue object with size N, where N is records or bytes.
		 *
		 * @param size  int Number of records or bytes to queue
		 * @param bytes boolean TRUE = bytes, FALSE = records
		 */
		public LastQueue(int size, boolean bytes) {
			_maxQueueSize = size;
			_currentQueueSize = 0;
			_bytes = bytes;
			_overflow = new ArrayList<String>();
			_holdRa = new String(""); // empty string indicates no pending overflow

			_FIFOQueue = new LinkedList<String>();
		}

		/**
		 * Push a record onto end of queue. If the resulting queue size 
		 * (in bytes or records) exceeds the MAX, pop enough records off 
		 * the front to get size back to MAX. Return popped record(s) to caller.
		 *
		 * @param s  String (input record) to push
		 * @return ArrayList<String> - array of records that push caused
		 *                             to overflow
		 */
		public ArrayList<String> push(String s) {

			// initialize overflow array returned to caller
			_overflow.clear();

			if (_bytes)
				return pushBytes(s);
			else
				return pushRecord(s);
		}

		private ArrayList<String> pushRecord(String s) {
			_FIFOQueue.addLast(s);

			if (_FIFOQueue.size() <= _maxQueueSize)
				return _overflow;

			// Queue is one record too big, pop oldest, return it to caller
			_overflow.clear();
			_overflow.add(_FIFOQueue.pollFirst());
			return _overflow;
		}

		private ArrayList<String> pushBytes(String s) {

			// Push this record onto end of queue, adjust queue size
			_FIFOQueue.addLast(s);
			_currentQueueSize += s.length();

			// If queue <= MAX size, we're done, return empty overflow
			if (_currentQueueSize <= _maxQueueSize)
				return _overflow;

			// Queue is too big

			// Remove (pop) entries from queue until back to exactly MAX bytes.
			// Popped records are overflow, returned to caller.
			while (_currentQueueSize > _maxQueueSize) {
				// Remove ONE entry from front of queue
				// If it's something to return to caller, add to overflow
				String ovrec = shortenBytesQueue();
				if (ovrec != null)
					_overflow.add(ovrec);
			}

			// Queue is now at MAX bytes, return any overflow records
			return _overflow;
		}

		/**
		 * Shorten the BYTES queue by one entry. If that caused a complete
		 * record to be taken off queue, return it as overflow.
		 * 
		 * @return String An overflow record, or NULL if no overflow yet.
		 */
		private String shortenBytesQueue() {

			// Pop one entry off front of queue, adjust queue size
			String popped = _FIFOQueue.pollFirst();
			_currentQueueSize -= popped.length();

			// If a pending record ('Ra') exists, then the popped entry
			// is 'Rb'. Reconstitute the orignial record.
			if (_holdRa.length() != 0) {
				// R <= Ra + Rb
				popped = _holdRa + popped;

				// Nothing pending anymore
				_holdRa = "";
			}

			// The queue is now shortened by removing the first entry.

			// If queue still too big, return the overflow rec to caller.
			// They will loop for another shot.
			if (_currentQueueSize > _maxQueueSize) 
				return popped;

			// If queue is just right, return the overflow rec to caller.
			// They will terminate their loop, push complete.
			if (_currentQueueSize == _maxQueueSize) 
				return popped;

			// Queue is now too small. Put some of popped record back on queue.

			// Split the record at appropriate spot.
			int putBackSize = _maxQueueSize - _currentQueueSize;
			int splitAt = popped.length() - putBackSize;

			// First part 'Ra' goes into hold area as pending overflow 
			_holdRa = popped.substring(0, splitAt); // (inclusive, exclusive)

			// Last part 'Rb' goes back on front of queue, adjust queue size
			String Rb = popped.substring(splitAt);
			_FIFOQueue.addFirst(Rb);
			_currentQueueSize += Rb.length();

			return null; // no returnable overflow at this time
		}

		/**
		 * Return the current held portion (Ra) of the split record, or 
		 * NULL if no split record was needed to maintain QUEUE size.
		 *
		 * @return String  The first part of a split record
		 */
		public String getHeldOverflow() {
			return ("".equals(_holdRa)) ? null : _holdRa;
		}

		public ListIterator<String> getIterator() {
			return _FIFOQueue.listIterator();
		}
	}
}
