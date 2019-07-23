package com.goodforgoodbusiness.rocks;

import java.util.Iterator;

import org.rocksdb.RocksIterator;

import com.goodforgoodbusiness.rocks.PrefixIterator.Row;

/**
 * Iterates over values matching a particular RocksDB prefix.
 * @author ijmad
 */
public class PrefixIterator implements Iterator<Row>, AutoCloseable {
	/**
	 * Check if one array starts with the contents of another
	 */
	private static boolean startsWith(byte [] bytes, byte [] prefix) {
		if (bytes.length >= prefix.length) {
			for (var x = 0; x < prefix.length; x++) {
				if (bytes[x] != prefix[x]) {
					return false;
				}
			}
			
			return true;
		}
		
		return false;
	}
	
	public static class Row {
		public final byte [] key, val;
		
		public Row(byte [] key, byte [] val) {
			this.key = key;
			this.val = val;
		}
	}
	
	private final RocksIterator it;
	private final byte[] prefix;
	
	private Row curRow = null;
	
	public PrefixIterator(RocksIterator it, byte [] prefix) {
		this.it = it;	
		this.prefix = prefix;
		
		if (prefix != null) {
			it.seek(prefix);
		}
		else {
			it.seekToFirst();
		}
		
		updateCurrent();
	}
	
	public PrefixIterator(RocksIterator it) {
		this(it, null);
	}
	
	private void updateCurrent() {
		if (it.isValid() && (prefix == null || startsWith(it.key(), prefix))) {
			curRow = new Row(it.key(), it.value());
		}
		else {
			curRow = null;
		}
	}
	
	@Override
	public boolean hasNext() {
		return curRow != null;
	}

	@Override
	public Row next() {
		var lastVal = curRow;
		
		it.next();
		updateCurrent();
		
		return lastVal;
	}

	@Override
	public void close() {
		it.close();
	}
}
