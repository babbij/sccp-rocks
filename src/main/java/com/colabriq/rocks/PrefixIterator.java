package com.colabriq.rocks;

import static com.colabriq.rocks.RocksUtils.startsWith;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.rocksdb.RocksIterator;

import com.colabriq.rocks.PrefixIterator.Row;

/**
 * Iterates over values matching a particular RocksDB prefix.
 * @author ijmad
 */
public class PrefixIterator implements Iterator<Row> {	
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
	
	public void close() {
		it.close();
	}
	
	/**
	 * Return a stream over the elements of this iterator
	 */
	public Stream<Row> stream() {
		Iterable<Row> iterable = () -> this;
		
		var stream = StreamSupport.stream(iterable.spliterator(), false);
		stream.onClose(() -> close());
		
		return stream;
	}
}
