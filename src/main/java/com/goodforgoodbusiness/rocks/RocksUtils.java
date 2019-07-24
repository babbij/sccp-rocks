package com.goodforgoodbusiness.rocks;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Random;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksUtils {
	private static final Random RANDOM = new Random();
	
	/**
	 * Find what column families exist in a RocksDB database
	 */
	static List<ColumnFamilyDescriptor> getColumnFamilyDescriptors(String dbPath) throws RocksDBException {
		try (var options = new Options()) {
			options.setCreateIfMissing(false);
			
			return RocksDB
				.listColumnFamilies(options, dbPath)
				.stream()
				.map(ColumnFamilyDescriptor::new)
				.collect(toList())
			;
		}
	}
	
	/**
	 * Create a composite key from some pieces to match, including extra randomness
	 */
	public static byte [] createCompositeKey(byte [] ... bsarr) {
		var len = 16; // extra room for random
		for (var bs : bsarr) len += bs.length;
		
		byte [] key = new byte[len];
		RANDOM.nextBytes(key);
		
		int pos = 0;
		for (byte [] bs : bsarr) {
			System.arraycopy(bs, 0, key, pos, bs.length);
			pos += bs.length;
		}
		
		return key;
	}
	
	/**
	 * Create a composite prefix from some pieces for iterator matching
	 */
	public static byte [] createCompositePrefix(byte [] ... bsarr) {
		var len = 0;
		for (var bs : bsarr) len += bs.length;
		
		byte [] pre = new byte[len];
		
		int pos = 0;
		for (byte [] bs : bsarr) {
			System.arraycopy(bs, 0, pre, pos, bs.length);
			pos += bs.length;
		}
		
		return pre;
	}
	
	/**
	 * Check if one array starts with the contents of another
	 */
	public static boolean startsWith(byte [] bytes, byte [] prefix) {
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
}
