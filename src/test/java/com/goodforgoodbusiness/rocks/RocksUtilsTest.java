package com.goodforgoodbusiness.rocks;

import java.util.Arrays;

public class RocksUtilsTest {
	public static void main(String[] args) {
		var key = RocksUtils.createCompositeKey(
			new byte [] { 1, 1, 1 },
			new byte [] { 2, 2, 2 },
			new byte [] { 3, 3, 3 }
		);
		
		System.out.println(Arrays.toString(key));
		
		var pre = RocksUtils.createCompositePrefix(
			new byte [] { 1, 1, 1 },
			new byte [] { 2, 2, 2 },
			new byte [] { 3, 3, 3 }
		);
		
		System.out.println(Arrays.toString(pre));
		
		System.out.println(RocksUtils.startsWith(key, pre));
		
		System.out.println(RocksUtils.startsWith(pre, key));
	}
}
