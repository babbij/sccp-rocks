package com.goodforgoodbusiness.rocks;

public class RocksManagerTest {
	public static void main(String[] args) throws Exception {
		var manager = new RocksManager("./rocks");
		
		manager.start();
		
		var cfh = manager.getOrCreateColFH(new byte [] { 1, 2, 3, 4, 5 });
	}
}
