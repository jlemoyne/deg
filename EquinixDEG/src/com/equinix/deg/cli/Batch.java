package com.equinix.deg.cli;

import com.equinix.deg.dblayer.DataTables;
import com.equinix.deg.dblayer.Hive2;
import com.equinix.deg.dblayer.Siebel;

public class Batch {

	public void test_siebel_to_hive_table_deninition() {
		String tableName = DataTables.table_name[10];
		Hive2 hive2 = new Hive2();
		Siebel siebel = new Siebel();
		String hql = siebel.hiveCreateTable(tableName);
		hive2.executeSql(hql);
	}
	
	public static void main(String[] args) {
		Batch batch = new Batch();
		batch.test_siebel_to_hive_table_deninition();
	}

}
