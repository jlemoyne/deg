package com.equinix.deg.cli;

import com.equinix.deg.dblayer.DataTables;
import com.equinix.deg.dblayer.Hive2;
import com.equinix.deg.dblayer.Siebel;

public class Batch {

	public void test_siebel_to_hive_table_deninition() {
		String tableName = DataTables.table_name[0];
		Hive2 hive2 = new Hive2();
		Siebel siebel = new Siebel();
		String hql = siebel.hiveCreateTable(tableName);
//		String hql = siebel.hiveCreateExternalTable(tableName, 
//				"/user/gse/SIEBEL.S_ORDER");
		hive2.executeSql(hql);
		System.out.println(hql);
		String loadQuery = "LOAD DATA INPATH '/user/gse/SIEBEL.S_ORDER' "
				+ "INTO TABLE SIEBEL.S_ORDER";
		hive2.execSql(loadQuery);
		System.out.println(".... done!");
	}
	
	public static void main(String[] args) {
		Batch batch = new Batch();
		batch.test_siebel_to_hive_table_deninition();
	}

}
