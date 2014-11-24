package com.equinix.deg.cli;

import com.equinix.deg.dblayer.DataTables;
import com.equinix.deg.dblayer.Hive2;
import com.equinix.deg.dblayer.PARTITION_TUPLE;
import com.equinix.deg.dblayer.Siebel;
import com.equinix.deg.sqoop.Process;

public class Batch {

	public void test_siebel_to_hive_table_deninition(int tablex) {
		String tableName = DataTables.table_name[tablex];
		Hive2 hive2 = new Hive2();
		Siebel siebel = new Siebel();
		String hql = siebel.hiveCreateTable(tableName, tableName, null);
//		String hql = siebel.hiveCreateExternalTable(tableName, 
//				"/user/gse/SIEBEL.S_ORDER");
		hive2.executeSql(hql);
		System.out.println(hql);
		String loadQuery = String.format("LOAD DATA INPATH '/user/gse/%s' "
				+ "INTO TABLE %s", tableName, tableName);
		hive2.execSql(loadQuery);
		System.out.println(".... done!");
	}
	
	public void addPartitionToHiveTable(String tableName, String partitionCol) {
		String alterQuery = String.format("ALTER TABLE %s ADD IF NOT EXISTS "
				+ "PARTITION (%s='1')", 
				tableName, partitionCol);
		Hive2 hive2 = new Hive2();
		hive2.executeSql(alterQuery);
		hive2.describeTable(tableName);
	}

	public void loadHdfsDataIntoHiveTable(Hive2 hive2, String tableName, PARTITION_TUPLE partition) {
//		String loadQuery = String.format("LOAD DATA INPATH '/user/gse/%s/part-m-00000' "
		String loadQuery = String.format("LOAD DATA INPATH '/user/gse/%s' "
				+ "OVERWRITE INTO TABLE %s PARTITION %s", 
//				+ "INTO TABLE %s PARTITION %s", 
					tableName, 
					tableName, 
					partition.getPartitionTuple());		
		hive2.execSql(loadQuery);		
	}
	
	public void importSiebelTableIntoExternalHiveTable(int tablex) {
		String tableName = DataTables.table_name[tablex];
		// import to HDFS
		Process.	sshSqoop(Process.WITHOUT_HIVE_IMPORT_OPTION, tableName, tableName, tableName);
		
		Hive2 hive2 = new Hive2();
		Siebel siebel = new Siebel();

		String hql_external_no_partitioned = siebel.hiveCreateExternalTable(tableName, 
						String.format("/user/gse/%s", tableName), null);		
		hive2.executeSql(hql_external_no_partitioned);		
		System.out.println(hql_external_no_partitioned);
					
		System.out.println(".... done!");
	}
	
	public void importSiebelTableIntoExternalPartitionedHiveTable(int tablex) {
		String tableName = DataTables.table_name[tablex];
		// import to HDFS
		Process.	sshSqoop(Process.WITHOUT_HIVE_IMPORT_OPTION, tableName, tableName, tableName);

		PARTITION_TUPLE partition = new PARTITION_TUPLE();
		partition.addPartCol("orderdt", "1");
		
		Hive2 hive2 = new Hive2();
		Siebel siebel = new Siebel();
		
		String hql_with_partition = siebel.hiveCreateTable(tableName, tableName, partition);
		hive2.executeSql(hql_with_partition);
		System.out.println(hql_with_partition);

//		String hql_external_partitioned = siebel.hiveCreateExternalTable(tableName, 
//						String.format("/user/gse/%s", tableName), partition);		
//		hive2.executeSql(hql_external_partitioned);		
//		System.out.println(hql_external_partitioned);
		
		loadHdfsDataIntoHiveTable(hive2, tableName, partition);
						
		System.out.println(".... done!");
	}
	
	
	public static void main(String[] args) {
		Batch batch = new Batch();
//		batch.test_siebel_to_hive_table_deninition(0);
//		batch.importSiebelTableIntoExternalHiveTable(0);
		batch.importSiebelTableIntoExternalPartitionedHiveTable(0);
//		batch.addPartitionToHiveTable(DataTables.table_name[0], "orderdt");
	}

}
