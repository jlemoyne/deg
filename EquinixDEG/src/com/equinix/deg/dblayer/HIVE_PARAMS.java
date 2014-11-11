package com.equinix.deg.dblayer;

public class HIVE_PARAMS {

	public String host = "10.193.153.186";
	
	public String user = "gse";
	public String password = "welcome1";
	// for now database is default
	public String dbname = "default";	
//	public String dbname = "siebel";	
	
	public String dbURL = null;
	
	public Hive2 hive = null;
	
	public HIVE_PARAMS() {      
		dbURL = String.format("jdbc:hive2://%s:10000/%s", host, dbname);
//		dbURL = String.format("jdbc:hive2://%s:10000/%s", host);
	}
	  
	public HIVE_PARAMS(
	        String host, 
	        String user, 
	        String password, 
	        String dbname) {
		this.host = host;
		this.user = user;
		this.password = password;
		this.dbname = dbname;
		dbURL = String.format("jdbc:hive://%s:10000/%s", host, dbname);
	}
    

}
