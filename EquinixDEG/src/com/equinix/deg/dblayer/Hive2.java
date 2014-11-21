package com.equinix.deg.dblayer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class Hive2 {
	
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public Connection connect = null;
    
    public void showTables() {
        String sql = "show tables in SIEBEL";
        try {
	        Statement stmt = connect.createStatement();
	        ResultSet res = stmt.executeQuery(sql);
	        while (res.next()) {
	          System.out.println(res.getString(1));
	        }
        } catch (SQLException e) {
 
			System.out.println("Connection Failed! Check connection rarameters!");
			e.printStackTrace();
		}    	
    }
    
    public void describeTable(String tableName) {
        String sql = "describe " + tableName;
        try {
	        Statement stmt = connect.createStatement();
	        ResultSet res = stmt.executeQuery(sql);
	        while (res.next()) {
	          System.out.println(String.format("%s: %s", res.getString(1), res.getString(2)));
	        }
        } catch (SQLException e) {
 
			System.out.println("Connection Failed! Check connection rarameters!");
			e.printStackTrace();
		}    	
    	
    }
	
    public boolean connected() {
        try {
      	  Class.forName(driverName);
      	  HIVE_PARAMS params = new HIVE_PARAMS();
            connect = DriverManager.getConnection(params.dbURL, params.user, params.password);          
        } catch (Exception e) {
            e.printStackTrace();
            return false;
          }
        
        return true;
        
    }
      
	public ResultSet execSql(String query) {
        ResultSet rs = null;
        Statement stmt;
        try {
                stmt = connect.createStatement();
                stmt.execute(query);
                rs = stmt.getResultSet();
        } catch (SQLException sqlx) {
                System.err.println("ERROR in execSQL ! " + sqlx.getMessage() + "\n" +
                                "$$$$ (" + query + ") " );
        }
        return rs;
	}
	
	public boolean executeSql(String query) {
        boolean ok = false;
        Statement stmt;
        try {
                stmt = connect.createStatement();
                ok = stmt.execute(query);
        } catch (SQLException sqlx) {
            System.err.println("ERROR in execSQL ! " + sqlx.getMessage() + "\n");
            System.err.println("$$$$ { " + query + " } " );
        }
        return ok;
	}
    
	public Hive2() {
		if (connected())
			System.out.println("Connected to Hive");
		else {
			System.out.println("NOT Connected to Hive");
			System.exit(101);
		}		
	}
        
	public static void main(String[] args) {
		// For testing
		Hive2 hive2 = new Hive2();
		
//		hive2.showTables();
//		System.out.println("~~~~~~~~~~~~~~~~~~~");
//		hive2.describeTable("SIEBEL.S_ORDER");
//		System.out.println("~~~~~~~~~~~~~~~~~~~");
//		hive2.describeTable("accounts_test");
		
		
	}

}
