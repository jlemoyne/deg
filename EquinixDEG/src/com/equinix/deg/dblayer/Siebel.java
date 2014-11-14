package com.equinix.deg.dblayer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hive.service.ThriftHive.Processor.execute;
import org.apache.hive.service.cli.Column;



public class Siebel {
//	public String connstr = "etlread/etlread@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(Host=10.193.152.163)(Port=1521))(CONNECT_DATA=(SID=INTSBL)))";
	public String connstr = "jdbc:oracle:thin:@10.193.152.163:1521:INTSBL";
	public String username = "etlread";
	public String password = "etlread";
	public Connection connection = null;
	public int verbose = 1;
	
	public boolean connected() {
		try {
			 
			Class.forName("oracle.jdbc.driver.OracleDriver");
 
		} catch (ClassNotFoundException e) {
 
			System.out.println("Where is Oracle JDBC Driver?");
			e.printStackTrace();
			return false;
 
		}
		
		try {
			 
			connection = DriverManager.getConnection(connstr, username, password);
 
		} catch (SQLException e) {
 
			System.out.println("Connection Failed! Check connection rarameters!");
			e.printStackTrace();
			return false;
 
		}
		

		return true;
	}
	
    public COLNAME_TYPE[] getColNameTypes(String tablename) {
		if (verbose == 1) 
			System.out.println(String.format("--- TABLE: %s ---", tablename));
    String query = String.format("SELECT * FROM %s WHERE ROWNUM < 2", tablename); 
    COLNAME_TYPE[] colnametype = null;    	
    Statement stmt;
	try {
		stmt = connection.createStatement();
        stmt.execute(query);

        ResultSet rs = stmt.getResultSet();

        ResultSetMetaData meta = rs.getMetaData();

        colnametype = new COLNAME_TYPE[meta.getColumnCount()];
        
        
        int n = 0;
        for (int i=1; i<=meta.getColumnCount(); i++) {
        		if (verbose == 1)
        			System.out.println(String.format("# %d --- %s: %s \t%s \t%s \t%s", i ,
        					meta.getColumnLabel(i), 
        					meta.getColumnTypeName(i),
        					meta.getColumnType(i),
        					meta.getPrecision(i),
        					meta.getScale(i)));
            colnametype[n++] = new COLNAME_TYPE(meta.getColumnLabel(i), 
            		meta.getColumnTypeName(i),
            		meta.getPrecision(i),
            		meta.getScale(i));
        }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return colnametype;
	}

    public COLNAME_TYPE[] getAllFields(String tableName) {
        ArrayList<COLNAME_TYPE> allFields = new ArrayList<COLNAME_TYPE>();
        try {
            DatabaseMetaData dbMetaData = connection.getMetaData();
//            int ncol = dbMetaData.getColumn
//            ResultSet columnsResultSet = dbMetaData.getColumns(null, null, tableName, null);
            int p = tableName.indexOf(".");
            if (p == -1) {
            		System.err.println("NO SCHEMA name provided - should pass <schema.tablename>");
            		return null;
            }
            String schema_pattern = tableName.substring(0, p);
            String tablename_pattern = tableName.substring(p + 1);
            
            ResultSet columnsResultSet = dbMetaData.getColumns(null, schema_pattern, tablename_pattern, null);
            ResultSetMetaData rsMeta = columnsResultSet.getMetaData();
            int ncol = rsMeta.getColumnCount();
            while (columnsResultSet.next()) {
            		String colName = columnsResultSet.getString("COLUMN_NAME");
            		String colType = columnsResultSet.getString("TYPE_NAME");
            		int colPrecis = columnsResultSet.getInt("COLUMN_SIZE");
            		int colScale = columnsResultSet.getInt("DECIMAL_DIGITS");
            		allFields.add(new COLNAME_TYPE(colName, colType, colPrecis, colScale));
            		if (verbose == 2)
            			System.out.println(String.format("%s\t\t%s\t\t%s\t\t%s", colName, colType, colPrecis, colScale));
            		
            }
            
            if (verbose == 2) {
	            System.out.println("§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§");
	            for (int i = 1; i <= ncol; i++) {
	            		String colName = rsMeta.getColumnLabel(i); 
	            		String colType = rsMeta.getColumnTypeName(i);
	            		int colPrecis = rsMeta.getPrecision(i);
	            		int colScale = rsMeta.getScale(i);
	            		System.out.println(String.format("%s\t%s\t\t%s\t\t%s", colName, colType, colPrecis, colScale));
	            		allFields.add(
	            				new COLNAME_TYPE(colName, 
	            	            		colType,
	            	            		colPrecis,
	            	            		colScale) ); 
	            }
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        COLNAME_TYPE[] colnametype = new COLNAME_TYPE[allFields.size()];
        colnametype = allFields.toArray(colnametype);
        if (verbose == 1) {
	        System.out.println("?????????????????????????????");
	        System.out.println(" ===========> " + tableName + " <============");
	        for (COLNAME_TYPE colnt: colnametype) {
		    		String colName = colnt.colname;
		    		String colType = colnt.coltype;
		    		int colPrecis = colnt.colprecis;
		    		int colScale = colnt.colscale;
	    			System.out.println(String.format("%s\t\t%s\t\t%s\t\t%s", colName, colType, colPrecis, colScale));        
	        }
        }
        return colnametype;
    }
    
    public String hiveCreateTable(String tableName, COLNAME_TYPE[] colname) {
    		String hql = String.format("CREATE TABLE IF NOT EXISTS %s (\n", tableName);
    		int nk = 0;
    		TreeMap<String, Integer> uncoverted_types = new TreeMap<String, Integer>();
    		for (int i = 0; i < colname.length; i++) {
    			if (colname[i].coltype.startsWith("VARCHAR")) {
    				if ( i == 0)
    					hql += String.format("%s VARCHAR(%d)", 
    						colname[i].colname, colname[i].colprecis);
    				else
    					hql += String.format(",\n%s VARCHAR(%d)", 
        						colname[i].colname, colname[i].colprecis);
    				nk += 1;
    			} else
    			if (colname[i].coltype.startsWith("NUMBER")) {
    				if ( i == 0)
    					hql += String.format("%s DECIMAL(%d, %d)", 
    						colname[i].colname, colname[i].colprecis, colname[i].colscale);
    				else
    					hql += String.format(",\n%s DECIMAL(%d, %d)", 
        						colname[i].colname, colname[i].colprecis, colname[i].colscale);
    				nk += 1;
    			} else
        			if (colname[i].coltype.startsWith("CHAR")) {
        				if ( i == 0)
        					hql += String.format("%s CHAR(%d)", 
        						colname[i].colname, colname[i].colprecis);
        				else
        					hql += String.format(",\n%s CHAR(%d)", 
            						colname[i].colname, colname[i].colprecis);
        				nk += 1;
        			}
        			else
            			if (colname[i].coltype.startsWith("DATE")) {
            				if ( i == 0)
            					hql += String.format("%s DATE", 
            						colname[i].colname);
            				else
            					hql += String.format(",\n%s DATE", 
                						colname[i].colname);
            				nk += 1;
            			}
            		else {
            			uncoverted_types.put(colname[i].coltype, 0);
            		}
    		}
    		hql += ")";
    		hql += "\nCOMMENT 'CONVERTED From Siebel/Oracle Table'";
    		hql += "\nROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY \"\\t\"";
    		hql += "\n\tLINES TERMINATED BY \"\\n\"";
//    		hql += "\nSTORED AS ORC";
    		hql += "\nSTORED AS TEXT";
    		
    		if (verbose == 1) {
    			System.out.println(String.format("~~~ #field read: %d, converted: %d", colname.length, nk));
    		}
    		
    		if (uncoverted_types.size() > 0) {
    			System.err.println("***** NOT ALL COLUMN TYPES WERE CONVERTED! *****");
    			for (String type: uncoverted_types.keySet()) {
    				System.err.println(String.format("[[*** %s ***]]", type));
    			}
   			
    		}
    		
    		try {
				PrintWriter writer = new PrintWriter(String.format("/Users/jclaudel/Data/equinix/create_%s.hql", tableName));
				writer.println(hql);
				writer.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
    		return hql;
    }

    public String hiveCreateTable(String tableName) {
    		COLNAME_TYPE[] colname = getColNameTypes(tableName);
    		return hiveCreateTable(tableName, colname);
    }
    
	
	public ResultSet execSql(String query) {
        ResultSet rs = null;
        Statement stmt;
        try {
                stmt = connection.createStatement();
                stmt.execute(query);
                rs = stmt.getResultSet();
        } catch (SQLException sqlx) {
                System.err.println("ERROR in execSQL ! " + sqlx.getMessage() + "\n" +
                                "$$$$ (" + query + ") " );
        }
        return rs;
	}	

	public void downloadTable(String query, String path) {
        ResultSet rs = null;
        Statement stmt;
        try {
				PrintWriter writer = new PrintWriter(new FileOutputStream(new File(path)), true);
                stmt = connection.createStatement();
                stmt.execute(query);
                rs = stmt.getResultSet();
                ResultSetMetaData rsMeta = rs.getMetaData();
                int ncol = rsMeta.getColumnCount();
                String[] colname = new String[ncol];
                for (int i = 1; i <= ncol; i++) {
                		colname[i-1] = rsMeta.getColumnName(i);
                }
                int nNull = 0;
                int nrow = 0;
                while (rs.next()) {	
                		nrow++;
                		String csv_row = null;
                		for (int i = 0; i < ncol; i++) {
                			String field = rs.getString(colname[i]);
                			if (field == null ) nNull++;
                			if (i == 0)
                				csv_row = field;
                			else csv_row += "\t" + field;
                		}
                		writer.println(csv_row);
                }
                rs.close();
                double sparse = nNull * 100.0 / (double) (nrow * ncol);
                System.out.println(String.format("# null field: %d / %d" , nNull, nrow * ncol));
                System.out.println(String.format("Percent Sparse: %5.2f" , sparse));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
                
        } catch (SQLException sqlx) {
                System.err.println("ERROR in execSQL ! " + sqlx.getMessage() + "\n" +
                                "$$$$ (" + query + ") " );
                sqlx.printStackTrace();
        }
	}	
	
	
    public int rowCount(String tablename) {
    	int count = 0;
    	String query = String.format("SELECT count(*) as count FROM %s", tablename);
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = execSql(query);
            if (rs != null)
            {
                    if (rs.next())
                    {
                        count = rs.getInt(1);
                    }
                    rs.close();
            }
            stmt.close();
        } catch (SQLException ex) {
            Logger.getLogger(Siebel.class.getName()).log(Level.SEVERE, null, ex);
        }        	
        
        return count;
    }
	
    public int rowCount(String tablename, String selector, int selection) {
    	int count = 0;
    	String query = String.format("SELECT count(1) as count FROM %s WHERE `%s` = '%s';", 
    			tablename, selector, selection);
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = execSql(query);
            if (rs != null)
            {
                    if (rs.next())
                    {
                        count = rs.getInt(1);
                    }
                    rs.close();
            }
            stmt.close();
        } catch (SQLException ex) {
            Logger.getLogger(Siebel.class.getName()).log(Level.SEVERE, null, ex);
        }        	
        
        return count;
    }
    
    public Siebel() {
		if (connected())
			System.out.println("Connected to Siebel/INTSBL!");
		else System.out.println("NOT Connected!");    	
    }
	
	public static void main(String[] args) {
		// Test Connection
		Siebel siebel = new Siebel();
		
		int volume = 0;
		for (int i = 0; i < DataTables.table_name.length; i++) {
			String table_name = DataTables.table_name[i];
			int nrow = siebel.rowCount(table_name);
			volume += nrow;
			System.out.println(String.format("# rows for table %s: %d", table_name, nrow));
		}
		
		System.out.println(String.format("Total Row Volume: %d", volume));
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~");
		COLNAME_TYPE[] cols = siebel.getColNameTypes(DataTables.table_name[0]);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~");
		String hql = siebel.hiveCreateTable(DataTables.table_name[0], cols);
		System.out.println(hql);
		
		System.out.println("============================");
		cols = siebel.getAllFields(DataTables.table_name[0]);
		
		System.out.println("============================");
		String outputPath = "/Users/jclaudel/Data/equinix/s_score.csv";
		String sql = "SELECT * FROM SIEBEL.S_ORDER";
//		String sql = "SELECT ORDER_DT FROM SIEBEL.S_ORDER";
		System.out.println("Downloading table SIEBEL.S_ORDER to " + outputPath + " ...");
//		siebel.downloadTable(sql, outputPath);
//		if (siebel.execSql(sql) != null) System.out.println("SQL OK!");
		System.out.println("... done!");
		
	}

}
