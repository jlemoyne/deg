package com.equinix.deg.dblayer;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;



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


    public String hiveCreateTable(String tableName, COLNAME_TYPE[] colname) {
    		String hql = String.format("CREATE TABLE IF NOT EXISTS %s (\n", tableName);
    		int nk = 0;
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
    		}
    		hql += ")";
    		hql += "\nCOMMENT 'CONVERTED From Siebel/Oracle Table'";
    		hql += "\nROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY \"\\t\"";
    		hql += "\n\tLINES TERMINATED BY \"\\n\"";
    		hql += "\nSTORED AS ORC";
    		
    		if (verbose == 1) {
    			System.out.println(String.format("~~~ #field read: %d, converted: %d", colname.length, nk));
    		}
    		try {
				PrintWriter writer = new PrintWriter(String.format("/Users/jclaudel/create_%s.hql", tableName));
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
		COLNAME_TYPE[] cols = siebel.getColNameTypes("SIEBEL.S_ORDER_X");
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~");
		String hql = siebel.hiveCreateTable("SIEBEL.S_ORDER_X", cols);
		System.out.println(hql);
		
	}

}
