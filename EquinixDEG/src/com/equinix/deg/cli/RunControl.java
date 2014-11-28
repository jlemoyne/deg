package com.equinix.deg.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.TreeSet;

import com.equinix.deg.dblayer.DataTables;

/**
 * 
 * @author jclaudel
 * 
 * Read Batch run parameters from Java Properties File
 * 
 */
public class RunControl {
	
	public Properties rcParams = null;
	public String rcParamsFileName = "deg.rc";
	
	public void loadParamFile(String rcParamsPath) {
		File rcFile;
		if (rcParamsPath == null)
			rcFile = new File(rcParamsFileName);
		else rcFile = new File(rcParamsPath + "/" + rcParamsFileName);
		rcParams = new Properties();
		
		rcParams.putAll(rcParams);
		
		if (rcFile.exists()) {
			InputStream inStream;
			try {
				inStream = new FileInputStream(rcFile);
				rcParams.load(inStream);
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();		
			} catch (IOException e) {
				System.err.println("*** Could NOT load RC Params File deg.rc");
				e.printStackTrace();
				System.exit(101);
			}
			System.out.println("... Properties File deg.rc read!"  );
			
		} else {
			// default params
			rcParams.put("@hadoop-server-01", "{host:sv2lxgsed01, user:gse, password:welcome1}");
			rcParams.put("@hadoop-server-02", "{host:sv2lxgsed02, user:gse, password:welcome1}");
			rcParams.put("@hadoop-server-03", "{host:sv2lxgsed03, user:gse, password:welcome1}");
			rcParams.put("@hadoop-server-04", "{host:sv2lxgsed04, user:gse, password:welcome1}");
			rcParams.put("@oracle-server", "{dbURL:jdbc:oracle:thin:@10.193.152.163:1521:INTSBL, "
					+ "jdbc-name:oracle.jdbc.driver.OracleDriver, "
					+ "username:etlread, password:etlread}");
			rcParams.put("@hive-server", "{dbURL:jdbc:hive2://10.193.153.186:10000/default, "
					+ "jdbc-name:org.apache.hive.jdbc.HiveDriver, "
					+ "username:gse, password:welcome1}");
			rcParams.put("@sqoop-command", "{ssh-host:sv2lxgsed03.corp.equinix.com, "
					+ "ssh-user:gse, "
					+ "ssh-password:welcome1}");
			
			for (int i = 0; i < DataTables.table_name.length; i++) {
				String tableName = DataTables.table_name[i];
				rcParams.put(String.format("table%05d", i + 1),
						String.format("{name:%s, partition:<partition_col_name>, "
								+ "set:<formula-function>", tableName) );				
			}
			
			
			Properties tmp_rcParams = new Properties() {
			    /**
				 * 
				 */
				private static final long serialVersionUID = 1874274975449102585L;

				@Override
			    public synchronized Enumeration<Object> keys() {
			        return Collections.enumeration(new TreeSet<Object>(super.keySet()));
			    }
			};
			tmp_rcParams.putAll(rcParams);

			

			OutputStream outStream;
			try {
				outStream = new FileOutputStream(rcFile);
				tmp_rcParams.store(outStream, "Initial Template");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("... Template Properties File deg.rc generated!"  );
			
		}
	}
	
	public void parseRcParams() {
		
	}
	
	public static void main(String[] args) {
		RunControl rc = new RunControl();
		rc.loadParamFile("/Users/jclaudel");
	}

}
