package com.equinix.deg.sqoop;

import org.apache.hadoop.hive.ql.parse.HiveParser.withAdminOption_return;
import org.apache.sqoop.Sqoop;

import com.equinix.deg.dblayer.HIVE_PARAMS;

import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;

public class Process {
	
	public static final int HIVE_IMPORT_OPTION = 1;
	public static final int WITHOUT_HIVE_IMPORT_OPTION = 2;

	public static void runSqoop() {
		final int ret = Sqoop.runTool(new String[] { "import" });
		if (ret != 0) {
		  throw new RuntimeException("Sqoop failed - return code " + Integer.toString(ret));
		}		
	}
	
	public static void sshSqoop(int hive_import_option, String siebelTableName, String hiveTableName) {
		
		String sqoop_hive_import = "sqoop import "
	    		+ "--connect jdbc:oracle:thin:@10.193.152.163:1521:INTSBL "
	    		+ "--username etlread "
	    		+ "--password etlread "
	    		+ String.format("--table %s ", siebelTableName)
	    		+ String.format("--hive-table %s ", hiveTableName)
	    		+ "--fields-terminated-by '\t' "
	    		+ "--hive-import "
	    		+ "--split-by ROW_ID"
	    		+ "--hive-partition-key dt_ordered "
	    		+ "--hive-overwrite "
	    		+ "--null-string '~' "
	    		+ "--null-non-string '~' "
	    		+ "-m 1 ";
		
		String sqoop_import = "sqoop import "
	    		+ "--connect jdbc:oracle:thin:@10.193.152.163:1521:INTSBL "
	    		+ "--username etlread "
	    		+ "--password etlread "
	    		+ String.format("--table %s ", siebelTableName)
	    		+ "--target-dir SIEBEL.S_ORDER2 "
	    		+ String.format("--hive-table %s ", hiveTableName)
	    		+ "--fields-terminated-by '\t' "
	    		+ "--split-by ROW_ID"
	    		+ "--hive-overwrite "
	    		+ "--null-string '~' "
	    		+ "--null-non-string '~' "
	    		+ "-m 1 ";
		
	    // Initialize a ConnBean object, parameter list is ip, username, password
	    ConnBean cb = new ConnBean("sv2lxgsed03.corp.equinix.com", "gse","welcome1");

	    // Put the ConnBean instance as parameter for SSHExec static method getInstance(ConnBean) to retrieve a singleton SSHExec instance
	    SSHExec ssh = SSHExec.getInstance(cb);          
	    // Connect to server
	    ssh.connect();
	    CustomTask sampleTask1 = new ExecCommand("echo $SSH_CLIENT"); // Print Your Client IP By which you connected to ssh server on Horton Sandbox
	    try {
			System.out.println(ssh.exec(sampleTask1));
			CustomTask sampleTask2;
			switch (hive_import_option) {
				case HIVE_IMPORT_OPTION:
					sampleTask2 = new ExecCommand(sqoop_hive_import);
				case WITHOUT_HIVE_IMPORT_OPTION:
					sampleTask2 = new ExecCommand(sqoop_import);
				default:	
					sampleTask2 = new ExecCommand(sqoop_import);
			}
			
		    ssh.exec(sampleTask2);
		    ssh.disconnect();   
		} catch (TaskExecFailException e) {
			e.printStackTrace();
		}
		
	}
	
	
	public static void main(String[] args) {
//		sshSqoop(WITHOUT_HIVE_IMPORT_OPTION, "SIEBEL.S_ORDER", "SIEBEL.OREDER");
		sshSqoop(WITHOUT_HIVE_IMPORT_OPTION, "SIEBEL.S_ORDER", "SIEBEL.OREDER");
	}

}
