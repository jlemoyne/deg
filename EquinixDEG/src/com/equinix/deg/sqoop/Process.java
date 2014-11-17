package com.equinix.deg.sqoop;

import org.apache.sqoop.Sqoop;

import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;

public class Process {

	public static void runSqoop() {
		final int ret = Sqoop.runTool(new String[] { "import" });
		if (ret != 0) {
		  throw new RuntimeException("Sqoop failed - return code " + Integer.toString(ret));
		}		
	}
	
	public static void sshSqoop() {
	    // Initialize a ConnBean object, parameter list is ip, username, password

	    ConnBean cb = new ConnBean("sv2lxgsed03.corp.equinix.com", "gse","welcome1");

	    // Put the ConnBean instance as parameter for SSHExec static method getInstance(ConnBean) to retrieve a singleton SSHExec instance
	    SSHExec ssh = SSHExec.getInstance(cb);          
	    // Connect to server
	    ssh.connect();
	    CustomTask sampleTask1 = new ExecCommand("echo $SSH_CLIENT"); // Print Your Client IP By which you connected to ssh server on Horton Sandbox
	    try {
			System.out.println(ssh.exec(sampleTask1));
		    CustomTask sampleTask2 = new ExecCommand("sqoop import "
		    		+ "--connect jdbc:oracle:thin:@10.193.152.163:1521:INTSBL "
		    		+ "--username etlread "
		    		+ "--password etlread "
		    		+ "--table SIEBEL.S_ORDER "
		    		+ "--hive-table SIEBEL.S_ORDER "
		    		+ "--hive-import "
		    		+ "--hive-partition-key dt_ordered "
		    		+ "--hive-overwrite -m 1 ");
		    ssh.exec(sampleTask2);
		    ssh.disconnect();   
		} catch (TaskExecFailException e) {
			e.printStackTrace();
		}
		
	}
	
	
	public static void main(String[] args) {
		sshSqoop();
	}

}
