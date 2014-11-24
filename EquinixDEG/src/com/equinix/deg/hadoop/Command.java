package com.equinix.deg.hadoop;

import java.io.IOException;
import java.net.URISyntaxException;
import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.Result;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;

public class Command {
	
	public static void sshx(String cmd) {
	    // Initialize a ConnBean object, parameter list is ip, username, password
	    ConnBean cb = new ConnBean("sv2lxgsed03.corp.equinix.com", "gse","welcome1");

	    // Put the ConnBean instance as parameter for SSHExec static method getInstance(ConnBean) to retrieve a singleton SSHExec instance
	    SSHExec ssh = SSHExec.getInstance(cb);          
	    // Connect to server
	    ssh.connect();
	    CustomTask sampleTask1 = new ExecCommand(cmd); 
	    try {
			System.out.println(ssh.exec(sampleTask1));			
		    Result res = ssh.exec(sampleTask1);
		    ssh.disconnect();   
		} catch (TaskExecFailException e) {
			e.printStackTrace();
		}
		
	}

	public static void main(String[] args) throws IOException, URISyntaxException {
//		hdfsx("ls");
		sshx("hdfs dfs -ls /user/gse");
	}

}
