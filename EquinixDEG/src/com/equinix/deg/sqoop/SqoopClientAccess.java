package com.equinix.deg.sqoop;

import java.util.List;
import java.util.ResourceBundle;

import org.apache.sqoop.client.*;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

public class SqoopClientAccess {

	static public String url = "http://10.193.153.188:12000/sqoop/";

	static void describe(List<MForm> forms, ResourceBundle resource) {
		for (MForm mf : forms) {
			System.out.println(resource.getString(mf.getLabelKey())+":");
			List<MInput<?>> mis = mf.getInputs();
			for (MInput mi : mis) {
				System.out.println(resource.getString(mi.getLabelKey()) + " : " + mi.getValue());
			}
			System.out.println();
		}
	}

	private static void printMessage(List<MForm> formList) {
		for(MForm form : formList) {
			List<MInput<?>> inputlist = form.getInputs();
		    if (form.getValidationMessage() != null) {
		      System.out.println("Form message: " + form.getValidationMessage());
		    }
		    for (MInput minput : inputlist) {
		    		if (minput.getValidationStatus() == Status.ACCEPTABLE) {
		    			System.out.println("Warning:" + minput.getValidationMessage());
		    		} else if (minput.getValidationStatus() == Status.UNACCEPTABLE) {
		    			System.out.println("Error:" + minput.getValidationMessage());
		    		}
		    }
		 }
	}	
	
	public static void exportJob(String jobName, long xid, String schemaName, String tableName, String sqlQuery) {
		SqoopClient client = new SqoopClient(url);
		MJob newjob = client.newJob(xid, org.apache.sqoop.model.MJob.Type.EXPORT);
		MJobForms connectorForm = newjob.getConnectorPart();
		MJobForms frameworkForm = newjob.getFrameworkPart();

		newjob.setName(jobName);
		//Database configuration
		connectorForm.getStringInput("table.schemaName").setValue(schemaName);
		//Input either table name or sql
		if (tableName != null)
			connectorForm.getStringInput("table.tableName").setValue(tableName);
		else connectorForm.getStringInput("table.sql").setValue(sqlQuery);
		connectorForm.getStringInput("table.columns").setValue("ROW_ID,ORDER_DT");
//		connectorForm.getStringInput("table.columns").setValue("*");

		//Input configurations
		frameworkForm.getStringInput("input.inputDirectory").setValue("/input");

		//Job resources
		frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
		frameworkForm.getIntegerInput("throttling.loaders").setValue(1);

		Status status = client.createJob(newjob);
		if(status.canProceed()) {
		  System.out.println("New Job ID: "+ newjob.getPersistenceId());
		} else {
		  System.out.println("Check for status and forms error ");
		}

		//Print errors or warnings
		printMessage(newjob.getConnectorPart().getForms());
		printMessage(newjob.getFrameworkPart().getForms());
	
	}
	
	public static long createImportJob(	SqoopClient client, String jobName, 
										String schemaName, String tableName, String sqlQuery,
										String partitionCol) {
		long jobid = -1;
		//Creating dummy job object
		MJob newjob = client.newJob(1, org.apache.sqoop.model.MJob.Type.IMPORT);
		MJobForms connectorForm = newjob.getConnectorPart();
		MJobForms frameworkForm = newjob.getFrameworkPart();

		newjob.setName(jobName);
		//Database configuration
		connectorForm.getStringInput("table.schemaName").setValue(schemaName);
		//Input either table name or sql
		if (tableName != null)
			connectorForm.getStringInput("table.tableName").setValue(tableName);
		else
			connectorForm.getStringInput("table.sql").setValue(sqlQuery);
		
//		connectorForm.getStringInput("table.columns").setValue("id,name");
		connectorForm.getStringInput("table.partitionColumn").setValue(partitionCol);
		//Set boundary value only if required
		//connectorForm.getStringInput("table.boundaryQuery").setValue("");

		//Output configurations
		frameworkForm.getEnumInput("output.storageType").setValue("HDFS");
		frameworkForm.getEnumInput("output.outputFormat").setValue("TEXT_FILE");//Other option: SEQUENCE_FILE
		frameworkForm.getStringInput("output.outputDirectory").setValue(String.format("/user/gse/%s.%s", schemaName, tableName));

		//Job resources
		frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
		frameworkForm.getIntegerInput("throttling.loaders").setValue(1);

		Status status = client.createJob(newjob);
		if(status.canProceed()) {
			jobid = newjob.getPersistenceId();
			System.out.println("New Job ID: "+ jobid);
		} else {
		 System.out.println("Check for status and forms error ");
		}

		//Print errors or warnings
		printMessage(newjob.getConnectorPart().getForms());
		printMessage(newjob.getFrameworkPart().getForms());
		
		return jobid;
		
	}
	
	public static void createExportJob(SqoopClient client, String jobName, String schemaName, String tableName, String sqlQuery) {
		MJob newjob = client.newJob(1, org.apache.sqoop.model.MJob.Type.EXPORT);
		MJobForms connectorForm = newjob.getConnectorPart();
		MJobForms frameworkForm = newjob.getFrameworkPart();

		newjob.setName(jobName);
		//Database configuration
		connectorForm.getStringInput("table.schemaName").setValue(schemaName);
		//Input either table name or sql
		if (tableName != null)
			connectorForm.getStringInput("table.tableName").setValue(tableName);
		else connectorForm.getStringInput("table.sql").setValue(sqlQuery);
		connectorForm.getStringInput("table.columns").setValue("ROW_ID,ORDER_DT");
//		connectorForm.getStringInput("table.columns").setValue("*");

		//Input configurations
		frameworkForm.getStringInput("input.inputDirectory").setValue("/input");

		//Job resources
		frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
		frameworkForm.getIntegerInput("throttling.loaders").setValue(1);

		Status status = client.createJob(newjob);
		if(status.canProceed()) {
		  System.out.println("New Job ID: "+ newjob.getPersistenceId());
		} else {
		  System.out.println("Check for status and forms error ");
		}

		//Print errors or warnings
		printMessage(newjob.getConnectorPart().getForms());
		printMessage(newjob.getFrameworkPart().getForms());
	
	}
	
	public static void submitJob(SqoopClient client, long jobid) {
		MSubmission submission = client.startSubmission(jobid);
		System.out.println("Status : " + submission.getStatus());
		if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
		  System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		}
		System.out.println("Hadoop job id :" + submission.getExternalId());
		System.out.println("Job link : " + submission.getExternalLink());
		Counters counters = submission.getCounters();
		if(counters != null) {
		  System.out.println("Counters:");
		  for(CounterGroup group : counters) {
		    System.out.print("\t");
		    System.out.println(group.getName());
		    for(Counter counter : group) {
		      System.out.print("\t\t");
		      System.out.print(counter.getName());
		      System.out.print(": ");
		      System.out.println(counter.getValue());
		    }
		  }
		}
		if(submission.getExceptionInfo() != null) {
		  System.out.println("Exception info : " +submission.getExceptionInfo());
		}
		
	}
	
	public static void checkJobStatus(SqoopClient client, long jobid) {
		//Check job status
		MSubmission submission = client.getSubmissionStatus(jobid);
		if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
		  System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		}
		
	}
	
	public static void main(String[] args) {
		SqoopClient client = new SqoopClient(url);	
		MConnection conn1 = client.newConnection(1);
		//Get connection and framework forms. Set name for connection
		MConnectionForms conForms = conn1.getConnectorPart();
		MConnectionForms frameworkForms = conn1.getFrameworkPart();
		conn1.setName("Alpha");		
		
		//Set connection forms values
		conForms.getStringInput("connection.connectionString").setValue("jdbc:oracle:thin:@10.193.152.163:1521:INTSBL");
		conForms.getStringInput("connection.jdbcDriver").setValue("oracle.jdbc.driver.OracleDriver");
		conForms.getStringInput("connection.username").setValue("etlread");
		conForms.getStringInput("connection.password").setValue("etlread");
		
		frameworkForms.getIntegerInput("security.maxConnections").setValue(0);
		
		Status status  = client.createConnection(conn1);
		if(status.canProceed()) {
			System.out.println("Created. New Connection ID : " + conn1.getPersistenceId());
		} else {
			System.out.println("Check for status and forms error ");
		}	
		
		
		printMessage(conn1.getConnectorPart().getForms());
		printMessage(conn1.getFrameworkPart().getForms());

		List<MConnection> mconnList = client.getConnections();
		for (MConnection mconn : mconnList) {
			Long perid = mconn.getPersistenceId();
			System.out.println(String.format("==> Connector ID: %d   %d", mconn.getConnectorId(), perid));
		}

		describe(client.getConnection(1).getConnectorPart().getForms(), client.getResourceBundle(1));
		describe(client.getConnection(1).getFrameworkPart().getForms(), client.getFrameworkResourceBundle());
		
//		client.deleteConnection(1);
		
//		exportJob("ex_test" + conn1.getPersistenceId(), conn1.getPersistenceId(), "SIEBEL", "S_ORDER", null);
//		createExportJob(client, "ex_test" + conn1.getPersistenceId(), "SIEBEL", "S_ORDER", null);
//		client.stopSubmission(2);
		
		long jobid = createImportJob(client, "eximp" + conn1.getPersistenceId(), 
				"SIEBEL", "S_ORDER", null, "ORDER_DT");
		submitJob(client, jobid);
		checkJobStatus(client, jobid);
		
	}

}
