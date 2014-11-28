package com.equinix.deg.cli;

import java.util.HashMap;

/**
 * 
 * @author jclaudel
 *	
 *	After reading and parsing the Run-Control file 
 *	the data is interpreted and assembled here
 */
public class RC_PARAMS {
	public HashMap<String, SERVER_NODE> server_nodes = null;
	public ORACLE_ACCESS oracle_acess = null;
	public HIVE_ACCESS hive_access = null;
	public SQOOP_SSH sqoop_ssh = null;
	HashMap<String, SOURCE_TABLE> src_tables = null;
}
