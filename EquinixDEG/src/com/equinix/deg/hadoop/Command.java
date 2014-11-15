package com.equinix.deg.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

public class Command {
	
	public static void methodOne() {
        try {
            UserGroupInformation ugi
                = UserGroupInformation.createRemoteUser("gse");

            ugi.doAs(new PrivilegedExceptionAction<Void>() {

                public Void run() throws Exception {

                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://sv2lxgsed01.corp.equinix.com:8020/user/gse/xxx");
                    conf.set("hadoop.job.ugi", "gse");

                    FileSystem fs = FileSystem.get(conf);

                    fs.createNewFile(new Path("/user/gse/xxx/test"));

                    FileStatus[] status = fs.listStatus(new Path("/user/gse/xxx"));
                    for(int i=0;i<status.length;i++){
                        System.out.println(status[i].getPath());
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
		
	}
	
	public static void methodTwo() throws IOException {
	    Configuration conf = new Configuration();
	    conf.set("fs.default.name","hdfs://10.193.153.186:9000/");
	    FileSystem fileSystem = FileSystem.get(conf);
//	    FileSystem fileSystem = FileSystem.get(new URI("hdfs://10.193.153.186:54310"),conf);
	    if(fileSystem instanceof DistributedFileSystem) {
	      System.out.println("HDFS is the underlying filesystem");
	    }
	    else {
	      System.out.println("Other type of file system "+fileSystem.getClass());
	    }
		
	}
	

	public static void main(String[] args) throws IOException, URISyntaxException {
		methodOne();
	}

}
