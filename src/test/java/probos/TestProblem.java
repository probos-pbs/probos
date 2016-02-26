package probos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.kitten.client.YarnClientParameters;
import com.cloudera.kitten.client.YarnClientService;
import com.cloudera.kitten.client.params.lua.LuaYarnClientParameters;
import com.cloudera.kitten.client.service.YarnClientFactory;
import com.cloudera.kitten.client.service.YarnClientServiceImpl;
import com.google.common.collect.ImmutableMap;

public class TestProblem {
	
	MiniYARNCluster miniCluster;
	YarnConfiguration yConf;
	static final String HERE = new File(".").getAbsolutePath().toString();
	
	@Before public void setupCluster() throws Exception {
		String name = "mycluster";
		int noOfNodeManagers = 1;
		int numLocalDirs = 1;
		int numLogDirs = 1;
		YarnConfiguration conf = new YarnConfiguration();
		conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
		conf.setClass(YarnConfiguration.RM_SCHEDULER,
		              FifoScheduler.class, ResourceScheduler.class);
		miniCluster = new MiniYARNCluster(
				name, noOfNodeManagers, 
				numLocalDirs, numLogDirs);
		miniCluster.init(conf);
		miniCluster.start();

		//once the cluster is created, you can get its configuration
		//with the binding details to the cluster added from the minicluster
		yConf = new YarnConfiguration(miniCluster.getConfig());
		
	}
	
	@After public void teardownCluster() throws Exception {
		miniCluster.close();
	}
	
	public static String getURL(String url) throws Exception
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(new URL(url).openStream()));
		StringBuilder rtr = new StringBuilder();
		String line;
		while((line = br.readLine()) != null)
		{
			rtr.append(line);
		}
		return rtr.toString();
	}
	
	@Test public void testSmall()  throws Exception {
		dotest(2, jobDefinitionSmall);
	}
	
	@Test public void testLarge()  throws Exception {
		dotest(5, jobDefinition);
	}
	
	void dotest(int N, String defn) throws Exception {
		if (System.getenv("HADOOP_HOME") == null && System.getenv("HADOOP_COMMON_HOME") == null)
			fail("HADOOP_HOME must be set");
		if (System.getenv("JAVA_HOME") == null )
			fail("JAVA_HOME must be set");
		File luaFile = File.createTempFile("kittenFile", ".lua");
		FileWriter w = new FileWriter(luaFile);
		w.write(defn);
		w.close();
		Map<String, Object> extraLuaValues = ImmutableMap.<String, Object>of();
		Map<String, String> extraLocalResources = ImmutableMap.<String, String>of();
		
		
		YarnClientParameters params = new LuaYarnClientParameters(
				luaFile.toString(), "probos", yConf, 
				extraLuaValues, extraLocalResources);
		YarnClientService service = new YarnClientServiceImpl(params);
		service.startAndWait();
		while(! service.isApplicationFinished())
		{
			Thread.sleep(1000);
		}
		assertEquals(FinalApplicationStatus.SUCCEEDED, service.getFinalReport().getFinalApplicationStatus());
		
		ApplicationAttemptId aaid = service.getFinalReport().getCurrentApplicationAttemptId();
		YarnClient yc = new YarnClientFactory(this.yConf).connect();
		List<ContainerReport> lcr = yc.getContainers(aaid);
		for (ContainerReport cr : lcr)
		{	
			String stdErrURL = "http:" + cr.getLogUrl() + "/stderr?start=0";
			System.err.println(cr.getContainerId().toString() + " " + stdErrURL);
			String stderr = getURL(stdErrURL);
			System.err.println(stderr);
			assertFalse("Container" + cr.getContainerId().toString() + " " + stderr, stderr.contains("ArrayIndexOutOfBoundsException"));
		}
		
		//service.getFinalReport().get
		
		System.err.println();
		Thread.sleep(60000);
		for (int id=1;id<=N;id++)
			for(String type : new String[]{"o", "e"})
			{
				String file = HERE + "/testHostname."+type+"1-"+ id;
				assertTrue("File not found " + file, new File(file).exists());
			}
	}
	
	static final String jobDefinitionSmall = "MASTER_JAR_LOCATION = \"" + HERE + "/./lib/kitten-master-0.3.0-jar-with-dependencies.jar\""+
			"base_resources = {"+
			" [\"master.jar\"] = { file = MASTER_JAR_LOCATION }"+
			"}"+
			"base_env = cat {"+
			"\tCLASSPATH = table.concat({\"${CLASSPATH}\", \"./master.jar\"}, \":\"),"+
			"}"+
			"job_env = cat {"+
			" CLASSPATH = table.concat({\"${CLASSPATH}\", \"./master.jar\"}, \":\"),"+
			
			"}"+
			"probos = yarn {"+
			" name = \"ktestHostname\","+
			" timeout = -1,"+
			" queue = \"queue\","+
			" memory = 512,"+
			" master = {"+
			"  env = base_env {},"+
			"  resources = base_resources,"+
			"  command = {"+
			"   base = \"export 1>> <LOG_DIR>/stdout ; echo ${JAVA_HOME}/bin/java -cp \\\"${CLASSPATH}:`HADOOP_CONF_DIR= ${HADOOP_HOME:$HADOOP_COMMON_HOME}/bin/hadoop classpath`\\\" -Xms64m -Xmx128m  com.cloudera.kitten.appmaster.ApplicationMaster 1>> <LOG_DIR>/stdout ; ${JAVA_HOME}/bin/java -cp \\\"${CLASSPATH}:`HADOOP_CONF_DIR= ${HADOOP_HOME:$HADOOP_COMMON_HOME}/bin/hadoop classpath`}\\\" -Xms64m -Xmx128m  com.cloudera.kitten.appmaster.ApplicationMaster\" ,"+
			"   args = { \"-conf job.xml\", \"1>> <LOG_DIR>/stdout\", \"2>> <LOG_DIR>/stderr\" },"+
			"  }"+
			" },"+
			" containers = {"+
			"  {"+
			"   timeout = 3600000,"+
			"   cores = 1,"+
			"   memory = 512,"+
			"   env = job_env {"+
			"    CORES = \"1\","+
			"    VMEM = \"512\","+
			"    ARRAYID = \"1\","+
			"   TMP = \"$(pwd)/tmp\","+
			"   },"+
			"   command = \"mkdir $(pwd)/tmp; echo ${ARRAYID} 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr ;  cp -v <LOG_DIR>/stdout " + HERE + "/testHostname.o1-1 >> <LOG_DIR>/stderr 2>&1 ;  cp -v <LOG_DIR>/stderr "+HERE+"/testHostname.e1-1 >> <LOG_DIR>/stderr 2>&1  \""+
			"  },"+
			"  {"+
			"   timeout = 3600000,"+
			"   cores = 1,"+
			"   memory = 512,"+
			"   env = job_env {"+
			"    CORES = \"1\","+
			"    VMEM = \"512\","+
			"    ARRAYID = \"2\","+
			"   TMP = \"$(pwd)/tmp\","+
			"   },"+
			"   command = \"mkdir $(pwd)/tmp; echo ${ARRAYID} 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr ;  cp -v <LOG_DIR>/stdout " + HERE + "/testHostname.o1-2 >> <LOG_DIR>/stderr 2>&1 ;  cp -v <LOG_DIR>/stderr "+HERE+"/testHostname.e1-2 >> <LOG_DIR>/stderr 2>&1 \""+
			"  },"+
			" }"+
			"}";
	
	static final String jobDefinition = "MASTER_JAR_LOCATION = \"" + HERE + "/./lib/kitten-master-0.3.0-jar-with-dependencies.jar\""+
			"base_resources = {"+
			" [\"master.jar\"] = { file = MASTER_JAR_LOCATION }"+
			"}"+
			"base_env = cat {"+
			"\tCLASSPATH = table.concat({\"${CLASSPATH}\", \"./master.jar\"}, \":\"),"+
			"}"+
			"job_env = cat {"+
			" CLASSPATH = table.concat({\"${CLASSPATH}\", \"./master.jar\"}, \":\"),"+	
			"}"+
			"probos = yarn {"+
			" name = \"ktestHostname\","+
			" timeout = -1,"+
			" queue = \"queue\","+
			" memory = 512,"+
			" master = {"+
			"  env = base_env {},"+
			"  resources = base_resources,"+
			"  command = {"+
			"   base = \"export 1>> <LOG_DIR>/stdout ; echo ${JAVA_HOME}/bin/java -agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=y -cp \\\"${CLASSPATH}:`HADOOP_CONF_DIR= ${HADOOP_HOME:$HADOOP_COMMON_HOME}/bin/hadoop classpath`\\\" -Xms64m -Xmx128m  com.cloudera.kitten.appmaster.ApplicationMaster 1>> <LOG_DIR>/stdout ; ${JAVA_HOME}/bin/java -cp \\\"${CLASSPATH}:`HADOOP_CONF_DIR= ${HADOOP_HOME:$HADOOP_COMMON_HOME}/bin/hadoop classpath`}\\\" -Xms64m -Xmx128m  com.cloudera.kitten.appmaster.ApplicationMaster\" ,"+
			"   args = { \"-conf job.xml\", \"1>> <LOG_DIR>/stdout\", \"2>> <LOG_DIR>/stderr\" },"+
			"  }"+
			" },"+
			" containers = {"+
			"  {"+
			"   timeout = 3600000,"+
			"   cores = 1,"+
			"   memory = 512,"+
			"   env = job_env {"+
			"    CORES = \"1\","+
			"    VMEM = \"512\","+
			"    ARRAYID = \"1\","+
			"   TMP = \"$(pwd)/tmp\","+
			"   },"+
			"   command = \"mkdir $(pwd)/tmp; echo ${ARRAYID} 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr ;  cp -v <LOG_DIR>/stdout " + HERE + "/testHostname.o1-1 >> <LOG_DIR>/stderr 2>&1 ;  cp -v <LOG_DIR>/stderr "+HERE+"/testHostname.e1-1 >> <LOG_DIR>/stderr 2>&1  \""+
			"  },"+
			"  {"+
			"   timeout = 3600000,"+
			"   cores = 1,"+
			"   memory = 512,"+
			"   env = job_env {"+
			"    CORES = \"1\","+
			"    VMEM = \"512\","+
			"    ARRAYID = \"2\","+
			"   TMP = \"$(pwd)/tmp\","+
			"   },"+
			"   command = \"mkdir $(pwd)/tmp; echo ${ARRAYID} 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr ;  cp -v <LOG_DIR>/stdout " + HERE + "/testHostname.o1-2 >> <LOG_DIR>/stderr 2>&1 ;  cp -v <LOG_DIR>/stderr "+HERE+"/testHostname.e1-2 >> <LOG_DIR>/stderr 2>&1 \""+
			"  },"+
			"  {"+
			"   timeout = 3600000,"+
			"   cores = 1,"+
			"   memory = 512,"+
			"   env = job_env {"+
			"    CORES = \"1\","+
			"    VMEM = \"512\","+
			"    ARRAYID = \"3\","+
			"   TMP = \"$(pwd)/tmp\","+
			"   },"+
			"   command = \"mkdir $(pwd)/tmp; echo ${ARRAYID} 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr ;  cp -v <LOG_DIR>/stdout " + HERE + "/testHostname.o1-3 >> <LOG_DIR>/stderr 2>&1 ;  cp -v <LOG_DIR>/stderr "+HERE+"/testHostname.e1-3 >> <LOG_DIR>/stderr 2>&1 \""+
			"  },"+
			"  {"+
			"   timeout = 3600000,"+
			"   cores = 1,"+
			"   memory = 512,"+
			"   env = job_env {"+
			"    CORES = \"1\","+
			"    VMEM = \"512\","+
			"    ARRAYID = \"4\","+
			"   TMP = \"$(pwd)/tmp\","+
			"   },"+
			"   command = \"mkdir $(pwd)/tmp; echo ${ARRAYID} 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr ;  cp -v <LOG_DIR>/stdout " + HERE + "/testHostname.o1-4 >> <LOG_DIR>/stderr 2>&1 ;  cp -v <LOG_DIR>/stderr "+HERE+"/testHostname.e1-4 >> <LOG_DIR>/stderr 2>&1 \""+
			"  },"+
			"  {"+
			"   timeout = 3600000,"+
			"   cores = 1,"+
			"   memory = 512,"+
			"   env = job_env {"+
			"    CORES = \"1\","+
			"    VMEM = \"512\","+
			"    ARRAYID = \"5\","+
			"   TMP = \"$(pwd)/tmp\","+
			"   },"+
			"   command = \"mkdir $(pwd)/tmp; echo ${ARRAYID} 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr ;  cp -v <LOG_DIR>/stdout " + HERE + "/testHostname.o1-5 >> <LOG_DIR>/stderr 2>&1 ;  cp -v <LOG_DIR>/stderr "+HERE+"/testHostname.e1-5 >> <LOG_DIR>/stderr 2>&1 \""+
			"  },"+
			" }"+
			"}";

}
