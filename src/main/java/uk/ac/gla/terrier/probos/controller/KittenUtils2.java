/**
 * Copyright (c) 2016, University of Glasgow. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package uk.ac.gla.terrier.probos.controller;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.gla.terrier.probos.Constants;
import uk.ac.gla.terrier.probos.PConfiguration;
import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.api.PBSJob;

import com.cloudera.kitten.util.LocalDataHelper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

public class KittenUtils2 {

	static final int TASK_COUNT_NEEDS_CORE_FOR_APP_MASTER = 100;
	static final boolean DEBUG_MASTER = false;
	
	@VisibleForTesting
	public static final boolean DEBUG_TASKS = false;
	
	protected static final Logger LOG = LoggerFactory.getLogger(KittenUtils2.class);
	
	Configuration pConf;
	int jobid;
	PBSJob job;
	boolean noProBoS;
	NodeRequest nodeSpec;
	
	public KittenUtils2(Configuration pConf, int jobid, PBSJob job, NodeRequest nodeSpec)
	{
		this.pConf = pConf;
		this.jobid = jobid;
		this.job = job;
		this.nodeSpec = nodeSpec;
	}
	
	public int getNumberOfTasks()
	{
		return 1;
	}
	
	public void setProbosMasterStatus(boolean noProBoS) {
		this.noProBoS = noProBoS;
	}
	
	public void writeKJobToFile(Path targetScript, File luaFile) throws IOException
	{
		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(luaFile)));
		writeKJob(targetScript, pw);		
		pw.close();
		
		BufferedReader br = new BufferedReader(new FileReader(luaFile));
		String line = null;
		while((line = br.readLine()) != null)
		{
			System.out.println(line);
		}
		br.close();		
	}
	
	public void writeKJob(Path targetScript, PrintWriter w) throws IOException
	{
		w.println("KITTEN_MASTER_JAR_LOCATION = \""+Constants.PACKAGED_KITTEN_JAR_ON_SERVER+"\"");
		w.println("PROBOS_MASTER_JAR_LOCATION = \""+Constants.PACKAGED_PROBOS_JAR_ON_SERVER+"\"");
		w.println("base_resources = {");
		w.println("\t[\"kitten.jar\"] = { file = KITTEN_MASTER_JAR_LOCATION },");
		w.println("\t[\"probos.jar\"] = { file = PROBOS_MASTER_JAR_LOCATION }");
		w.println("}");
		w.println("base_env = cat {");
		w.println("\tCLASSPATH = table.concat({\"${CLASSPATH}\", \"./kitten.jar\", \"./probos.jar\"}, \":\"),");
		w.println("\tPBS_JOBID = \"" + String.valueOf(jobid) + "\",");
		w.println("}");

		w.println("job_env = cat {");
		w.println("\tCLASSPATH = table.concat({\"${CLASSPATH}\", \"./kitten.jar\", \"./probos.jar\"}, \":\"),");
		w.println("\tPBS_JOBID = \"" + String.valueOf(jobid) + "\",");
		
		for(String k : new TreeSet<>(job.getVariable_List().keySet()))
		{
			String value = job.getVariable_List().get(k);
			assert value != null : "Value for " + k + " was null";
			//skip BASH_FUNC_module()="() \{  eval `/usr/bin/modulecmd bash $*`\10\}"
			if (value.contains("`"))
				continue;
			w.println("\t" + luaKeyEscape(k) + " = \"" + luaStringEscape(value) + "\",");
		}
		w.println("}");
		
		
		
		String controllerMasterAddress = Utils.getHostname() + ":"+ String.valueOf(1+pConf.getInt(PConfiguration.KEY_CONTROLLER_PORT, 8027));
		//w.println("job_env_fn = cat { job_env }");
		w.println(Constants.PRODUCT_NAME + " = yarn {");
		w.println(" name = \"" + job.getJob_Name() + "\",");
		w.println(" app_type = \"" + Constants.PRODUCT_NAME + "\",");
		w.println(" timeout = -1,");
		if (job.getQueue() != null && job.getQueue().length() > 0 && ! job.getQueue().equals("default") && ! job.getQueue().equals("null"))
			w.println(" queue = \""+job.getQueue()+"\",");
		w.println(" memory = 512,");
		renderMaster(controllerMasterAddress, w, new HashMap<String,String>());

		//TODO: check the job's rerunnable flag
		
		renderJob(targetScript, w);
		w.println("}");
	}

	protected void renderJob(Path targetScript, PrintWriter w) throws IOException 
	{	
		//plain single node job
		w.println(" container = {");
		w.println("  instances = 1,");
		printTaskContainer(String.valueOf(jobid), targetScript, w, "  ", Collections.<String,String>emptyMap(), this.nodeSpec);
		w.println(" }");
	}

	protected void renderMaster(String controllerMasterAddress, PrintWriter w, Map<String,String> extraEnv) {
		w.println(" master = {");
		w.println("  env = base_env {");
		w.println("    PBS_CONTROLLER = \""+ controllerMasterAddress +"\",");
		for(String k : extraEnv.keySet())
		{
				w.println("    " + luaKeyEscape(k) + " = \"" + luaStringEscape(extraEnv.get(k)) + "\",");
		}
		w.println("  },");
		
		//gitlab issue 7: tell Kitten where you want the temporary folder placed
		w.println("  conf = {");
		w.println("    [\""+ LocalDataHelper.APP_BASE_DIR + "\"] = \"" + pConf.get(PConfiguration.KEY_CONTROLLER_JOBDIR) + "\"");
		//THIS DOENST WORK
		//w.println("    [\""+ YarnConfiguration.NM_USER_HOME_DIR + "\"] = \"" + job.getVariable_List().get("HOME") + "\"");
		w.println("  },");
		
		w.println("  resources = base_resources,");
		
		//we only allocate a core for the master when we get to really 
		// big jobs (# of array tasks, or # of parallel tasks)
		if (getNumberOfTasks() > TASK_COUNT_NEEDS_CORE_FOR_APP_MASTER)
			w.println("  cores = 1,");
		else
			w.println("  cores = 0,");
		w.println("  command = {");
		
		//String javaOpts = "";
		//javaOpts = "-agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=y";
		final String masterCmd = getAppMasterCommand();
		if (DEBUG_MASTER)
			w.println("   base = \"export 1>> <LOG_DIR>/stdout ; echo "+masterCmd+" 1>> <LOG_DIR>/stdout ; "+masterCmd+"\" ,");
		else
			w.println("   base = \""+masterCmd+"\",");
		w.println("   args = { \"-conf job.xml\", \"1>> <LOG_DIR>/stdout\", \"2>> <LOG_DIR>/stderr\" },"); //{},");// 
		w.println("  }");
		w.println(" },");
	}

	protected String getAppMasterCommand() {
		return noProBoS
			? "${JAVA_HOME}/bin/java -cp \\\"${CLASSPATH}:`HADOOP_CONF_DIR= hadoop classpath`}\\\" -Xms64m -Xmx128m "
					+ "com.cloudera.kitten.appmaster.ApplicationMaster"
			: Constants.PROBOS_HOME + "/bin/pbs_appmaster";
	}
	
	protected static void printNodeLabels(String prefix, PrintWriter w, String[] nodeLabels)
	{
		if (nodeLabels == null || nodeLabels.length == 0)
			return;
		w.println(prefix + "node_labels = \"" +StringUtils.join(nodeLabels, " && ") + "\",");
	}
	
	protected void printContainer(String jt_id, String command, PrintWriter w, 
			String prefix, Map<String,String> extraEnv, NodeRequest thisNode)
	{
		long jobDuration = pConf.getLong(PConfiguration.KEY_JOB_DEFAULT_DURATION, 3600);
		if (job.getResource_List().containsKey("walltime"))
		{
			String wallTimeLimit = job.getResource_List().get("walltime");
			try {
				jobDuration = Utils.parseTime(wallTimeLimit);
			} catch (IllegalArgumentException iae) {
				LOG.warn("Invalid time expression " + wallTimeLimit + " - setting to default" + jobDuration + " seconds");
			}
		}
		
		//set HOME variable for containers by overriding the yarn configuration.
		//THIS DOENST WORK
		//w.println("  conf = {");
		//w.println("    [\""+ YarnConfiguration.NM_USER_HOME_DIR + "\"] = \"" + job.getVariable_List().get("HOME") + "\"");
		//w.println("  },");
		
		//kitten expects a timeout in ms
		w.println(prefix + "timeout = " + String.valueOf(jobDuration*1000l) + ",");
		
		//Resource capability = JobUtils.getResources(job, pConf);
		w.println(prefix + "cores = " + String.valueOf(thisNode.corePerNode) + ",");
		w.println(prefix + "memory = " + String.valueOf(thisNode.memory) + ",");
		w.println(prefix + "gpus = " + String.valueOf(thisNode.gpus) + ",");
		
		//node_label
		printNodeLabels(prefix, w, thisNode.labels);
		//node hostname
		if (thisNode instanceof SingleNodeRequest)
		{
			w.println(prefix + "node = \""+((SingleNodeRequest)thisNode).hostname+"\",");
		}
			
		//next, create the container's environment
		w.print(prefix + "env = job_env");
		w.println(" {");
		w.println(prefix + " PBS_CORES = \""+ String.valueOf(thisNode.corePerNode) + "\",");
		w.println(prefix + " PBS_GPUS = \""+ String.valueOf(thisNode.gpus) + "\",");	
		w.println(prefix + " PBS_VMEM = \""+ String.valueOf(thisNode.memory) + "\",");
		for(String k : extraEnv.keySet())
		{
				w.println(prefix + " " + luaKeyEscape(k) + " = \"" + luaStringEscape(extraEnv.get(k)) + "\",");
		}
		w.println(prefix + " PBS_YARN_WORKDIR = \"$(pwd)\",");
		w.println(prefix + " TMP = \"$(pwd)/tmp\",");
		w.print(prefix + "}");
		w.println(",");

		w.println(prefix + "command = \"" + command + "\"" );
	}

	protected void printTaskContainer(String jt_id, Path targetScript, PrintWriter w, String prefix, Map<String,String> extraEnv, NodeRequest nr)
	{	
		//its important to have an absolute path, lets just check this here
		assert targetScript.isUriPathAbsolute() : targetScript.toString() + " is not absolute";
		
		w.println(prefix + "resources = {");
		w.println(prefix + " [\"job.SC\"] = { hdfs = \""+ targetScript.toString() +"\" },");
		w.println(prefix + "},");
		
		String shell = job.getShell();
		if (shell == null)
		{
			shell = "${SHELL}";
		}
		
		//configure where the output
		String stdOutErrRedirect = null;
		String stdOutErrCopy = null;
		String jobJoin = job.getJoin();
		if (jobJoin == null)
		{
			stdOutErrRedirect = "1>> <LOG_DIR>/stdout 2>> <LOG_DIR>/stderr";
			stdOutErrCopy = prepareCopy(pConf, "<LOG_DIR>/stdout", job.getOutput_Path()+ jt_id) + " ; " +
					" " + prepareCopy(pConf, "<LOG_DIR>/stderr", job.getError_Path()+ jt_id);
		} else if (jobJoin.equals("oe")) {
			stdOutErrRedirect = "1>> <LOG_DIR>/stdout 2>&1 ";
			stdOutErrCopy = prepareCopy(pConf, "<LOG_DIR>/stdout", job.getOutput_Path()+ jt_id);
		} else if (jobJoin.equals("eo")) {
			stdOutErrRedirect = "2>> <LOG_DIR>/stdout 1>&2 ";
			stdOutErrCopy = prepareCopy(pConf, "<LOG_DIR>/stderr", job.getError_Path()+ jt_id);
		} else {
			throw new IllegalArgumentException("invalid job join: " + jobJoin);
		}
		
		//find where we are going to start the job
		String initdir = job.getVariable_List().get("PBS_O_INITDIR");
		
		
		
		//TODO: in the following PBS_O_HOME is a hack. YARN overwrites the HOME env var,
		//(see ContainerLaunch.java:690)
		//so we assume, possibly unreasonably, that the user's home directory on 
		//the execution node is the same as on the submission host
		String finalCommand = " "  
				+ "cd " + (initdir != null ? initdir : " ${PBS_O_HOME}") + "; " //change to the correct folder
				+ shell + " ${PBS_YARN_WORKDIR}/job.SC"  // run the command
				+ " " + stdOutErrRedirect + " ; "  //capture the stdout & stderr
				+ " EXIT=\\\\${?}; "
				+ (DEBUG_TASKS ? "echo Exit code was \\\\${EXIT} 1>> <LOG_DIR>/stdout 2>> <LOG_DIR>/stderr;" : "") 
				+ " " + stdOutErrCopy + " ; "; //copy the stdout & stderr back to the submission node:/path
				
		if (DEBUG_TASKS)
			finalCommand = "echo '" + finalCommand + "' 1>> <LOG_DIR>/stdout 2>><LOG_DIR>/stderr ; "  + finalCommand;
		
		final String finalCmd = finalCommandPrefix() + finalCommand + finalCommandSuffix() + " exit \\\\${EXIT}; ";
		printContainer(jt_id, finalCmd, w, prefix, extraEnv, nr);
	}
	
	protected String finalCommandPrefix()
	{
		String rtr = "mkdir -p $(pwd)/tmp; "; //make a tmp directory
		if (job.getVariable_List().containsKey("HOME"))
			rtr += " export HOME='"+job.getVariable_List().get("HOME") + "' ;"; 
		
		//issue #4, CLASSPATH gets set unexpectedly by YARN
		if (job.getVariable_List().containsKey("CLASSPATH"))
			rtr += " export CLASSPATH='"+job.getVariable_List().get("CLASSPATH") + "' ;"; 
		else
			rtr += " export CLASSPATH='' ;"; 
		return rtr;
	}
	
	protected String finalCommandSuffix()
	{
		return "";
	}
	
	public static KittenUtils2 createKittenUtil(Configuration pConf, PBSJob job, int jobid)
	{
		List<NodeRequest> requestList = Lists.newArrayList();
		int totalNodes = parseNodeResourceList(job, requestList);
		if (job.getInteractive())
		{
			if (totalNodes > 1)
			{
				throw new IllegalArgumentException();
			}
			return new InteractiveKittenUtil(pConf, jobid, job, requestList.get(0));
		}
		else if (job.getArrayTaskIds() != null)
		{
			if (totalNodes > 1)
			{
				throw new IllegalArgumentException();
			}
			return new ArrayKittenUtils(pConf, jobid, job, requestList.get(0));
		}
		else if (totalNodes > 1)
		{
			assert totalNodes > 1;
			return new DistributedKittenUtils(pConf, jobid, job, requestList, totalNodes);
		}		
		else
		{
			if (totalNodes > 1)
			{
				throw new IllegalArgumentException();
			}
			return new KittenUtils2(pConf, jobid, job, requestList.get(0));
		}
	}

	static Pattern MEMCOUNT = Pattern.compile("^mem=(\\d+)$");
	static Pattern PPNCOUNT = Pattern.compile("^ppn=(\\d+)$");
	static Pattern GPUCOUNT = Pattern.compile("^gpus=(\\d+)$");
	static Pattern COUNT = Pattern.compile("^\\d+");	
	
	/** Represents how a job requests a node */
	static class NodeRequest {
		int corePerNode;
		int memory;
		int gpus;
		String[] labels;
		
		NodeRequest(int ppn, int mem, int gpus, String[] reqLabels) {
			this.corePerNode = ppn;
			this.memory = mem;
			this.gpus = gpus;
			this.labels = reqLabels;
		}
	}
	
	/** a node request where the user specified the hostname
	 * they wanted
	 */
	static class SingleNodeRequest extends NodeRequest {
		
		String hostname;
		
		SingleNodeRequest(int ppn, int mem, int gpus, String[] reqLabels, String host) {
			super(ppn, mem, gpus, reqLabels);
			this.hostname = host;
		}		
	}
	
	/** a node request for a number of node of 
	 * the same specification */
	static class MultiNodeRequest extends NodeRequest {
		
		int count;
		
		MultiNodeRequest(int ppn, int mem, int gpus, String[] reqLabels, int number) {
			super(ppn, mem, gpus, reqLabels);
			this.count = number;
		}
		
	}
	
	@VisibleForTesting
	public static boolean isNode(String node)
	{
		//TODO this is a hack
		return node.matches(".*node(-?)\\d+");
	}
	
	protected static int parseNodeResourceList(PBSJob job,
			List<NodeRequest> requestList) {
		int DEFAULT_MEM = 512;
		int DEFAULT_GPUS = 0;
		int totalNodeCount = 0;
		String nodesExpression =  job.getResource_List().get("nodes");
		//no nodes statement, return default
		if (nodesExpression == null)
		{
			requestList.add(new NodeRequest(1, DEFAULT_MEM, DEFAULT_GPUS, null));
			return 1;
		}
		String[] eachNodeRequest = nodesExpression.split("\\+");
		for(String request : eachNodeRequest) 
		{
			final String[] requestPortions = request.split(":");
			int nodeCount = 1;
			int mem = DEFAULT_MEM;
			int gpus = DEFAULT_GPUS;
			List<String> labels = Lists.newArrayList();
			String nodeName = null;
			int ppn = 1;
			for(String portion : requestPortions)
			{
				Matcher m; 
				if (COUNT.matcher(portion).matches())
				{//its a node count
					nodeCount = Integer.parseInt(portion);
				}
				else if ( (m = PPNCOUNT.matcher(portion)).matches() )
				{
					ppn = Integer.parseInt(m.group(1));
				}
				else if ( (m = MEMCOUNT.matcher(portion)).matches() )
				{
					mem = Integer.parseInt(m.group(1));
				}
				else if ( (m = GPUCOUNT.matcher(portion)).matches() )
				{
					gpus = Integer.parseInt(m.group(1));
				}
				else if (isNode(portion))
				{
					nodeName = portion;
				}
				else
				{
					labels.add(portion);
				}
			}
			
			if (nodeName != null && nodeCount > 1)
			{
				throw new IllegalArgumentException("Cannot have a node cout for a hostname");
			}
			NodeRequest spec;
			if (nodeCount > 1)
			{
				spec = new MultiNodeRequest(ppn, mem, gpus, labels.toArray(new String[0]), nodeCount);
			}
			else if (nodeName != null) 
			{
				spec = new SingleNodeRequest(ppn, mem, gpus, labels.toArray(new String[0]), nodeName);
			}
			else
			{
				spec = new NodeRequest(ppn, mem, gpus, labels.toArray(new String[0]));
			}
			requestList.add(spec);
			totalNodeCount += nodeCount;
		}
		return totalNodeCount;
	}
	
	
	
	/** uses cp if it matches one of the known prefixes, otherwise uses scp
	 * @return the copying string */
	static final String prepareCopy(Configuration pConf, String localSource, String hostDest)
	{
		final String split[] = hostDest.split(":", 2);
		assert split.length > 1 : "hostDest has no hostname: '"+hostDest+"'";
		assert split[0] != null;
		assert ! split[0].equals("null");
		
		final String RCP = pConf.get(PConfiguration.KEY_RCP);
		assert RCP != null && RCP.length() > 0;
		final String path = split[1];
		
		boolean cp = detectUseCp(pConf, path);
		String rtr = null;
		if (cp)
		{
			rtr = "cp -v " + localSource + " " + path;
		}
		else
		{
			rtr = RCP + " -v " + localSource + " " + path;
		}
		//rtr = rtr + " >> /tmp/cp.log 2>&1  ";
		LOG.debug("Copying " + localSource + " to " + hostDest + " using " +(cp ? "cp" : RCP));
		//System.err.println(rtr);
		return rtr;
	}

	public static boolean detectUseCp(Configuration pConf, final String path)
	{
		boolean cp = false;
		final String[] USE_RCP_DIRS = pConf.getStrings(PConfiguration.KEY_RCP_USE);
		
		if (USE_RCP_DIRS != null)
		{
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Checking is " + path + " matches in " + Arrays.deepToString(USE_RCP_DIRS));
			}
			for(String prefix : USE_RCP_DIRS)
			{
				if (path.startsWith(prefix))
				{
					cp = true;
					LOG.debug(path + " matches in " + prefix);
					break;
				}
			}
		}
		else
		{
			LOG.warn(PConfiguration.KEY_RCP_USE + " was null, it should normally be at least empty.");
		}
		return cp;
	}
	
	static String luaKeyEscape(String name)
	{
		if (name == null)
			return null;
		if (name.contains(".") || name.contains("(") || name.contains(")") || name.contains("-"))
			return "[\"" + name + "\"]";
		return name;
	}
	
	static String luaStringEscape(String input)
	{
		if (input == null)
			return null;
		input = input.replace("\"", "\\\"");
		input = input.replace("{", "\\{");
		input = input.replace("}", "\\}");
		input = input.replace("\n", "\\10");
		return input;
	}

}
