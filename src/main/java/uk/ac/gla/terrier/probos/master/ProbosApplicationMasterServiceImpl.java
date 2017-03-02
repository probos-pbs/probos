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

package uk.ac.gla.terrier.probos.master;

import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServlet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.gla.terrier.probos.PBSClientFactory;
import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.api.PBSMasterClient;
import uk.ac.gla.terrier.probos.api.PBSMasterClient.EventType;
import uk.ac.gla.terrier.probos.api.ProbosDelegationTokenIdentifier;
import uk.ac.gla.terrier.probos.common.MapEntry;
import uk.ac.gla.terrier.probos.common.WebServer;
import uk.ac.gla.terrier.probos.master.webapp.ConfServlet;
import uk.ac.gla.terrier.probos.master.webapp.JobProgressServlet;
import uk.ac.gla.terrier.probos.master.webapp.KittenConfServlet;
import uk.ac.gla.terrier.probos.master.webapp.LogsServlet;
import uk.ac.gla.terrier.probos.master.webapp.QstatJobServlet;

import com.cloudera.kitten.ContainerLaunchParameters;
import com.cloudera.kitten.appmaster.ApplicationMasterParameters;
import com.cloudera.kitten.appmaster.service.ApplicationMasterServiceImpl;

public class ProbosApplicationMasterServiceImpl extends ApplicationMasterServiceImpl {

	static final String[] REQUIRED_ENV = new String[]{"PBS_CONTROLLER", "PBS_JOBID", "CONTAINER_ID"};
	
	private static final long HEARTBEAT_INTERVAL_MS = 3600 * 1000;
	
	private static final Logger LOG = LoggerFactory.getLogger(ProbosApplicationMasterServiceImpl.class);
	final PBSMasterClient masterClient;
	final PBSClient controllerClient;
	final WebServer webServer;
	final String container;
	final int jobId;
	final Configuration conf;
	final List<Token<ProbosDelegationTokenIdentifier>> probosTokens;
	final ProbosTokenRenewer renewer;
	long lastHeartbeat = 0;
	final TIntObjectHashMap<ContainerId> arrayId2ContainerId = new TIntObjectHashMap<ContainerId>();
	
	@SuppressWarnings("unchecked")
	public ProbosApplicationMasterServiceImpl(
			ApplicationMasterParameters parameters, Configuration _conf) throws Exception {
		super(parameters, _conf);
		LOG.info("Starting " + this.getClass().getSimpleName() + " on "+ Utils.getHostname());

		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
		probosTokens = new ArrayList<Token<ProbosDelegationTokenIdentifier>>();
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        LOG.info("Executing on "+Utils.getHostname()+" with tokens:");
        while (iter.hasNext()) {
          Token<?> token = iter.next();
          LOG.info(token.toString());
          if (token.getKind().equals(ProbosDelegationTokenIdentifier.KIND_NAME))
          {        	 
        	  probosTokens.add((Token<ProbosDelegationTokenIdentifier>) token);
          }
        }
        renewer = new ProbosTokenRenewer();
        
		this.conf = _conf;
		StringWriter sw = new StringWriter();
		Configuration.dumpConfiguration(conf, sw);
		//LOG.info("Master conf is " + sw.toString());
		
		for(String k : REQUIRED_ENV) {
			if (System.getenv(k) == null)
				throw new IllegalArgumentException("Env "+k+" must be set");
		}
		
		String hostPort = System.getenv("PBS_CONTROLLER");
		String[] hostPortSplit = hostPort.split(":");
		final String serverHostname = hostPortSplit[0];
		final int port = Integer.parseInt(hostPortSplit[1]);
		
		final InetSocketAddress server = new InetSocketAddress(
				serverHostname, 
				port);
		masterClient = RPC.getProxy(PBSMasterClient.class,
		        RPC.getProtocolVersion(PBSMasterClient.class),
		        server, UserGroupInformation.getCurrentUser(), conf, 
		        NetUtils.getDefaultSocketFactory(conf));
		controllerClient = PBSClientFactory.getPBSClient();
		LOG.info("Connected to controller " + hostPort);
		
		jobId = Integer.parseInt(System.getenv("PBS_JOBID"));
		container = System.getenv("CONTAINER_ID");
		masterClient.jobEvent(jobId, EventType.MASTER_START, container, null);		
		
		final List<Entry<String,HttpServlet>> masterServlets = new ArrayList<>();
		masterServlets.add(new MapEntry<String,HttpServlet>("/", new JobProgressServlet("./", masterServlets, controllerClient, this)));
		masterServlets.add(new MapEntry<String,HttpServlet>("/qstatjob", new QstatJobServlet("./qstatjob", masterServlets, controllerClient, this)));	
		masterServlets.add(new MapEntry<String,HttpServlet>("/conf", new ConfServlet("./conf", masterServlets, controllerClient, this)));
		masterServlets.add(new MapEntry<String,HttpServlet>("/kittenconf", new KittenConfServlet("./kittenconf", masterServlets, controllerClient, this)));
		masterServlets.add(new MapEntry<String,HttpServlet>("/logs", new LogsServlet("./logs", masterServlets, controllerClient, this)));
		
		//0 means any random free port
		webServer = new WebServer("ProbosControllerHttp", masterServlets, 0);
		webServer.init(_conf);
	}
	
	@Override
	protected void startUp() throws IOException {
		
		webServer.start();
		String uri = webServer.getURI().toString();
		LOG.info("Ready to serve HTTP requests at " + uri);
		super.getParameters().setTrackingUrl(uri);
		super.startUp();
	}

	@Override
	protected void runOneIteration() throws Exception {
		final long now = System.currentTimeMillis();
		if (now - lastHeartbeat> HEARTBEAT_INTERVAL_MS)
		{
			for(Token<ProbosDelegationTokenIdentifier> t : probosTokens)
			{
				//masterClient.heartbeat(jobId, t);
				renewer.renew(t, this.conf);
			}
			lastHeartbeat = System.currentTimeMillis();
		}
		super.runOneIteration();
	}

	@Override
	public void onShutdownRequest() {
		webServer.stop();
		RPC.stopProxy(masterClient);
		super.onShutdownRequest();
	}
	
	public Configuration getConf()
	{
		return this.conf;
	}

	public int getJobId()
	{
		return jobId;
	}
	
	public boolean isArray()
	{
		return arrayId2ContainerId.size() > 0;
	}
	
	public TIntObjectHashMap<ContainerId> getArrayIds()
	{
		return arrayId2ContainerId;
	}
	
	protected ContainerTracker getTracker(ContainerLaunchParameters clp) {
		if (clp.getEnvironment().get("PBS_ARRAYID") != null)
			return new ProbosArrayContainerTracker(clp);
		return new ProbosNormalContainerTracker(clp);
	}
	
	@Override
	public void onContainersCompleted(List<ContainerStatus> containerList) {
		super.onContainersCompleted(containerList);
		
		//?*HACK*?
		//for some reason, we don't observe onContainerStopped() being called 
		//until the end of the application, but onContainersCompleted() being
		//called as expected. Hence, we map these through to help the Controller
		//observe array task completions
		
		for(ContainerStatus status : containerList) {
			ContainerId cid = status.getContainerId();
			for (ContainerTracker tracker : trackers) {
				if (tracker.owns(cid))
					if (tracker instanceof ProbosContainerTracker)
						((ProbosContainerTracker)tracker).onContainerStopped(status);
					else
						tracker.onContainerStopped(cid);
			}
		}
	}

	@Override
	protected void shutDown() {
		super.shutDown();
		try{
			masterClient.jobEvent(jobId, EventType.MASTER_END, container, null);
		} catch (Exception e) {
			LOG.warn("Failed to send MASTER_END event to controller", e);
		}
	}
	
	/** a ContainerTracker for array jobs. a container failure does not indicate
	 * a job failure */
	protected class ProbosArrayContainerTracker extends ProbosContainerTracker {

		int arrayId = 0;
		
		public ProbosArrayContainerTracker(ContainerLaunchParameters parameters) {
			super(parameters);
			arrayId = Integer.parseInt(parameters.getEnvironment().get("PBS_ARRAYID"));
		}
		
		@Override
		public void onContainerStarted(ContainerId containerId,
				Map<String, ByteBuffer> allServiceResponse) {
			super.onContainerStarted(containerId, allServiceResponse);
			masterClient.jobArrayTaskEvent(jobId, arrayId, EventType.START, containerId.toString(), null);
			arrayId2ContainerId.put(arrayId, containerId);
		}

		@Override
		public void onContainerStopped(ContainerStatus containerStatus) {
			super.onContainerStopped(containerStatus.getContainerId());
			String message = null;
			int code;
			if ( (code = containerStatus.getExitStatus()) != 0)
			{
				message = "(exit code "+code+") "+ containerStatus.getDiagnostics();
				String[] paths = masterClient.getJobArrayOutputFiles(jobId, arrayId);
				new Thread(new OuputFileWriter(">>PBS Job array task stopped : " + message, paths)).start();
			}
			masterClient.jobArrayTaskEvent(jobId, arrayId, EventType.END, containerStatus.getContainerId().toString(), message);
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			super.onContainerStopped(containerId);
			masterClient.jobArrayTaskEvent(jobId, arrayId, EventType.END, containerId.toString(), null);
		}
		
		@Override
		public void onStopContainerError(ContainerId containerId,
				Throwable throwable) {
			super.onStopContainerError(containerId, throwable);
			String message = makeMessage(throwable);
			masterClient.jobArrayTaskEvent(jobId, arrayId, EventType.TERMINATE, containerId.toString(), message);
		}
	}
	
	
	/** a ContainerTracker for normal jobs, rather than array jobs */
	protected class ProbosNormalContainerTracker extends ProbosContainerTracker {
		
		public ProbosNormalContainerTracker(ContainerLaunchParameters parameters) {
		    super(parameters);
		}
		
		@Override
		public void onContainerStarted(ContainerId containerId,
				Map<String, ByteBuffer> allServiceResponse) {
			super.onContainerStarted(containerId, allServiceResponse);
			masterClient.jobEvent(jobId, EventType.START, containerId.toString(), null);
		}

		//a new containerStop event which records the ContainerStatus diagnostic message
		public void onContainerStopped(ContainerStatus containerStatus) {
			super.onContainerStopped(containerStatus.getContainerId());
			String message = null;
			int code;
			if ( (code = containerStatus.getExitStatus()) != 0)
			{
				message = "(exit code "+code+") "+ containerStatus.getDiagnostics();
				String[] paths = masterClient.getJobOutputFiles(jobId);
				new Thread(new OuputFileWriter(">>PBS Job stopped: " + message, paths)).start();
			}
			masterClient.jobEvent(jobId, EventType.END, containerStatus.getContainerId().toString(), message);
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			super.onContainerStopped(containerId);
			masterClient.jobEvent(jobId, EventType.END, containerId.toString(), null);
		}

		@Override
		public void onStopContainerError(ContainerId containerId,
				Throwable throwable) {
			super.onStopContainerError(containerId, throwable);
			
			String message = makeMessage(throwable);
			masterClient.jobEvent(jobId, EventType.TERMINATE, containerId.toString(), message);
		}
		
	}
	
	/** a generic, abstract ContainerTracker for containers relating to probos.
	 * we also expose a onContainerStopped(ContainerStatus) method that can be
	 * called from onContainersCompleted() */
	protected abstract class ProbosContainerTracker extends ContainerTracker {
		
		int jobId;
		boolean array = false;
		int arrayId = 0;
		
		protected ProbosContainerTracker(ContainerLaunchParameters parameters) {
		    super(parameters);
		    jobId = Integer.parseInt(parameters.getEnvironment().get("PBS_JOBID"));
		}
		
		public abstract void onContainerStopped(ContainerStatus containerStatus);

		protected String makeMessage(Throwable throwable) {
			String message = null;
			if (throwable != null)
			{
				StringWriter s = new StringWriter();
				PrintWriter p = new PrintWriter(s);
				p.append(throwable.toString());
				throwable.printStackTrace(p);
				p.flush();
				message = s.toString();
			}
			return message;
		}
		
	}

	/** 
	 * Runnable class for writing out/error files out in event of an error, using RCP if necessary 
	 */
	static class OuputFileWriter implements Runnable
	{	
		boolean append = true;
        String message;
        String[] paths;

        public OuputFileWriter(String message, String[] paths) {
                this.message = message;
                this.paths = paths;
        }
        
        /** write a file with the message to the specified location, using the specified RCP if necessary (based on the filename) */
        void writeFile(String path, String rcp) throws Exception
        {
        	//path will start with "hostname:" if RCP is to be used (according to the controller)
        	if ( ! path.contains(":"))
        	{
        		//write the file directly
        		FileWriter fw = new FileWriter(path);
				fw.write(message);
				fw.close();
				return;
        	}
        	if (append)
        		appendRemotePath(path,rcp);
        	else
        		overwriteRemotePath(path, rcp);        	
        }
        
        /** uses rsh/ssh to append the error message to the output file */
        void appendRemotePath(String path, String rcp)
        	throws IOException, InterruptedException
        {
        	//we assume these are accessible on the path for the ApplicationMaster process
        	String rsh_command = rcp.endsWith("rcp") ? "rsh" : "ssh";
        	
        	File tempFile = File.createTempFile("joberr", ".file");
        	FileWriter fw = new FileWriter(tempFile);
        	fw.close();
        	
        	String[] parts = path.split(":", 2);
        	String host = parts[0];
        	String targetFile = parts[1];
        	
        	ProcessBuilder pb = new ProcessBuilder();
        	String[] RSH_ARGS = new String[]{rsh_command, host, "cat >> " + targetFile};
        	pb.redirectInput(tempFile);
        	pb.command(RSH_ARGS);
        	Process p = pb.start();
        	p.waitFor();
        	int exit = p.exitValue();
        	if (exit != 0){
        		LOG.warn("Could not append supplemental output file to " + path + " exit code "+ exit + " cmd=" + Arrays.toString(RSH_ARGS));
        	}
        	tempFile.delete();
        }

        /** uses rcp/scp to write the error message to the output file */
		void overwriteRemotePath(String path, String rcp)
				throws IOException, InterruptedException {
			//this is a host path. we need to recover the path, 
    		//write to a temporary file, then use RCP to copy it
    		//to the client machine
    		//String targetFilename;
        	//String[] parts = path.split(":", 2);
        	//path = parts[1];
        	//String host = parts[0];
        	
        	File tempFile = File.createTempFile("joberr", ".file");
        	FileWriter fw = new FileWriter(tempFile);
        	fw.close();
        	
        	ProcessBuilder pb = new ProcessBuilder();
        	String[] RCP_ARGS = new String[]{rcp, tempFile.toString(), path};
        	pb.command(RCP_ARGS);
        	Process p = pb.start();
        	p.waitFor();
        	int exit = p.exitValue();
        	if (exit != 0){
        		LOG.warn("Could not copy supplemental output file to " + path + " exit code "+ exit + " cmd=" + Arrays.toString(RCP_ARGS));
        	}
        	tempFile.delete();
		}

        @Override
        public void run() {
        	try{
	        	//write stdout file, using specified RCP if necessary
	        	writeFile(paths[0], paths[2]);
	        	//write stdout file, using specified RCP if necessary
	        	writeFile(paths[1], paths[2]);
        	} catch (Exception e ) {
        		LOG.warn("Problem writing supplemental output file", e);
        	}
        	
        }
	}

	
	class ProbosTokenRenewer extends TokenRenewer
	{
		
		private ProbosTokenRenewer() {}
		

		@Override
		public boolean handleKind(Text kind) {
			return ProbosDelegationTokenIdentifier.KIND_NAME.equals(kind);
		}

		@Override
		public boolean isManaged(Token<?> token) throws IOException {
			return true;
		}

		@SuppressWarnings("unchecked")
		@Override
		public long renew(Token<?> token, Configuration conf)
				throws IOException, InterruptedException {
			try{
				return masterClient.heartbeat(jobId, (Token<ProbosDelegationTokenIdentifier>) token);
			}catch (Exception e) {
				throw new IOException(e);
			}			
		}

		@Override
		public void cancel(Token<?> token, Configuration conf)
				throws IOException, InterruptedException {
			throw new UnsupportedOperationException();
		}
		
	}

}
