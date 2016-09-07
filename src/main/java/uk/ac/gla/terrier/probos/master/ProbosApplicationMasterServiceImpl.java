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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import uk.ac.gla.terrier.probos.common.BaseServlet;
import uk.ac.gla.terrier.probos.common.MapEntry;
import uk.ac.gla.terrier.probos.common.WebServer;
import uk.ac.gla.terrier.probos.master.webapp.ConfServlet;
import uk.ac.gla.terrier.probos.master.webapp.JobProgressServlet;
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
		
		final List<Entry<String,BaseServlet>> masterServlets = new ArrayList<Entry<String,BaseServlet>>();
		masterServlets.add(new MapEntry<String,BaseServlet>("/", new JobProgressServlet("./", masterServlets, controllerClient, this)));
		masterServlets.add(new MapEntry<String,BaseServlet>("/qstatjob", new QstatJobServlet("./qstatjob", masterServlets, controllerClient, this)));	
		masterServlets.add(new MapEntry<String,BaseServlet>("/conf", new ConfServlet("./conf", masterServlets, controllerClient, this)));
		masterServlets.add(new MapEntry<String,BaseServlet>("/logs", new LogsServlet("./logs", masterServlets, controllerClient, this)));
		
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
		return new ProbosContainerTracker(clp);
	}
	
	@Override
	public void onContainersCompleted(List<ContainerStatus> containerList) {
		super.onContainersCompleted(containerList);
		
		//?*HACK*?
		//for some reason, we dont observe onContainerStopped() being called 
		//until the end of the application, but onContainersCompleted() being
		//called as expected. Hence, we map these through to help the Controller
		//observe array task completions
		
		for(ContainerStatus status : containerList) {
			ContainerId cid = status.getContainerId();
			for (ContainerTracker tracker : trackers) {
				if (tracker.owns(cid))
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
	
	protected class ProbosContainerTracker extends ContainerTracker {
		
		int jobId;
		boolean array = false;
		int arrayId = 0;
		
		public ProbosContainerTracker(ContainerLaunchParameters parameters) {
		    super(parameters);
		    jobId = Integer.parseInt(parameters.getEnvironment().get("PBS_JOBID"));
		}

		@Override
		public void onContainerStarted(ContainerId containerId,
				Map<String, ByteBuffer> allServiceResponse) {
			super.onContainerStarted(containerId, allServiceResponse);
			masterClient.jobEvent(jobId, EventType.START, containerId.toString(), null);
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
