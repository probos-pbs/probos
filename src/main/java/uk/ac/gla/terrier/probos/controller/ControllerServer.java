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

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TIntObjectProcedure;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServlet;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.jmx.JMXJsonServlet;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.gla.terrier.probos.Constants;
import uk.ac.gla.terrier.probos.JobUtils;
import uk.ac.gla.terrier.probos.PConfiguration;
import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.api.PBSInteractiveClient;
import uk.ac.gla.terrier.probos.api.PBSJob;
import uk.ac.gla.terrier.probos.api.PBSJobArrayStatusLight;
import uk.ac.gla.terrier.probos.api.PBSJobSelector;
import uk.ac.gla.terrier.probos.api.PBSJobStatusDistributed;
import uk.ac.gla.terrier.probos.api.PBSJobStatusFat;
import uk.ac.gla.terrier.probos.api.PBSJobStatusInteractive;
import uk.ac.gla.terrier.probos.api.PBSJobStatusLight;
import uk.ac.gla.terrier.probos.api.PBSJobStatusNodes;
import uk.ac.gla.terrier.probos.api.PBSMasterClient;
import uk.ac.gla.terrier.probos.api.PBSNodeStatus;
import uk.ac.gla.terrier.probos.api.ProbosDelegationTokenIdentifier;
import uk.ac.gla.terrier.probos.common.MapEntry;
import uk.ac.gla.terrier.probos.common.WebServer;
import uk.ac.gla.terrier.probos.controller.webapp.PbsnodesServlet;
import uk.ac.gla.terrier.probos.controller.webapp.QstatServlet;
import uk.ac.gla.terrier.probos.controller.webapp.VersionServlet;

import com.cloudera.kitten.client.YarnClientParameters;
import com.cloudera.kitten.client.YarnClientService;
import com.cloudera.kitten.client.params.lua.LuaYarnClientParameters;
import com.cloudera.kitten.client.service.YarnClientFactory;
import com.cloudera.kitten.client.service.YarnClientServiceImpl;
import com.cloudera.kitten.util.LocalDataHelper;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;
/** This ProBoS controller class implements the equivalent functionality of PBS Server. 
 * PBS jobs are received, and then passed onto YARN for processing.
 * We use Kitten to control the status of a job. The ApplicationMaster
 * informs the controller when job tasks start, end or are aborted.
 * @author craigm
 */
public class ControllerServer extends AbstractService implements PBSClient {
	
	static enum MailEvent {
		ABORT, BEGIN, END
	}
	
	static enum HoldType {
		USER,
		DEPENDENCY
	}
	
	static class JobHold {
		HoldType type;
		String owner;
		
		public JobHold(HoldType t, String o) {
			this.type = t;
			this.owner = o;
		}
		
		String dependencyName;
		int jobId;
	}
	
	static class JobInteractiveInfo {
		String hostname;
		int port;
		String secret;		
	}
	
	static class JobDistributedInfo {
		List<String> hostnames = Lists.newArrayList();
		TIntArrayList ports = new TIntArrayList();
		String secret;		
	}
	
	static boolean canBeReleased(int jobid, PBSJob j) {
		return false;
	}
	
	static class ControllerAPISecretManager extends AbstractDelegationTokenSecretManager<ProbosDelegationTokenIdentifier>
	{
		//60000, 60000, 60000, 60000
		public ControllerAPISecretManager(long delegationKeyUpdateInterval,
				long delegationTokenMaxLifetime,
				long delegationTokenRenewInterval,
				long delegationTokenRemoverScanInterval) {
			super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
					delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
		}

		@Override
		public ProbosDelegationTokenIdentifier createIdentifier() {
			String user = null;
			try{
				user = UserGroupInformation.getCurrentUser().getUserName();
			}catch (Exception e) {
				LOG.error("Could not determine current user for delegation token", e);
			}
			Text owner = new Text(user);
			return new ProbosDelegationTokenIdentifier(owner, owner, null);
		}		
	}
	
	private static final Random random = new Random();
	private static final Logger LOG = LoggerFactory.getLogger(ControllerServer.class);
	
	private Map<String, Object> extraLuaValues = ImmutableMap.<String, Object>of();
	private Map<String, String> extraLocalResources = ImmutableMap.<String, String>of();
	
	AtomicInteger nextJobId = new AtomicInteger();
	
	@VisibleForTesting public static class JobInformation {
		final long ctime = System.currentTimeMillis();
		long mtime = System.currentTimeMillis();
		
		int jobId;
		PBSJob jobSpec;
		JobInteractiveInfo interactive = null;
		JobDistributedInfo distributed = new JobDistributedInfo();
		
		String files;
		YarnClientService kitten;
		UserGroupInformation proxyUser;
		
		String masterContainerId;
		String taskContainerId;
		TIntObjectHashMap<String> array2Container = new TIntObjectHashMap<String>();
		Path scriptLocation;
		Path folderLocation;
		
		public JobInformation(int jobId, PBSJob spec) {
			this.jobId = jobId;
			this.jobSpec = spec;
		}
		
		public final void modify()
		{
			mtime = System.currentTimeMillis();
		}
		
	}
	
	//the main job datastructure. indexed by jobid
	TIntObjectHashMap<JobInformation> jobArray = new TIntObjectHashMap<JobInformation>();
	TObjectIntHashMap<String> user2QueuedCount = new TObjectIntHashMap<String>();
	
	TIntObjectHashMap<JobHold> jobHolds = new TIntObjectHashMap<JobHold>();
	
	final MetricRegistry metrics = new MetricRegistry();
	JmxReporter jmxMetricReporter;
	Configuration yConf;
	Configuration pConf;
	Thread watcherThread;
	final RPC.Server clientRpcserver;
	final RPC.Server masterRpcserver;
	final RPC.Server interactiveRpcserver;
	final ControllerAPISecretManager secretManager;
	final WebServer webServer;
	YarnClient yClient;
	MailClient mClient;
	final Path probosFolder;
	
	Counter runningJobs;
	Counter mailEvents;
	Counter mailFailures;
	Counter killedJobs;
	Counter rejectedJobs;
	
	protected void mailEvent(int jobid, PBSJob job, MailEvent event, String additionalContent)
	{
		String dest = job.getMailUser();
		String subject = null;
		switch(event){
		case ABORT:
			if (! job.getMailEvent().contains("a"))
				return;
			subject = "Job " + jobid + " was aborted";
			break;
		case BEGIN:
			if (! job.getMailEvent().contains("b"))
				return;
			subject = "Job " + jobid + " begun execution";
			break;
		case END:
			if (! job.getMailEvent().contains("e"))
				return;
			subject = "Job " + jobid + " completed execution";
			break;
		default:
			break;		
		}
		
		List<String> msg = new ArrayList<String>();
		msg.add(subject);
		if (additionalContent != null)
			msg.add(additionalContent);
		for (String username : dest.split(","))
		{
			mailEvents.inc();
			boolean status = mClient.sendEmail(username, subject, msg.toArray(new String[msg.size()]));
			if (! status)
			{
				LOG.warn("Failed to send email to "+ username + " about job " + jobid + " event of " + event.toString());
				mailFailures.inc();
			}
		}
	 }
	
	public ControllerServer(Configuration _hconf) throws IOException
	{
		Constants.constantsVerification();
		this.yConf = new YarnConfiguration(_hconf);
		yConf.addResource("yarn-site.xml");
		UserGroupInformation.setConfiguration(yConf);
		
		this.pConf = new PConfiguration(_hconf);
		
		//do the Kerberos authentication
		if (UserGroupInformation.isSecurityEnabled())
		{
			final String principal = pConf.get(PConfiguration.KEY_CONTROLLER_PRINCIPAL);
			String keytab = pConf.get(PConfiguration.KEY_CONTROLLER_KEYTAB);
			File fKeytab = new File(keytab);
			if (! fKeytab.exists())
			{
				if (! fKeytab.isAbsolute())
				{
					keytab = System.getProperty("probos.conf") + '/' + keytab;
					fKeytab = new File(keytab);
					pConf.set(PConfiguration.KEY_CONTROLLER_KEYTAB, keytab);
				}
				if (! fKeytab.exists())
					throw new FileNotFoundException("Could not find keytab file " + keytab);				
			}			
			
			LOG.debug("Starting login for " + principal + " using keytab " + keytab);
			SecurityUtil.login(pConf, PConfiguration.KEY_CONTROLLER_KEYTAB,
					PConfiguration.KEY_CONTROLLER_PRINCIPAL, Utils.getHostname());
			LOG.info("Switched principal to " + UserGroupInformation.getCurrentUser().getUserName());
		}
		
		this.mClient = MailClient.getMailClient(this.pConf);
		final String bindAddress = pConf.get(PConfiguration.KEY_CONTROLLER_BIND_ADDRESS);
		if (bindAddress == null)
			throw new IllegalArgumentException(PConfiguration.KEY_CONTROLLER_BIND_ADDRESS + " cannot be null");
		
		secretManager = new ControllerAPISecretManager(
				//delegationKeyUpdateInterval
				//renewal interval for delegation token
				7 * 24 * 3600 *1000, //Yarn default is 7 day
				
				//delegationTokenMaxLifetime -- maximum lifetime for which a delegation token is valid
				//i.e. how long can we keep renewing the token for?
				14 * 24 * 3600 *1000, //Yarn default is 14 days
				
				//delegationTokenRenewInterval -- how long should a token last?
				7 * 24 * 3600 *1000, //Yarn default is 7 day
				
				//delegationTokenRemoverScanInterval -- how often are expired keys removed?
				3600 *1000); //Yarn default is 1 hour
		
		//build the client rpc server: 8027
		int port = pConf.getInt(PConfiguration.KEY_CONTROLLER_PORT, 8027);
		LOG.info("Starting RPC server for "+ PBSClient.class.getSimpleName() + " on port " + port);
		clientRpcserver = new RPC.Builder(yConf).
                setInstance(this).
                setBindAddress(bindAddress).
                setProtocol(PBSClient.class).
                setPort(port).
                setSecretManager(secretManager).
               //setVerbose(true).
                build();
		System.setProperty("hadoop.policy.file", Constants.PRODUCT_NAME + "-policy.xml");
		clientRpcserver.refreshServiceAclWithLoadedConfiguration(yConf, new ControllerPolicyProvider());
			
		
		//build the master rpc server: 8028
		port = Constants.CONTROLLER_MASTER_PORT_OFFSET+ pConf.getInt(PConfiguration.KEY_CONTROLLER_PORT, 8027);
		LOG.info("Starting RPC server for "+ PBSMasterClient.class.getSimpleName() + " on port " + port);
		masterRpcserver = new RPC.Builder(yConf).
                setInstance(new ApplicationMasterAPI()).
                setBindAddress(bindAddress).
                setProtocol(PBSMasterClient.class).
                setPort(port).
                setSecretManager(secretManager).
               //setVerbose(true).
                build();
		masterRpcserver.refreshServiceAclWithLoadedConfiguration(yConf, new ControllerPolicyProvider());
		
		
		port = Constants.CONTROLLER_INTERACTIVE_PORT_OFFSET+ pConf.getInt(PConfiguration.KEY_CONTROLLER_PORT, 8027);
		LOG.info("Starting RPC server for "+ PBSInteractiveClient.class.getSimpleName() + " on port " + port);
		//build the interactive rpc server: 8026
		interactiveRpcserver = new RPC.Builder(yConf).
            setInstance(new InteractiveTaskAPI()).
            setBindAddress(bindAddress).
            setProtocol(PBSInteractiveClient.class).
            setPort(port).
            setSecretManager(secretManager).
            //setVerbose(true).
            build();
		interactiveRpcserver.refreshServiceAclWithLoadedConfiguration(yConf, new ControllerPolicyProvider());

		
		//build the webapp UI server
		final List<Entry<String,HttpServlet>> controllerServlets = new ArrayList<>();
		controllerServlets.add(new MapEntry<String,HttpServlet>("/", new QstatServlet("/", controllerServlets, this)));
		controllerServlets.add(new MapEntry<String,HttpServlet>("/version", new VersionServlet("/", controllerServlets, this)));
		controllerServlets.add(new MapEntry<String,HttpServlet>("/pbsnodes", new PbsnodesServlet("/", controllerServlets, this)));
		//metrics is the Servlet from metrics.dropwizard for accessing metrics
		controllerServlets.add(new MapEntry<String,HttpServlet>("/metrics", new MetricsServlet(metrics)));
		//this is the hadoop servlet for accessing anything defined in JMX
		controllerServlets.add(new MapEntry<String,HttpServlet>("/jmx", new JMXJsonServlet()));
		final int httpport = pConf.getInt(PConfiguration.KEY_CONTROLLER_HTTP_PORT, Constants.DEFAULT_CONTROLLER_PORT+Constants.CONTROLLER_HTTP_PORT_OFFSET);		
		LOG.info("Starting Jetty ProbosControllerHttp on port " + httpport);
		webServer = new WebServer("ProbosControllerHttp", controllerServlets, httpport);
		webServer.init(pConf);
				
		//this thread detects yarn jobs that have ended
		watcherThread = new Thread(new ControllerWatcher());
		watcherThread.setName(ControllerWatcher.class.getSimpleName());
		
		//ensure we have the directory
		Path _probosFolder = new Path(pConf.get(PConfiguration.KEY_CONTROLLER_JOBDIR));
		FileSystem controllerFS = FileSystem.get(yConf);
		if (! _probosFolder.isUriPathAbsolute())
		{
			_probosFolder = _probosFolder.makeQualified(controllerFS.getUri(), controllerFS.getWorkingDirectory());
			assert _probosFolder.isUriPathAbsolute();
		}
		probosFolder = _probosFolder;
		if (! controllerFS.exists( probosFolder ))
		{
			throw new IllegalArgumentException(probosFolder.toString() + " does not exist");
		}
		
		//now initialise the metrics
		
		//jobs.queued.size
		metrics.register(MetricRegistry.name(ControllerServer.class, "jobs", "queued.size"),
			new Gauge<Integer>() {
				@Override
				public Integer getValue() {
					int sum = 0;
					for(int i :  user2QueuedCount.values())
						sum += i;
					return sum;
				}
			});
		//jobs.size
		metrics.register(MetricRegistry.name(ControllerServer.class, "jobs", "size"),
			new Gauge<Integer>() {
				@Override
				public Integer getValue() {
					return jobArray.size();
				}
			});
		//jobs.held.size
		metrics.register(MetricRegistry.name(ControllerServer.class, "jobs", "held.size"),
			new Gauge<Integer>() {
				@Override
				public Integer getValue() {
					return jobHolds.size();
				}
			});
		
		//nodes.size
		metrics.register(MetricRegistry.name(ControllerServer.class, "nodes", "size"),
				new Gauge<Integer>() {
					@Override
					public Integer getValue() {
						try{
							return getNodesStatus().length;
						} catch (Exception e) {
							return 0;
						}
					}
				});
		
		//nodes.free.size
		metrics.register(MetricRegistry.name(ControllerServer.class, "nodes", "free.size"),
				new Gauge<Integer>() {
					@Override
					public Integer getValue() {
						try{
							PBSNodeStatus[] nodes = getNodesStatus();
							int count = 0;
							for(PBSNodeStatus n : nodes)
								if ("free".equals(n.getState()))
									count++;							
							return count;
						} catch (Exception e) {
							return 0;
						}
					}
				});
		
		runningJobs = metrics.counter(MetricRegistry.name(ControllerServer.class, "jobs", "running.counter"));
		rejectedJobs = metrics.counter(MetricRegistry.name(ControllerServer.class, "jobs", "rejected.counter"));
		killedJobs = metrics.counter(MetricRegistry.name(ControllerServer.class, "jobs", "killed.counter"));
		mailEvents = metrics.counter(MetricRegistry.name(ControllerServer.class, "mails", "counter"));
		mailFailures = metrics.counter(MetricRegistry.name(ControllerServer.class, "mails", "failure.counter"));
		
	}
	
	@Override
	protected void doStart() {
		clientRpcserver.start();
		masterRpcserver.start();
		try{
			secretManager.startThreads();
		}catch (IOException ioe) {
			LOG.error("Failed to start secretmanager", ioe);
		}
		interactiveRpcserver.start();		
		LOG.info("Ready to serve RPC requests");
		
		webServer.start();
		LOG.info("Ready to serve HTTP requests at " + webServer.getURI().toString());
		
		watcherThread.start();
		yClient = new YarnClientFactory(yConf).connect();
		jmxMetricReporter = JmxReporter.forRegistry(metrics).build();
		jmxMetricReporter.start();
		notifyStarted();
	}
	
	@Override
	protected void doStop() {
		closeThread(this.watcherThread);
		mClient.close();
		webServer.stop();
		clientRpcserver.stop();
		masterRpcserver.stop();
		secretManager.stopThreads();
		interactiveRpcserver.stop();
		jmxMetricReporter.stop();
		notifyStopped();
	}
	
	private void closeThread(Thread t) {
	    if (t != null && t.isAlive()) {
	      LOG.info("Stopping " + t.getName());
	      t.interrupt();
	      try {
	        t.join();
	      } catch (InterruptedException ex) {
	        ex.printStackTrace();
	      }
	    }
	}

	@Override
	public int submitJob(final PBSJob job, byte[] scriptSource) throws IOException {	
		UserGroupInformation caller = Server.getRemoteUser();
		LOG.info(caller + " submitted a job!");
		
		final String requestorUserName = caller.getShortUserName();
		
		//check for ProBoS queue limits
		final int maxUserQueueable = pConf.getInt(PConfiguration.KEY_JOB_MAX_USER_QUEUE, 5000);
		final int maxQueueable = pConf.getInt(PConfiguration.KEY_JOB_MAX_QUEUE, 10000);
		if (jobArray.size() > maxQueueable)
		{
			rejectedJobs.inc();
			return -1;
		}
		if (user2QueuedCount.get(requestorUserName) > maxUserQueueable)
		{
			rejectedJobs.inc();
			return -1;		
		}
		int newId = nextJobId.incrementAndGet();
		JobInformation ji = new JobInformation(newId, job);
		jobArray.put(newId, ji);
		ji.jobId = newId;
		ji.modify();
		user2QueuedCount.adjustOrPutValue(requestorUserName, 1, 1);
		if (! storeJobScript(ji, requestorUserName, scriptSource))
		{
			jobArray.remove(newId);
			user2QueuedCount.adjustOrPutValue(requestorUserName, -1, 0);
			rejectedJobs.inc();
			return -1;
		}
		
		if (job.getUserHold()) {
			jobHolds.put(newId, new JobHold(HoldType.USER, requestorUserName));
			return newId;
		} else {
			//yarnJob returns the job id on success
			if (yarnJob(ji, requestorUserName) == newId)
			{
				return newId;
			}
			else
			{
				jobArray.remove(newId);
				user2QueuedCount.adjustOrPutValue(requestorUserName, -1, 0);
				rejectedJobs.inc();
				return -1;
			}
		}
		
	}
	
	protected boolean storeJobScript(final JobInformation ji, final String requestorUserName, final byte[] source) throws IOException
	{
		final String jobFolderName = String.valueOf(Math.abs(random.nextInt()));
		
		final Path jobFolder = new Path(probosFolder, jobFolderName);
		final Path script = new Path(probosFolder,jobFolderName + ".SC");
		PrivilegedExceptionAction<Path> submitAction = new PrivilegedExceptionAction<Path>() {
		  public Path run() throws Exception {
			  FileSystem fs = FileSystem.get(yConf);			  
			  fs.mkdirs(jobFolder);
			  OutputStream os = fs.create(script);
			  os.write(source);
			  os.close();
			  LOG.info("Wrote " + source.length + " bytes to " + script.toString() + " as the job script for job "+ ji.jobId);
			  return script;
		  }
		};
		
		//setuid to the requestor's user id
		UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(requestorUserName, UserGroupInformation.getLoginUser());
		Path rtr = null;
		try{
			if (UserGroupInformation.isSecurityEnabled())
				rtr = proxyUser.doAs(submitAction);
			else
				rtr = submitAction.run();
			ji.proxyUser = proxyUser;
			ji.scriptLocation = rtr;
			ji.folderLocation = jobFolder;
			ji.modify();
			return true;
		} catch (Exception e) {
			LOG.error("Could not store job file!", e);
			return false;
		}
	}
	
	protected int yarnJob(final JobInformation ji, final String requestorUserName) throws IOException
	{
		assert ji.scriptLocation != null;
		assert ji.folderLocation != null;
		final PBSJob job = ji.jobSpec;
		PrivilegedExceptionAction<Integer> submitAction = new PrivilegedExceptionAction<Integer>() {
			  public Integer run() throws Exception {
				File luaFile = writeJobKittenSpec(job, ji.scriptLocation, ji.jobId, false);
				Configuration kConf = new Configuration(yConf);
				kConf.set(LocalDataHelper.APP_BASE_DIR, ji.folderLocation.toUri().toString());
				YarnClientParameters params = new LuaYarnClientParameters(
						luaFile.toString(), Constants.PRODUCT_NAME, kConf, 
						extraLuaValues, extraLocalResources);
				ji.jobSpec.setQueue(params.getQueue());
				
				
				Credentials creds = new Credentials();				
				
				//create delegation tokens
				//interactive rpc
				InetSocketAddress addr = NetUtils.getConnectAddress(interactiveRpcserver);
				Text host = new Text(addr.getAddress().getHostAddress() + ":"
					        + addr.getPort());
				ProbosDelegationTokenIdentifier tokenId = secretManager.createIdentifier(); 
				Token<ProbosDelegationTokenIdentifier> delgationToken = new Token<ProbosDelegationTokenIdentifier>(tokenId, secretManager);
				delgationToken.setService(host);
				creds.addToken(host, delgationToken);
				LOG.info("Interactive: Generated token for " + creds.toString() + " : " + delgationToken);
				
				//client rpc
				tokenId = secretManager.createIdentifier(); 
				delgationToken = new Token<ProbosDelegationTokenIdentifier>(tokenId, secretManager);
				addr = NetUtils.getConnectAddress(clientRpcserver);
				host = new Text(addr.getAddress().getHostAddress() + ":"
					        + addr.getPort());				
				delgationToken.setService(host);
				creds.addToken(host, delgationToken);
				LOG.info("Client: Generated token for " + creds.toString() + " : " + delgationToken);
				
				//master rpc
				tokenId = secretManager.createIdentifier(); 
				delgationToken = new Token<ProbosDelegationTokenIdentifier>(tokenId, secretManager);
				addr = NetUtils.getConnectAddress(masterRpcserver);
				host = new Text(addr.getAddress().getHostAddress() + ":"
					        + addr.getPort());				
				delgationToken.setService(host);
				creds.addToken(host, delgationToken);
				LOG.info("Master: Generated token for " + creds.toString() + " : " + delgationToken);
				
				
				YarnClientService service = new YarnClientServiceImpl(params, creds)
				{
					@Override
					protected void submitApplication(ApplicationSubmissionContext appContext) {
						 LogAggregationContext logAggregationContext = Records.newRecord(LogAggregationContext.class);
						 if (! VersionInfo.getVersion().startsWith("2.6") )
						 {
							 try{
							 logAggregationContext
							 	.getClass()
							 	.getMethod("setRolledLogsExcludePattern", new Class[]{String.class})
							 	.invoke(logAggregationContext, "stderr|stdout");
							 } catch (Exception e) {
								 LOG.warn("Could not use setRolledLogsExcludePattern", e);
							 }
						 }
						 appContext.setLogAggregationContext(logAggregationContext);
						 super.submitApplication(appContext);
					}					
				};
				service.startAndWait();
				if (!service.isRunning()) {
				     LOG.error("YarnClientService failed to startup, exiting...");
				     jobArray.remove(ji.jobId);
				     return ji.jobId;
				}
				ji.kitten = service;
				ji.modify();
				return ji.jobId;
			  }
		};
		//setuid to the requestor's user id
		UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(requestorUserName, UserGroupInformation.getLoginUser());
		Integer rtr = null;
		try{
			if (UserGroupInformation.isSecurityEnabled())
				rtr = proxyUser.doAs(submitAction);
			else
				rtr = submitAction.run();
			ji.proxyUser = proxyUser;
			ji.modify();
			runningJobs.inc();
			return rtr.intValue();
		} catch (Exception e) {
			LOG.error("job did not submit!", e);
			return -1;
		}
		
	}
	
	@Override
	public String kittenSpecification(PBSJob job, boolean luaHeaders, boolean noProbos) throws IOException
	{
		try {
			
			File f = writeJobKittenSpec(job, new Path("1.SC"), 1, noProbos);	
			String[] contents = Utils.slurpString(f);
			f.delete();
			StringBuilder s = new StringBuilder();
			
			if (luaHeaders)
			{
				BufferedReader br = new BufferedReader(new InputStreamReader(
						this.getClass().getResourceAsStream("/lua/kitten.lua")));
				String line;
				while((line = br.readLine()) != null)
				{
					s.append(line);
					s.append('\n');
				}
				s.append('\n');
			}
				
			for(String line : contents){
				s.append(line);
				s.append('\n');
			}
			return s.toString();			
		} catch (Exception e) {
			return e.toString();
		}
		
	}
	
	protected File writeJobKittenSpec(final PBSJob job, final Path targetScript, int newId, boolean noProBoS)
			throws IOException {
		File luaFile = File.createTempFile("kpbs-job" + String.valueOf(newId), ".lua");
		KittenUtils2 ku = KittenUtils2.createKittenUtil(pConf, job, newId);
		ku.setProbosMasterStatus(noProBoS);
		ku.writeKJobToFile(targetScript, luaFile);
		return luaFile;
	}
	
	/** Kills the specified job. 
	 * @param jobId id of the job to be killed
	 * @return 0 for success, -1 for no such job, -2 for job could not be killed
	 * @throws Exception
	 */
	@Override
	public int killJob(final int jobId, boolean purge) throws Exception {
		UserGroupInformation caller = Server.getRemoteUser();
		LOG.info(caller + " asked to kill job " + jobId);
		if (! jobArray.containsKey(jobId))
			return -1;
		
		final JobInformation ji = jobArray.get(jobId);
		checkOwnerOrRoot(ji);
		UserGroupInformation proxyUser = ji.proxyUser;
		Integer status;
		PrivilegedExceptionAction<Integer> doKill = new PrivilegedExceptionAction<Integer>() {
			 public Integer run() throws Exception {
				final long kill_deadline = System.currentTimeMillis() + pConf.getLong(PConfiguration.KEY_CONTROLLER_KILL_TIMEOUT, 5000);
				
				YarnClientService kittenClient = ji.kitten;
				YarnClient yarnClient = YarnClient.createYarnClient();
				yarnClient.init(yConf);
				yarnClient.start();
				yarnClient.killApplication(kittenClient.getApplicationId());				
				while(! kittenClient.isApplicationFinished())
				{
					Thread.sleep(100);
					if (System.currentTimeMillis() > kill_deadline)
						return -2;
				}
				return 0;
			 }
		};
		//perform the actual kill, as the user
		if (UserGroupInformation.isSecurityEnabled())
			status = proxyUser.doAs(doKill);
		else 
			status = doKill.run();
		runningJobs.dec();
		killedJobs.inc();
		//purge, aka qdel -p.
		//conditional on superuser
		if (purge)
		{
			jobArray.remove(jobId);
			status = 0;
		}
		return status;
	}

	@Override
	public PBSJob getJob(int jobId) {
		return jobArray.get(jobId).jobSpec;
	}

	@Override
	public int[] getJobs() {
		return jobArray.keys();
	}
	
	
	@Override 
	public int releaseJob(int jobid) {
		JobHold jh = jobHolds.get(jobid);
		if (jh == null)
			return -1;
		if (jh.type == HoldType.DEPENDENCY)
			return -2;
		int status = -1;
		try{
			status = yarnJob(jobArray.get(jobid), jh.owner);
		} catch (Exception e) {
			LOG.warn("Could not submit held job " + jobid + " to YARN", e);
		}
		if (status == jobid)
		{
			jobHolds.remove(jobid);
			jobArray.get(jobid).jobSpec.setUserHold(false);
			jobArray.get(jobid).mtime = System.currentTimeMillis();
			return 0;
		}
		return -3;
	}
	
	
	@Override
	public PBSJobStatusLight getJobStatus(int jobId, int requestType) throws Exception {
		
		if (requestType > 5 || requestType < 0 )
			throw new IllegalArgumentException("requestType must be [0,1,2,3,4,5]");
		
		char state = '*';
		
		if (! jobArray.containsKey(jobId))
			state = '?';
		
		final JobInformation ji = jobArray.get(jobId);
		final PBSJob job = ji != null ? ji.jobSpec : null;
		YarnClientService kittenClient = ji != null ? ji.kitten : null;
		ApplicationReport appReport = null;		
		
		if (kittenClient == null || (appReport = kittenClient.getApplicationReport()) == null)
		{
			state = '?';
			if (jobHolds.get(jobId) != null)
			{
				state = 'H';
			}
		}
		else
		{	
			YarnApplicationState appState = appReport.getYarnApplicationState();
			if (kittenClient.isApplicationFinished())
				state = 'E';
			else	
				switch (appState) {
				case NEW:
				case NEW_SAVING:
				case ACCEPTED:
				case SUBMITTED:
					state = 'Q';
					break;
				case FAILED:
				case KILLED:
				case FINISHED:
					state = 'E';
					break;
				case RUNNING:
					state = appReport.getApplicationResourceUsageReport().getNumUsedContainers() > 1
							? 'R' //running
							: 'S'; //at least a master container has started - so "Started"/"Suspended"
					break;
				default:
					state = '?';
					break;				
				}
		}
		String timeUse = appReport == null
				? "0"
				: Utils.makeTime( appReport.getApplicationResourceUsageReport().getVcoreSeconds() );
		
		String appURL = appReport == null
				? ""
				: appReport.getTrackingUrl();
		
		PBSJobStatusLight rtr = null;
		String nodes = null;
		List<ContainerReport> cReports = null;
		String appId = null;
		
		if (requestType == 0)
		{
			rtr = new PBSJobStatusLight(jobId, 
					job!= null ? job.getArrayTaskIds() != null : false,
					job!= null ? job.getJob_Name() : null, 
					job!= null ? job.getJob_Owner() : null, 
					timeUse, 
					state, 
					job!= null ? job.getQueue() : null,
					appURL
					);
		}
		else if (requestType == 4)
		{
			checkOwnerOrRoot(ji);
			JobInteractiveInfo jii = ji != null ? ji.interactive : null;
			rtr = new PBSJobStatusInteractive(jobId, 
					job!= null ? job.getArrayTaskIds() != null : false,
					job!= null ? job.getJob_Name() : null, 
					job!= null ? job.getJob_Owner() : null,
					timeUse, state, 
					job!= null ? job.getQueue() : null, 
					appURL,
					jii != null ? jii.hostname : null, 
					jii != null ? jii.port : -1 , 
					jii != null ? jii.secret : null);
		}
		else if (requestType == 5)
		{
			checkOwnerOrRoot(ji);
			JobDistributedInfo jid = ji != null ? ji.distributed : null;
			String secret = jid != null ? jid.secret : null;
			String[] hostnames = jid!=null ? jid.hostnames.toArray(new String[0]) : null;
			int[] ports = jid!=null ? jid.ports.toArray() : null;
			
			rtr = new PBSJobStatusDistributed(jobId, 
					job!= null ? job.getArrayTaskIds() != null : false,
					job!= null ? job.getJob_Name() : null, 
					job!= null ? job.getJob_Owner() : null,
					timeUse, state, 
					job!= null ? job.getQueue() : null, 
					appURL,
					hostnames, 
					ports, 
					secret);
		}
		//we need the nodes also
		else if (requestType >= 1)
		{
			if (kittenClient != null)
			{
				ApplicationId aid = kittenClient.getApplicationReport().getApplicationId();
				appId = aid.toString();
				List<ApplicationAttemptReport> aaids = yClient.getApplicationAttempts(aid);
				ApplicationAttemptId aaid = aaids.get(aaids.size() -1).getApplicationAttemptId();
				cReports = yClient.getContainers(aaid);
				StringBuilder sNodes = new StringBuilder();
				if (cReports.size() > 0)
				{
					for(ContainerReport cReport : cReports)
					{
						if (cReport.getContainerState() == ContainerState.RUNNING)
						{
							sNodes.append(cReport.getAssignedNode().getHost());
							sNodes.append("+");
						}
					}
					//remove trailing ,
					sNodes.setLength(sNodes.length()-1);
				}
				nodes = sNodes.toString();
			}
			if (requestType == 1)
			{
				rtr = new PBSJobStatusNodes(jobId, 
						job.getArrayTaskIds() != null,
						job!= null ? job.getJob_Name() : null, 
						job!= null ? job.getJob_Owner() : null, 
						timeUse, 
						state, 
						job!= null ? job.getQueue() : null,
						appURL,
						nodes
						);
			}
			else if (requestType == 2) {
				
				String[] tContainers;
				if (job != null)
				{
					tContainers = job.getArrayTaskIds() != null 
						? ji.array2Container.values(new String[0])
						: new String[]{ji.taskContainerId};
				}
				else
				{
					tContainers = new String[0];
				}
				
				String trackingURL = appReport != null ? appReport.getTrackingUrl() : null;
						
				rtr = new PBSJobStatusFat(jobId, 
						job!= null ? job.getArrayTaskIds() != null : false,
						job!= null ? job.getJob_Name() : null, 
						job!= null ? job.getJob_Owner() : null, 
						timeUse, 
						state, 
						job!= null ? job.getQueue() : null,
						nodes,
						ji!= null ? ji.jobSpec : null,
						ji!= null ? ji.masterContainerId: null,
						tContainers, trackingURL, appId
						);
			} else if (requestType == 3) {
				int[] arrayIds = job != null
						? JobUtils.getTaskArrayItems(job.getArrayTaskIds())
						: new int[0];
				if (arrayIds == null)
					arrayIds = new int[0];
				char[] states = new char[arrayIds.length];
				//String[] walltime = new String[arrayIds.length];
				int i=0;
				for(int arid : arrayIds)
				{
					String containerStatus = ji.array2Container.get(arid);
					if (containerStatus == null)
						states[i] = 'Q';
					else if (containerStatus.equals("DONE"))
						states[i] = 'C';
					else if (containerStatus.equals("ABORTED"))
						states[i] = 'C';
					else {
						 states[i] = 'R';
//						 ContainerId c = ContainerId.fromString(containerStatus);
//						 for(ContainerReport cReport : cReports)
//						 {
//							if (cReport.getContainerId().equals(c)
//							{
//								walltime[i] = cReport.
//							}
					}	
					i++;
				}
				
				rtr = new PBSJobArrayStatusLight(jobId, 
						job!= null ? job.getJob_Name() : null, 
						job!= null ? job.getJob_Owner() : null, 
						timeUse, 
						state, 
						job!= null ? job.getQueue() : null,
						appURL,
						arrayIds,
						states
						);
			} else { //this should not be reached.
				throw new IllegalArgumentException("Bad requestType");
			}
		}
		return rtr;		
	}
	
	private TIntObjectHashMap<List<ContainerId>> getAllActiveContainers()
	{
		TIntObjectHashMap<List<ContainerId>> rtr = new TIntObjectHashMap<List<ContainerId>>(jobArray.size());
		//
		for(JobInformation ji : jobArray.valueCollection())
		{
			List<ContainerId> containerList = new ArrayList<ContainerId>();
			if (ji.masterContainerId != null && ji.masterContainerId.startsWith("container"))
				containerList.add(ContainerId.fromString(ji.masterContainerId));
			if (ji.taskContainerId != null && ji.taskContainerId.startsWith("container"))
				containerList.add(ContainerId.fromString(ji.taskContainerId));
			for (String arrayItem : ji.array2Container.valueCollection())
				if (arrayItem != null && arrayItem.startsWith("container"))
					containerList.add(ContainerId.fromString(arrayItem));
			if (containerList.size() > 0)
				rtr.put(ji.jobId, containerList);
		}
		return rtr;
	}
	
	protected void checkOwnerOrRoot(JobInformation ji) throws Exception {
		if (ji == null)
			return;// you can do what you want if there is no job to act upon it
		UserGroupInformation caller = Server.getRemoteUser();
		if (ji != null)
		{
			//craigm@AD.DCS.GLA.AC.UK (auth:KERBEROS) denied access, 
			//expected craigm (auth:PROXY) via probos/salt@DCS.GLA.AC.UK (auth:KERBEROS)
			//we just check that shortusername match
			if (! ji.proxyUser.getShortUserName().equals(caller.getShortUserName()))
			{
				SecurityException se = new SecurityException("No permission to access this information");
				LOG.warn(caller.toString() + " denied access, job owner was " + ji.proxyUser.toString(), se);
				throw se;
			}
		}
	}
	
	@Override
	public PBSNodeStatus[] getNodesStatus() throws Exception {
		
		//first use the container reports of all running jobs to get a picture of the hosts in use
		//for each job
		TIntObjectHashMap<List<ContainerId>> job2con = getAllActiveContainers();
		final Map<String,TIntArrayList> node2job = new HashMap<String,TIntArrayList>();
		job2con.forEachEntry(new TIntObjectProcedure<List<ContainerId>>() {
			@Override
			public boolean execute(int jobId, List<ContainerId> containerList) {
				for(ContainerId cid : containerList) {
					try {
						ContainerReport cr = yClient.getContainerReport(cid);
						String hostname = cr.getAssignedNode().getHost();
						
						TIntArrayList jobs = node2job.get(hostname);
						if (jobs == null)
							node2job.put(hostname, jobs = new TIntArrayList());
						jobs.add(jobId);
					} catch (Exception e) {
						LOG.warn("Could not getContainerReport", e);
					}					
				}
				return true;
			}
		});
//		System.err.println(job2con);
//		System.err.println(node2job);
		
		
		List<NodeReport> nodeReports = yClient.getNodeReports();
		Collections.sort(nodeReports, new Comparator<NodeReport>()
		{
			@Override
			public int compare(NodeReport o1, NodeReport o2) {
				return o1.getNodeId().getHost().compareTo(o2.getNodeId().getHost());
			}			
		});
		
		PBSNodeStatus[] rtr = new PBSNodeStatus[nodeReports.size()];
		for (int i=0;i<rtr.length;i++)
		{
			final NodeReport node = nodeReports.get(i);
			String hostname = node.getNodeId().getHost();
			String yarnState = node.getNodeState().toString();
			
			String rack = node.getRackName();
			String tracker = node.getHttpAddress();
			int numContainers = node.getNumContainers();
			int numProcs = node.getCapability().getVirtualCores();
			TIntArrayList jobList = node2job.get(hostname);
			int[] jobs;
			if (jobList == null)
				jobs = new int[0];
			else
				jobs = jobList.toArray();
			String state = "free";
			if (numContainers >= numProcs)
				state = "busy";
			
			StringBuilder status = new StringBuilder();
			//these emulate the Torque PBS implementation
			status.append("ncpus=" + numProcs);
			status.append(",physmem=" + node.getCapability().getMemory());
			//these are our own formatting
			status.append(",capacity=" + node.getCapability().toString());
			status.append(",used=" + node.getUsed().toString());
			
			rtr[i] = new PBSNodeStatus(
					hostname, state, status.toString(), jobs,
					tracker,node.getHealthReport(),
					rack, yarnState, numProcs, node.getNodeLabels());
		}
		return rtr;
	}
	
	@Override
	public int[] selectJobs(PBSJobSelector[] selectors) throws Exception
	{
		return selectJobs(jobArray.iterator(), selectors, this);		
	}
	
	@VisibleForTesting
	public static int[] selectJobs(TIntObjectIterator<JobInformation> iterator, PBSJobSelector[] selectors, PBSClient server) throws Exception 
	{
		TIntArrayList matchingJobs = new TIntArrayList();
		//for each job, see if it matches all of the passed selectors.
		JOB: while(iterator.hasNext())
		{
			iterator.advance();
			int jobId = iterator.key();
			JobInformation ji = iterator.value();
			for(PBSJobSelector sel : selectors)
			{
				if (! sel.matches(server, jobId, ji.jobSpec))
				{
					continue JOB;
				}
			}
			matchingJobs.add(jobId);
		}
		return matchingJobs.toArray();
	}

	@Override
	public byte[] jobLog(int jobid, int arrayId, boolean stdout, long start, boolean URLonly) throws Exception
	{
		boolean masterRequest = false;
		if (jobid < 0)
		{
			masterRequest = true;
			jobid = -1 * jobid;
		}
		JobInformation ji = jobArray.get(jobid);
		if (ji == null)
			return new byte[0];
		if (ji.kitten == null || ji.taskContainerId == null)
			return new byte[0];
		
		String containerId = null;
		
		//either its a master request, an array request, or the main job task
		if (masterRequest) 
		{
			containerId = ji.masterContainerId;
		}
		else if (ji.jobSpec.getArrayTaskIds() == null)//basic job
		{
			containerId = ji.taskContainerId;
		}
		else
		{
			containerId = ji.array2Container.get(arrayId);
		}
		
		
		if (containerId == null || containerId.equals("DONE") || containerId.equals("ABORTED"))	
		{
			return new byte[0];
		}
		byte[] bytes = new byte[0];
		try{
			ContainerReport cs = yClient.getContainerReport(ContainerId.fromString(containerId));
			String url = cs.getLogUrl() + (stdout ? "/stdout" : "/stderr") + "?start="+start;
			//some hadoop version do not prepend the protocol to the URL. fix, if necessary
			if (! url.startsWith("http"))
				url = "http:" + url;
			
			if (! URLonly) {
				//System.err.println(url);
				InputStream is = new URL(url).openStream();
				bytes = IOUtils.toByteArray(is);
				is.close();
				//convoluted process to re-obtain raw byte form
				//TODO: some wild assumptions about encoding here.
				String htmlPage = new String(bytes);
				htmlPage = REPLACE_PRE_PRE.matcher(htmlPage).replaceAll("");
				htmlPage = REPLACE_POST_PRE.matcher(htmlPage).replaceAll("");
				
				htmlPage.replaceAll(".*<pre>", "").replaceAll("</pre>.*", "");
				htmlPage = StringEscapeUtils.unescapeHtml(htmlPage);
				bytes = htmlPage.getBytes();	
			} else {
				bytes = url.getBytes();
			}
		} catch (ContainerNotFoundException ce) {
			LOG.warn("Too late to get job log for " + containerId);
		} catch (Exception e) {
			LOG.warn("Failed to get job log for " + containerId, e);
		}
		return bytes;
	}
	
	
	static final Pattern REPLACE_PRE_PRE = Pattern.compile("^.*<pre>", Pattern.DOTALL);
	static final Pattern REPLACE_POST_PRE = Pattern.compile("</pre>.*$", Pattern.DOTALL);

	@Override
	public ProtocolSignature getProtocolSignature(String protocol,
		      long clientVersion, int clientMethodsHash) throws IOException {
		return ProtocolSignature.getProtocolSignature(
			this, protocol, clientVersion, clientMethodsHash);
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return PROTOCOL_VERSION;
	}
	
	class InteractiveTaskAPI implements PBSInteractiveClient 
	{
		@Override
		public long getProtocolVersion(String protocol, long clientVersion)
				throws IOException {
			return PROTOCOL_VERSION;
		}

		@Override
		public ProtocolSignature getProtocolSignature(String protocol,
				long clientVersion, int clientMethodsHash) throws IOException {
			return ProtocolSignature.getProtocolSignature(
					this, protocol, clientVersion, clientMethodsHash);
		}

		@Override
		public void interactiveDaemonStarted(int jobid, String hostname, int port, String secret) {
			JobInformation ji = jobArray.get(jobid);
			ji.interactive = new JobInteractiveInfo();
			ji.interactive.hostname = hostname;
			ji.interactive.port = port;
			ji.interactive.secret = secret;
			LOG.info("Received notification of interactive daemon for job "+jobid+" running on "+ hostname + ":" + port + " key "+ secret);
		}
		
		@Override
		public void distributedDaemonStarted(int jobid, String hostname, int port, String secret) {
			JobInformation ji = jobArray.get(jobid);
			ji.distributed.hostnames.add(hostname);
			ji.distributed.ports.add(port);
			ji.distributed.secret = secret;
			LOG.info("Received notification of distributed daemon for job "+jobid+" running on "+ hostname + ":" + port + " key "+ secret);
		}
		
	}
	
	class ApplicationMasterAPI implements PBSMasterClient
	{
		Counter amHeartbeatCounter = metrics.counter(MetricRegistry.name(ApplicationMasterAPI.class, "heartbeat.counter"));
		Counter amJobEventCounter = metrics.counter(MetricRegistry.name(ApplicationMasterAPI.class, "jobevent.counter"));
		Counter amJobEventTerminateCounter = metrics.counter(MetricRegistry.name(ApplicationMasterAPI.class, "jobevent.terminate.counter"));
		Counter amJobArrayEventCounter = metrics.counter(MetricRegistry.name(ApplicationMasterAPI.class, "jobarrayevent.counter"));
		Counter amJobArrayTerminateEventCounter = metrics.counter(MetricRegistry.name(ApplicationMasterAPI.class, "jobarrayevent.terminate.counter"));
		
		@Override
		public long getProtocolVersion(String protocol, long clientVersion)
				throws IOException {
			return PROTOCOL_VERSION;
		}

		@Override
		public ProtocolSignature getProtocolSignature(String protocol,
				long clientVersion, int clientMethodsHash) throws IOException {
			return ProtocolSignature.getProtocolSignature(
					this, protocol, clientVersion, clientMethodsHash);
		}

		@Override
		public void jobEvent(int jobId, EventType e, String containerId, String statusMessage) {
			//renewToken(jobId);
			
			LOG.info("Job=" + jobId + " Event="+e.toString() + " container="+containerId.toString());
			amJobEventCounter.inc();
			
			JobInformation ji = jobArray.get(jobId);
			
			//prevent NPE
			if (ji == null) {
				LOG.warn("Received event for unknown job " + jobId);
				return;
			}
			
			switch(e)
			{
			case MASTER_START:				
				ji.masterContainerId = containerId;
				break;
			case MASTER_END:
				ji.masterContainerId = null;
				ji.interactive = null;
				break;
			case END:
				ji.taskContainerId = "DONE";
				mailEvent(jobId, jobArray.get(jobId).jobSpec, MailEvent.END, null);
				break;
			case START:
				ji.taskContainerId = containerId;
				mailEvent(jobId, jobArray.get(jobId).jobSpec, MailEvent.BEGIN, null);
				break;
			case TERMINATE:
				ji.taskContainerId = "ABORTED";
				String diagnosticMessage = null;
				try{
					diagnosticMessage = yClient.getContainerReport(ContainerId.fromString(containerId)).getDiagnosticsInfo();
					LOG.info("containerId="+containerId + " terminated for job "+jobId+"; diagnostic was " + 
						diagnosticMessage);
				} catch (Exception ex) {
					LOG.warn("Failed to get a diagnostic message", ex);
				}
				mailEvent(jobId, jobArray.get(jobId).jobSpec, MailEvent.ABORT, diagnosticMessage);
				amJobEventTerminateCounter.inc();
				break;
			default:
				break;			
			
			}
			ji.modify();
		}

		@Override
		public void jobArrayTaskEvent(int jobId, int arrayId, EventType e,
				String containerId, String message) {
			
			//renewToken(jobId);
			
			LOG.info("Job=" + jobId + " Array="+arrayId+" Event="+e.toString() + " container="+containerId.toString()  + " message=" + message);
			amJobArrayEventCounter.inc();
			JobInformation ji = jobArray.get(jobId);
			
			//prevent NPE
			if (ji == null) {
				LOG.warn("Received event for unknown job " + jobId);
				return;
			}
			
			switch(e)
			{
				case END:
					ji.array2Container.put(arrayId, "DONE");
					mailEvent(jobId, ji.jobSpec, MailEvent.END, null);
					break;
				case START:
					ji.array2Container.put(arrayId, containerId);
					mailEvent(jobId, ji.jobSpec, MailEvent.BEGIN, null);
					break;
				case TERMINATE:
					ji.array2Container.put(arrayId, "ABORTED");
					mailEvent(jobId, ji.jobSpec, MailEvent.ABORT, null);
					amJobArrayTerminateEventCounter.inc();
					break;
				default:
					break;
			}
			ji.modify();
		}

		@Override
		public int getDistributedHostCount(int jobId) {
			
			//renewToken(jobId);
			JobInformation ji = jobArray.get(jobId);
			
			//prevent NPE
			if (ji == null) {
				LOG.warn("getDistributedHostCount for unknown job " + jobId);
				return -1;
			}
			
			return ji.distributed.hostnames.size();
		}
		
		@Override
		public long heartbeat(int jobId, Token<ProbosDelegationTokenIdentifier> token) throws Exception {
			LOG.info("Master heartbeat received from " + jobId);
			amHeartbeatCounter.inc();
			return renewToken(jobId, token);
		}
		
		protected long renewToken(int jobId, Token<ProbosDelegationTokenIdentifier> token)
				throws Exception
		{
			JobInformation ji = jobArray.get(jobId);
			
			final long newExpiry = secretManager.renewToken(
					token, 
					ji.proxyUser.getUserName());
			LOG.info("Renewed token " + token.toString() + ", new expiry="+newExpiry );
			
			return newExpiry;
//			try{
//				int count = 0;
//				//TODO security check - is the owner the same as the owner of the job?
//				
//				LOG.warn("Server.getRemoteUser()="+ Server.getRemoteUser().toString());
//				LOG.warn("UserGroupInformation.getCurrentUser()="+ UserGroupInformation.getCurrentUser().toString());
//				
//				UserGroupInformation currentUser = Server.getRemoteUser();
//						//UserGroupInformation.getCurrentUser();
//				Collection<Token<? extends TokenIdentifier>> toks = currentUser.getTokens();
//				LOG.debug("Current user has " + toks.size() + " tokens");
//				for(Token<? extends TokenIdentifier> tok : toks)
//				{
//					LOG.debug("Checking if " + tok.toString() + " should be renewed");
//					if (tok.getKind().equals(ProbosDelegationTokenIdentifier.KIND_NAME)){
//						secretManager.renewToken(
//								(Token<ProbosDelegationTokenIdentifier>) tok, 
//								currentUser.getUserName());
//						count++;
//					}
//				}
//				LOG.info("Renewed "+ count + " " + ProbosDelegationTokenIdentifier.KIND_NAME + " tokens");
//			} catch (Exception e) {
//				LOG.warn("Problem renewing token", e);
//			}
		}

		@Override
		public String[] getJobOutputFiles(int jobId) {
			
			String outFile = jobArray.get(jobId).jobSpec.getOutput_Path();
			String errFile = jobArray.get(jobId).jobSpec.getError_Path();
			String RCP = pConf.get(PConfiguration.KEY_RCP);
			String[] rtr = new String[]{outFile,errFile, RCP};
			
			//we need to check if CP can be used fine for these paths.
			for(int i=0;i<2;i++)
			{
				String path = rtr[i].split(":", 2)[1];
				if (KittenUtils2.detectUseCp(pConf, path))
					rtr[i] = path;
			}
			
			return rtr;
		}
		
		@Override
		public String[] getJobArrayOutputFiles(int jobId, int arrayId) {
			String[] rtr = this.getJobOutputFiles(jobId);
			rtr[0] += "-" + arrayId;
			rtr[1] += "-" + arrayId;
			return rtr;
		}
		
	}
	
	class ControllerWatcher implements Runnable
	{
		boolean run = true;
		@Override
		public void run() {
			while(this.run)
			{			
				jobArray.forEachEntry(new TIntObjectProcedure<JobInformation>() {
	
					@Override
					public boolean execute(final int jobId, final JobInformation ji) {
						YarnClientService service = ji.kitten;
						if (service == null)
							return true;
						if (service.isRunning())
						{
							LOG.debug("Job " + jobId + " containers="+service.getApplicationReport().getApplicationResourceUsageReport().getNumUsedContainers()+"  progress=" + service.getApplicationReport().getProgress());
						} else {
							ApplicationReport report = service.getFinalReport();
							LOG.info("Job "+jobId+" finished: " + report);
							jobArray.get(jobId).modify();
							jobArray.remove(jobId);
							UserGroupInformation owner = ji.proxyUser;
							user2QueuedCount.adjustOrPutValue(owner.getShortUserName(), -1, 0);
							switch (report.getFinalApplicationStatus())
							{
							case FAILED:								
							case KILLED:
								mailEvent(jobId, ji.jobSpec, MailEvent.ABORT, null);
								break;
							case SUCCEEDED:
								mailEvent(jobId, ji.jobSpec, MailEvent.END, null);
								break;
							case UNDEFINED:
								LOG.warn("Unknown final application stated for " + jobId + " app " + report.getApplicationId());
								break;
							default:
								break;
							
							}
							try {
								owner.doAs(new PrivilegedExceptionAction<Object>() {
									@Override
									public Object run() throws Exception {
										FileSystem fs = FileSystem.get(yConf);
										if (ji.folderLocation != null)
										{
											LOG.info("Cleanup for job " + jobId + " deleting recursively " + ji.folderLocation);
											fs.delete(ji.folderLocation, true);
										}
										if (ji.scriptLocation != null)
										{
											LOG.info("Cleanup for job " + jobId + " deleting " + ji.scriptLocation);
											fs.delete(ji.scriptLocation, false);
										}
										return null;
									}
								});
							} catch (Exception e) {
								LOG.warn("Problem cleaning up job "+jobId, e);
							}
						}	
						return true;
					}
				});
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					LOG.debug("Interrupted while sleeping", e);
					run = false;
				}
			}
		}		
	}
	

	public static void main(String[] args) throws IOException {
		Configuration c = new Configuration();
		ControllerServer cs = null;
		try{
			cs = new ControllerServer(c);
			cs.start();
		} catch (Exception e) {
			e.printStackTrace();
			if (cs != null)
				cs.stop();
		}
	}

	

	
	
}
