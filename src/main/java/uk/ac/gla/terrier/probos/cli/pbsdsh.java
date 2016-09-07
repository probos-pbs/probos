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

package uk.ac.gla.terrier.probos.cli;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.sshd.client.ClientBuilder;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ChannelExec;
import org.apache.sshd.client.channel.ClientChannel.ClientChannelEvent;
import org.apache.sshd.client.future.ConnectFuture;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.util.io.NoCloseOutputStream;

import uk.ac.gla.terrier.probos.PBSClientFactory;
import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.api.PBSJobStatusDistributed;

/** This implementation is pretty basic at present */
public class pbsdsh extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(pbsdsh.class);
	private PBSClient c;
	
	public pbsdsh(PBSClient _c) throws IOException
	{
		this.c = _c;
	}
	
	public pbsdsh() throws IOException
	{
		this(PBSClientFactory.getPBSClient());
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        LOG.debug("Executing with tokens:");
        while (iter.hasNext()) {
          Token<?> token = iter.next();
          LOG.debug(token.toString());
          if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
            iter.remove();
          }
        }
		
		Options options = new Options();
		options.addOption("h", true, "Specify hostname.");
		CommandLineParser parser = new GnuParser();
		CommandLine cmd = parser.parse( options, args);
		final String[] dibcommand = cmd.getArgs();
		
		int jobId = getJobId();
		if (jobId == -1)
		{
			System.err.println("PBS: PBS_JOBID not set");
			return 1;
		}
		String[] hosts;
		
		
		PBSJobStatusDistributed d = (PBSJobStatusDistributed) c.getJobStatus(jobId, 5);
		if (d.getState() == '?')
		{
			System.err.println("PBS: Job " + jobId + " was lost");
			return -1;
		}
		if (d.getState() != 'R')
		{
			System.err.println("PBS: Job " + jobId + " was not running");
			return -1;
		}
		
		int[] ports;
		if (cmd.hasOption('h'))
		{
			hosts = cmd.getOptionValues('h');
			ports = new int[hosts.length];
			String[] tmpH = d.getHostnames();
			int[] tmpP = d.getPorts();
			TObjectIntHashMap<String> host2port = new TObjectIntHashMap<String>(tmpH.length);
			for(int i=0;i<tmpH.length;i++)
			{
				host2port.put(tmpH[i], tmpP[i]);
			}
			int i=0;
			for(String h : hosts)
			{
				if (! host2port.contains(h))
				{
					throw new IllegalArgumentException("Host "+ h + " is not a member of this distributed job");
				}
				ports[i++] = host2port.get(h);
			}
			
		} else {					
			hosts = d.getHostnames();
			ports = d.getPorts();
		}

		
		
		final String secret = d.getSecret();
		if (secret == null)
			throw new IllegalArgumentException("No secret found - pbsdsh called too early? " + Arrays.toString(d.getHostnames()));
				
		LOG.debug("To run on " + Arrays.toString(hosts));
		final CyclicBarrier barrier = new CyclicBarrier(1+hosts.length);
		int i=0;
		for(final String h : hosts)
		{
			final int port = ports[i++];
			new Thread() { 
				@Override
				public void run() {
					try{
						if (connectToSister(h, port, secret, dibcommand) != 0)
							LOG.error("Could not connect");
					} catch (Exception e) {
						LOG.error("Could not connect", e);
					} finally {
						try {
							barrier.await();
						} catch (Exception e) {
							LOG.error("Barrier problem?");
						}
					}
				}			
			}.start();
		}
		barrier.await();
		return 0;
	}

	protected static int getJobId() {
		String env = System.getenv("PBS_JOBID");
		if (env == null || env.length() == 0)
			return -1;
		return Integer.parseInt(env);
	}
	
	protected int connectToSister(String hostname, int port, String secret, String[] command) throws Exception
	{
		LOG.debug("Connecting to " + hostname + ":" + String.valueOf(port) + " with password " + secret);
		SshClient client = ClientBuilder.builder().build();
		client.start();
		ConnectFuture cf = client.connect(System.getProperty("user.name"), hostname, port);
		cf.await(5000);
		if (! cf.isConnected())
		{
			LOG.warn("Failed to connect to " + hostname + ":" + String.valueOf(port));
			return 1;
		}
		LOG.debug("Connected to " + hostname);
		int rtr = 0;
		ClientSession session = cf.getSession();
		//session.addPublicKeyIdentity(new KeyPair(publicKey, privateKey))
		session.addPasswordIdentity(secret);
        session.auth().verify(1000);
        
        try(ChannelExec channel = session.createExecChannel(StringUtils.join(" ", command));) {        	
			channel.setOut(new NoCloseOutputStream(System.out));
			channel.setErr(new NoCloseOutputStream(System.err));
			channel.open().await();	       
	        channel.waitFor(Arrays.asList(ClientChannelEvent.CLOSED, ClientChannelEvent.TIMEOUT), 0);
        } catch (Exception e) {
			LOG.error("Problem creating channel", e);
			rtr = 1;
		} finally {
			session.close(false);
		}
        return rtr;
	}

	public static void main(String[] _args) throws Exception
	{
		int rc = ToolRunner.run(new Configuration(), new pbsdsh(), _args);
		LOG.debug("Exit code is "+ rc);
		System.exit(rc);
	}

}
