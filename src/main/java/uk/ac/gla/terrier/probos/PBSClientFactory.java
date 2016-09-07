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

package uk.ac.gla.terrier.probos;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;

import uk.ac.gla.terrier.probos.api.PBSClient;

/** Generates a PBS client connection to a Controller server.
 * <b>Configuration</b>:
 * <ul><li>Server hostname - set in <tt>probos.controller.hostname</tt>, which can
 * be overridden by the <tt>PBS_DEFAULT</tt> environment variable.</li>
 * <li>Server port <tt>probos.controller.port</tt> .</li></ul>
 */
public class PBSClientFactory {

	private static final Logger LOG = LoggerFactory.getLogger(PBSClientFactory.class);
	
	private static PBSClient forcedClient = null;
	
	@VisibleForTesting
	public static void forceClient(PBSClient c) {
		forcedClient = c;
	}
	
	public static PBSClient getPBSClient() throws IOException
	{
		if (forcedClient != null)
			return forcedClient;
		
		final Configuration c = new Configuration();
		final PConfiguration pConf = new PConfiguration(c);
		String _serverHostname = pConf.get(PConfiguration.KEY_CONTROLLER_HOSTNAME);
		if (System.getenv("PBS_DEFAULT") != null)
		{
			_serverHostname = System.getenv("PBS_DEFAULT");
		}
		final String serverHostname = _serverHostname;
		LOG.debug("Connecting to server " + serverHostname);
		
		InetSocketAddress server = new InetSocketAddress(
				serverHostname, 
				pConf.getInt(PConfiguration.KEY_CONTROLLER_PORT, 8027));
		LOG.debug("Connecting to server at address " + server.toString());
		PBSClient rtr = RPC.getProxy(PBSClient.class,
		        RPC.getProtocolVersion(PBSClient.class),
		        server, UserGroupInformation.getCurrentUser(), pConf, NetUtils
		        .getDefaultSocketFactory(c));
		LOG.debug("Got RPC connection!");
		return rtr;
	}
	
}
