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

package uk.ac.gla.terrier.probos.api;

import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;

import uk.ac.gla.terrier.probos.PConfiguration;
import uk.ac.gla.terrier.probos.controller.ProbosDelegationTokenSelector;

@ProtocolInfo(protocolName = "uk.ac.gla.terrier.probos.api.PBSInteractiveClient", protocolVersion = 2)
@KerberosInfo(serverPrincipal = PConfiguration.KEY_CONTROLLER_PRINCIPAL)
@TokenInfo(ProbosDelegationTokenSelector.class)
/** Defines the protocol for {@link ControllerServer} that lets {@link ProbosApplicationMaster}
 * inform the controller of progress
 **/
public interface PBSInteractiveClient extends VersionedProtocol {

	static int PROTOCOL_VERSION = 0;	
	public void interactiveDaemonStarted(int jobid, String hostname, int port, String secret);
	public void distributedDaemonStarted(int jobid, String hostname, int port, String secret);
	
}
