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

package probos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.junit.Test;

import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.controller.ControllerPolicyProvider;

public class TestStaticMethods {

	
	@Test public void testHostname() throws Exception
	{
		String hostname = Utils.getHostname();
		assertNotNull(hostname);
		assertTrue(hostname.length() > 0);
		assertFalse(hostname.contains("\n"));
	}
	
	@Test public void testPolicy() throws Exception
	{
		Configuration c = new Configuration();
		ServiceAuthorizationManager sam = new ServiceAuthorizationManager();
		System.setProperty("hadoop.policy.file", "probos-policy.xml");
		sam.refreshWithLoadedConfiguration(c, new ControllerPolicyProvider());
		AccessControlList acl = sam.getProtocolsAcls(PBSClient.class);
		assertNotNull(acl);
		assertEquals("*", acl.getAclString());
		assertTrue(acl.isUserAllowed(UserGroupInformation.createUserForTesting("testUser", new String[]{"mygroup"})));
		sam.authorize(UserGroupInformation.getCurrentUser(), PBSClient.class, c, InetAddress.getLocalHost());
	}
}
