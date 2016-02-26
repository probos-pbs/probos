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
