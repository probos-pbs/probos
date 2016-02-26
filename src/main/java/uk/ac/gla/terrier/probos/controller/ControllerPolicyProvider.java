package uk.ac.gla.terrier.probos.controller;

import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;

import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.api.PBSInteractiveClient;
import uk.ac.gla.terrier.probos.api.PBSMasterClient;
/**
 * {@link PolicyProvider} for ProBoS Controller protocols.
 */
public class ControllerPolicyProvider extends PolicyProvider {
  
  private static final Service[] controllerServices = 
   new Service[] {
	    new Service(
	        "security.pbsclient.protocol.acl", 
	        PBSClient.class),
        new Service(
    	    "security.pbsclient.protocol.acl", 
    	    PBSMasterClient.class),
	    new Service(
    	    "security.pbsclient.protocol.acl", 
    	    PBSInteractiveClient.class)
  };

  @Override
  public Service[] getServices() {
    return controllerServices;
  }

}