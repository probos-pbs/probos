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