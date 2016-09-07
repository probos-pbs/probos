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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import uk.ac.gla.terrier.probos.Constants;
import uk.ac.gla.terrier.probos.PConfiguration;
import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.api.PBSJob;

public class InteractiveKittenUtil extends KittenUtils2 {

	public InteractiveKittenUtil(Configuration pConf, int jobid, PBSJob job, NodeRequest nodeSpec) {
		super(pConf, jobid, job, nodeSpec);
	}

	@Override
	protected void renderJob(Path targetScript, PrintWriter w)
			throws IOException
	{
		String controllerIntRPCAddress = Utils.getHostname() + ":"+ 
			String.valueOf(Constants.CONTROLLER_INTERACTIVE_PORT_OFFSET+pConf.getInt(PConfiguration.KEY_CONTROLLER_PORT, 8027));
		w.println(" container = {");
		w.println("  instances = 1,");
		String interactiveCmd = Constants.PROBOS_HOME + "/bin/pbs_intclient -conf job.xml 1>> <LOG_DIR>/stdout 2>> <LOG_DIR>/stderr";
		String secret =  job.getEuser() + String.valueOf(jobid) + String.valueOf(System.currentTimeMillis());
		String secretKey = DigestUtils.md5Hex(secret);  //Base64.encodeBase64String(secret.getBytes()); //
		
		final Map<String,String> extraEnv = new HashMap<String,String>();
		extraEnv.put("PBS_SECRET", secretKey);
		extraEnv.put("PBS_CONTROLLER", controllerIntRPCAddress);			
		printContainer(String.valueOf(jobid), interactiveCmd, w, "  ", extraEnv, super.nodeSpec);
		w.println(" }");
	}
	
	

}
