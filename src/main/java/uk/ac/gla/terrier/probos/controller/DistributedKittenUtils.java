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
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import uk.ac.gla.terrier.probos.Constants;
import uk.ac.gla.terrier.probos.PConfiguration;
import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.api.PBSJob;

public class DistributedKittenUtils extends KittenUtils2 {

	int totalNodeCount;
	List<NodeRequest> nodeRequestList;
	String secretKey;
	
	public DistributedKittenUtils(Configuration pConf, int jobid, PBSJob job, List<NodeRequest> nodes, int nodeCount) {
		super(pConf, jobid, job, null);
		this.nodeRequestList = nodes;
		this.totalNodeCount = nodeCount;
		
		String secret =  job.getEuser() + String.valueOf(jobid) + String.valueOf(System.currentTimeMillis());
		secretKey = DigestUtils.md5Hex(secret);
	}
	
	@Override
	public int getNumberOfTasks() {
		return totalNodeCount;
	}
	
	@Override
	protected String getAppMasterCommand() {
		assert ! noProBoS;
		return Constants.PROBOS_HOME + "/bin/pbs_appmaster -d";
	}

	@Override
	protected void renderJob(Path targetScript, PrintWriter w)
			throws IOException {
		
		w.println(" containers = {");
		boolean first = true;
		for(NodeRequest nr : nodeRequestList)
		{
			if (nr instanceof MultiNodeRequest)
			{
				MultiNodeRequest mnr = (MultiNodeRequest)nr;
				int count = mnr.count;
				if (first)
				{
					count--;
					renderSisterSuperiorContainer(w, targetScript, mnr);
				}
				if (count > 0)
					renderSisterContainer(w, mnr, count);
				
			}
			else if (nr instanceof SingleNodeRequest) 
			{
				if (first)
				{
					renderSisterSuperiorContainer(w, targetScript, nr);
				}
				else
				{
					renderSisterContainer(w, nr, 1);
				}
			}
			else //any node request
			{
				if (first)
				{
					renderSisterSuperiorContainer(w, targetScript, nr);
				}
				else
				{
					renderSisterContainer(w, nr, 1);
				}
			}
		}
		w.println(" }");
	}
	
	@Override
	protected void renderMaster(String controllerMasterAddress, PrintWriter w, 
			NodeRequest nodeSpec, Map<String, String> extraEnv) {
		extraEnv.put("PBS_SISTER_COUNT", String.valueOf(totalNodeCount-1));
		super.renderMaster(controllerMasterAddress, w, nodeSpec, extraEnv);
	}

	protected void renderSisterSuperiorContainer(PrintWriter w, Path targetScript, NodeRequest nr) throws IOException
	{
		String controllerIntRPCAddress = Utils.getHostname() + ":"+ String.valueOf(
				Constants.CONTROLLER_MASTER_PORT_OFFSET+pConf.getInt(PConfiguration.KEY_CONTROLLER_PORT, 8027));
		w.println(" {");
		
		final Map<String,String> extraEnv = new HashMap<String,String>();
		extraEnv.put("PBS_SECRET", secretKey);
		extraEnv.put("PBS_CONTROLLER", controllerIntRPCAddress);			
		printTaskContainer(String.valueOf(jobid), targetScript, w, "  ", extraEnv, nr);
		w.println(" },");
	}
	
	@Override
	protected String finalCommandPrefix()
	{
		String rtr = super.finalCommandPrefix();
		rtr += " export PBS_NODEFILE=$(pwd)/tmp/nodefile; ";
		rtr += " echo \\\\${PBS_NODELIST_EX} | sed 's/,/ /g' | xargs -n1 echo > \\\\${PBS_NODEFILE} ; ";
		return rtr;
	}
	
	@Override
	protected String finalCommandSuffix()
	{
		String rtr = super.finalCommandSuffix();
		return rtr + " rm -f \\\\${PBS_NODEFILE}; ";
	}
	
	protected void renderSisterContainer(PrintWriter w, NodeRequest nr, int count) throws IOException
	{ 
		String controllerIntRPCAddress = Utils.getHostname() + ":"+ String.valueOf(-1+pConf.getInt(PConfiguration.KEY_CONTROLLER_PORT, 8027));
		w.println(" {");
		w.println("  instances = "+String.valueOf(count)+",");
		String sisterCmd = Constants.PROBOS_HOME + "/bin/pbs_dibclient -conf job.xml 1>> <LOG_DIR>/stdout 2>> <LOG_DIR>/stderr";
		
		final Map<String,String> extraEnv = new HashMap<String,String>();
		extraEnv.put("PBS_SISTER", "1");
		extraEnv.put("PBS_SECRET", secretKey);
		extraEnv.put("PBS_CONTROLLER", controllerIntRPCAddress);			
		printContainer(String.valueOf(jobid), sisterCmd, w, "  ", extraEnv, nr);
		w.println(" },");
	}

}
