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

package uk.ac.gla.terrier.probos.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.gla.terrier.probos.PConfiguration;

import com.cloudera.kitten.appmaster.ApplicationMasterParameters;
import com.cloudera.kitten.appmaster.ApplicationMasterService;
import com.cloudera.kitten.appmaster.params.lua.LuaApplicationMasterParameters;

public class ProbosApplicationMaster extends Configured implements Tool {

	Configuration c = null;
	
	@Override
	public Configuration getConf() {
		if (c != null)
			return c;
		//replace the configuration with a ProBoS configuration
		return c = new PConfiguration(super.getConf());		
	}

	@Override
	  public int run(String[] args) throws Exception {
		
		//we can detect if this is a distributed application master
		//which has different container start logic
		boolean dib = false;
		for(String s : args)
		{
			if (s.equals("-d"))
			{
				dib = true;
				break;
			}
		}
	    ApplicationMasterParameters params = new LuaApplicationMasterParameters(getConf());
	    ApplicationMasterService service;
	    if (dib)
	    	service = new ProbosApplicationMasterDistributedServiceImpl(params, getConf());
	    else
	    	service = new ProbosApplicationMasterServiceImpl(params, getConf());
	    
	    service.startAndWait();
	    while (service.hasRunningContainers()) {
	      Thread.sleep(1000);
	    }
	    service.stopAndWait();
	    return 0;
	  }

	  public static void main(String[] args) throws Exception {
	    try { 
	      int rc = ToolRunner.run(new Configuration(), new ProbosApplicationMaster(), args);
	      System.exit(rc);
	    } catch (Exception e) {
	      System.err.println(e);
	      e.printStackTrace();
	      System.exit(1);
	    }
	  }
	}
