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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.gla.terrier.probos.PBSClientFactory;
import uk.ac.gla.terrier.probos.api.PBSClient;

public class qrls extends Configured implements Tool {

	boolean quiet = false;
	public PBSClient c;
	
	
	public qrls() throws IOException
	{
		c = PBSClientFactory.getPBSClient();
	}
	
	@Override
	public int run(String[] args) throws Exception {

		for(String sId : args)
		{
			int rtr = c.releaseJob(Integer.parseInt(sId));
			if (rtr == -1)
			{
				System.err.println("Could not release job " + sId + " : no such job");
			} 
			else if (rtr == -2)
			{
				System.err.println("Could not release job " + sId + " : permission denied");
			}
			else if (rtr == -3)
			{
				System.err.println("Could not release job " + sId + " : job did not submit");
			}
		}
		return 0;
	}
	public static void main(String[] _args) throws Exception
	{
		int rc = ToolRunner.run(new Configuration(), new qrls(), _args);
		System.exit(rc);
	}




}
