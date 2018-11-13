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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import uk.ac.gla.terrier.probos.JobUtils;
import uk.ac.gla.terrier.probos.api.PBSJob;

public class ArrayKittenUtils extends KittenUtils2 {

	ArrayKittenUtils(Configuration pConf, int jobid, PBSJob job, NodeRequest nodeSpec) {
		super(pConf, jobid, job, nodeSpec);
	}

	@Override
	protected void renderJob(Path targetScript, PrintWriter w) throws IOException 
	{
		final int[] taskArrayIds = JobUtils.getTaskArrayItems(job);
		LOG.info(jobid + " has an array: " + taskArrayIds.length + " items");
		w.println(" containers = {");
		Map<String,String> extraEnv = new HashMap<String,String>(1);
		for(int id : taskArrayIds)
		{
			//w.println("  array"+id+" = {");
			w.println("  {");
			extraEnv.put("PBS_ARRAYID", String.valueOf(id));
			printTaskContainer("-" + id, targetScript,w,"   ", extraEnv, super.nodeSpec);
			w.println("  },");
		}
		w.println(" }");
	}

	@Override
	public int getNumberOfTasks() {
		return JobUtils.getTaskArrayItems(job).length;
	}

}
