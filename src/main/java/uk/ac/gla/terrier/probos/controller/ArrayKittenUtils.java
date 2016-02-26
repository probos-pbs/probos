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
			printTaskContainer(String.valueOf(jobid) + "-" + id, targetScript,w,"   ", extraEnv, super.nodeSpec);
			w.println("  },");
		}
		w.println(" }");
	}

	@Override
	public int getNumberOfTasks() {
		return JobUtils.getTaskArrayItems(job).length;
	}

}
