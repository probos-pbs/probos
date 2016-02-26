package probos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import gnu.trove.set.hash.TIntHashSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.After;
import org.junit.Before;
//import org.junit.Ignore;
import org.junit.Test;

import uk.ac.gla.terrier.probos.JobUtils;
import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.api.PBSJob;
import uk.ac.gla.terrier.probos.api.PBSJobStatusInteractive;
import uk.ac.gla.terrier.probos.cli.pbsnodes;
import uk.ac.gla.terrier.probos.cli.qpeek;
import uk.ac.gla.terrier.probos.cli.qstat;
import uk.ac.gla.terrier.probos.cli.qsub;
import uk.ac.gla.terrier.probos.controller.ControllerServer;
import uk.ac.gla.terrier.probos.controller.KittenUtils2;

public class TestEndToEnd {

	MiniYARNCluster miniCluster;
	ControllerServer cs;
	@Before public void setupCluster() throws Exception {
		System.setProperty("probos.home", System.getProperty("user.dir"));
		String name = "mycluster";
		int noOfNodeManagers = 1;
		int numLocalDirs = 1;
		int numLogDirs = 1;
		YarnConfiguration conf = new YarnConfiguration();
		conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
		conf.setClass(YarnConfiguration.RM_SCHEDULER,
		              FifoScheduler.class, ResourceScheduler.class);
		miniCluster = new MiniYARNCluster(
				name, noOfNodeManagers, 
				numLocalDirs, numLogDirs);
		miniCluster.init(conf);
		miniCluster.start();

		//once the cluster is created, you can get its configuration
		//with the binding details to the cluster added from the minicluster
		YarnConfiguration appConf = new YarnConfiguration(miniCluster.getConfig());
		cs = new ControllerServer(appConf);
		cs.startAndWait();
		Thread.sleep(1000);
		new pbsnodes().run(new String[0]);
	}
	
	@After public void teardownCluster() throws Exception {
		//while(true) { Thread.sleep(1000); }
		
		
		System.err.println("Entering teardown");
		cs.stopAndWait();
		miniCluster.close();
	}
	
	protected void testJobsConcurrent(int n) throws Exception 
	{
		qsub qs = new qsub();
		TIntHashSet desiredIds = new TIntHashSet();
		PBSJob[] js = new PBSJob[n];
		for(int i=1;i<=n;i++)
		{
			File job = File.createTempFile("test", ".sh");
			PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(job)));
			pw.println("#!/bin/bash");
			pw.println("sleep 15; pwd; hostname;");
			pw.close();
			
			PBSJob j = js[i-1] = qs.createJob(new String[]{"-N", "testHostname", job.toString()});
			String stdOut = j.getOutput_Path() + String.valueOf(i);
			String stdErr = j.getError_Path() + String.valueOf(i);
			new File(stdOut.split(":")[1]).delete();
			new File(stdErr.split(":")[1]).delete();
			assertNotExists(stdOut);
			assertNotExists(stdErr);
			
			int jobid = qs.submitJob(j);
			assertEquals(i, jobid);
			desiredIds.add(i);
		}
		new qstat().run(new String[0]);
		
		while(desiredIds.size() > 0)
		{
			for(int id : desiredIds.toArray())
			{
				char status = qs.c.getJobStatus(id, 0).getState();
				if (status == '?')
				{
					desiredIds.remove(id);
				}
				else
				{
					System.err.println(id + " " + status);
				}
			}
			Thread.sleep(1000);
		}
		for(int i=1;i<=n;i++)
		{
			PBSJob j = js[i-1];
			String stdOut = j.getOutput_Path() + String.valueOf(i);
			String stdErr = j.getError_Path() + String.valueOf(i);
			assertExists(stdOut);
			assertExists(stdErr);
			String[] lines = Utils.slurpString(new File(stdOut.split(":")[1]));
			assertTrue(lines.length > 0);
			//check that the job is executed in the user's home 
			//directory if no -d flag was specified to qsub
			if (! KittenUtils2.DEBUG_TASKS)
				assertEquals(System.getenv("HOME"), lines[0]);
		}
	}
	
	static final Pattern BASH_DECLARE_PATTERN = Pattern.compile("^declare -x (\\S+)=\"(.*?)\"$");
	
//	protected static void checkPattern(String t)
//	{
//		Matcher m = BASH_DECLARE_PATTERN.matcher(t);
//		assertTrue("When checking " + t, m.matches());
//		assertEquals(2, m.groupCount());
//		
//	}
//	
//	@Test
//	public void testDeclarePattern()
//	{
//		checkPattern("declare -x XPC_SERVICE_NAME=\"0\"");
//	}
	
	@Test
	public void testInteractive() throws Exception {
		qsub qs = new qsub()
		{
			@Override
			protected void interactiveStatus(PBSJobStatusInteractive si) {
				System.err.println("Job: " + si.getState() + " hostname="+ si.getHostname());
			}			
		};
		PBSJob j = qs.createJob(new String[]{"-I"});
		int jobid = qs.submitJob(j);
		
		String testLine = "This is a test";
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream stdout = new PrintStream( baos );
		int rtr = qs.waitForInteractive(jobid, 
				IOUtils.toInputStream("echo \""+testLine+"\"; exit"), 
				stdout, System.err, 60000);
		stdout.close();
		assertEquals(0,rtr);
		String[] output = new String(baos.toByteArray()).split("\n");
		boolean found = false;
		assertTrue(output.length > 0);
		for(String s : output)
		{
			if (s.contains(testLine))
				found = true;
		}
		assertTrue(found);
	}
	
	protected void testJobsSerial(int n) throws Exception 
	{
		
		for(int i=1;i<=n;i++)
		{
			File job = File.createTempFile("test", ".sh");
			PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(job)));
			pw.println("#!/bin/bash");
			pw.println("pwd");
			pw.println("export");
			pw.close();
			qsub qs = new qsub();
			
			File tmpInitDir = File.createTempFile(System.getProperty("io.tmp.dir") + "/", "probos_test");
			tmpInitDir.delete();
			tmpInitDir.mkdir();
			assertNotNull(tmpInitDir.toString());
			PBSJob j = qs.createJob(new String[]{"-N", "testEnv", "-d", tmpInitDir.toString(), job.toString()});
			
			String stdOut = j.getOutput_Path() + String.valueOf(i);
			String stdErr = j.getError_Path() + String.valueOf(i);
			new File(stdOut.split(":")[1]).delete();
			new File(stdErr.split(":")[1]).delete();
			assertNotExists(stdOut);
			assertNotExists(stdErr);
			
			
			int jobid = qs.submitJob(j);
			assertEquals(i, jobid);
			new qstat().run(new String[]{"-n"});
			Thread.sleep(1000);
			while(true)
			{
				new qstat().run(new String[]{"-n"});
				TIntHashSet jobids = new TIntHashSet(qs.c.getJobs());
				if (jobids.contains(jobid))
				{
					char state = qs.c.getJobStatus(jobid, 0).getState();
					System.err.println(jobid + " " + state);
					if (state == 'R')
						new qpeek().run(new String[]{String.valueOf(jobid)});
				}
				else
				{
					System.err.println(jobid + " Ended?" );
					break;
				}
				Thread.sleep(1000);
			}
			//Thread.sleep(360 * 1000);
			
			assertExists(stdOut);
			assertExists(stdErr);
			String[] lines = Utils.slurpString(new File(stdOut.split(":")[1]));
			assertTrue("File "+stdOut.split(":")[1]+ " is empty!", lines.length > 0);
			
			if (! KittenUtils2.DEBUG_TASKS)
			{
				Map<String,String> checkEnv = new HashMap<String,String>();
				
				String jobPwd = lines[0];
				assertEquals(tmpInitDir.toString(), jobPwd);
				
				for(int il=1;il<lines.length;il++)
				{
					String l = lines[il].trim();
					//System.err.println(l);
					//declare -x HOME="/Users/craigm"
					if (l.startsWith("declare -x"))
					{
						Matcher m = BASH_DECLARE_PATTERN.matcher(l);
						if (! m.matches())
							continue;
						//assertTrue("Line '"+l+"' doesnt match our pattern", m.matches());
						checkEnv.put(m.group(1), m.group(2));
					}
				}
				//System.err.println(checkEnv.toString());
				for (String var : JobUtils.COPY_VARS)
				{
					if (System.getenv(var) != null)
						assertEquals(System.getenv(var), checkEnv.get("PBS_O_"+var));
				}
			}
			//rest of environment is checked in TestKittenUtils
			tmpInitDir.delete();
		}
	}
	
	public void assertExists(String filename)
	{
		if (filename.contains(":"))
			filename = filename.split(":")[1];
		assertTrue("File not found: " + filename, new File(filename).exists());
	}
	
	public void assertNotExists(String filename)
	{
		if (filename.contains(":"))
			filename = filename.split(":")[1];
		assertFalse("File unexpectedly found: " + filename, new File(filename).exists());
	}
	
	@Test
	public void testSingleJob() throws Exception {
		testJobsSerial(1);
	}

	@Test(timeout=60000)
	public void testTwoSerialJob() throws Exception {
		testJobsSerial(2);
	}
	
	@Test
	public void testTwoConcurrentJob() throws Exception {
		testJobsConcurrent(2);
	}
	

	

	@Test 
	//(timeout=60000)
	public void testArrayJob() throws Exception {
		_testArrayJob(false);
	}
	
	@Test 
	//(timeout=60000)
	public void testArrayJobCopyEnv() throws Exception {
		_testArrayJob(true);
	}
	
	protected void _testArrayJob(boolean copyEnv) throws Exception {
		
		File job = File.createTempFile("test", ".sh");
		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(job)));
		pw.println("#!/bin/bash");
		pw.println("hostname");
		pw.close();
		qsub qs = new qsub();
		String[] args = new String[]{"-N", "testHostname", "-t", "1-5", job.toString()};
		if (copyEnv)
			args = new String[]{"-V", "-N", "testHostname", "-t", "1-5", job.toString()};
		PBSJob j = qs.createJob(args);
		
		for(int ar_id : new int[]{1,2,3,4,5})
		{
			String stdOut = j.getOutput_Path() + String.valueOf(1) + "-" + ar_id;
			String stdErr = j.getError_Path() + String.valueOf(1) + "-" + ar_id;
			new File(stdOut.split(":")[1]).delete();
			new File(stdErr.split(":")[1]).delete();
			assertFalse(new File(stdOut).exists());
			assertFalse(new File(stdErr).exists());
		}
		
		int jobid = qs.submitJob(j);
		assertEquals(1, jobid);
		new qstat().run(new String[0]);
		while(true)
		{
			TIntHashSet jobids = new TIntHashSet(qs.c.getJobs());
			if (jobids.contains(jobid))
			{
				new qstat().run(new String[]{"-t"});
				System.err.println(jobid + " " + qs.c.getJobStatus(jobid, 0).getState());
			}
			else
			{
				//System.err.println(jobid + " Ended?" );
				break;
			}
			Thread.sleep(1000);
		}
		
		for(int ar_id : new int[]{1,2,3,4,5})
		{
			String stdOut = j.getOutput_Path() + String.valueOf(1) + "-" + ar_id;
			String stdErr = j.getError_Path() + String.valueOf(1) + "-" + ar_id;
			assertExists(stdOut);
			assertExists(stdErr);
		}
	}
}
