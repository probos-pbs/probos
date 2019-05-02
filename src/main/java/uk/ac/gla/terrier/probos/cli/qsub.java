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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.sshd.client.ClientBuilder;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannel.ClientChannelEvent;
import org.apache.sshd.client.future.ConnectFuture;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.util.io.NoCloseInputStream;
import org.apache.sshd.common.util.io.NoCloseOutputStream;

import uk.ac.gla.terrier.probos.Constants;
import uk.ac.gla.terrier.probos.JobUtils;
import uk.ac.gla.terrier.probos.PBSClientFactory;
import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.api.PBSJob;
import uk.ac.gla.terrier.probos.api.PBSJobStatusInteractive;

public class qsub extends Configured implements Tool {

	boolean quiet = false;
	boolean luaOnly = false;
	boolean luaHeaders = false;
	boolean noProbos = false;
	boolean interactive = false;
	public PBSClient c;
		
	public qsub() throws IOException
	{
		c = PBSClientFactory.getPBSClient();
	}
	
	@Override
	public int run(String[] args) throws Exception {

		PBSJob job = createJob(args);
		if (job == null)
			return 0;//this is the breakout for the help options
		
		//we're only going to print out the lua definition of the job		
		if (luaOnly)
		{
			System.out.println(c.kittenSpecification(job, luaHeaders, noProbos));
			return 0;
		}
		
		//lets go an submit the job
		int id = submitJob(job);
		if (id == -1)
		{
			System.err.println("Could not submit job!");
			return -1;
		}
		if (! quiet)
			System.out.println(String.valueOf(id));
		if (! interactive)
			return 0;
		return waitForInteractive(id, System.in, System.out, System.err, -1);
	}
	
	protected void interactiveStatus(PBSJobStatusInteractive si) { }
	
	/**
	 * @return 0 for success; 1 for no connection after 5 seconds, -1 for job was deleted, 2 for timeout
	 * @throws Exception
	 */
	public int waitForInteractive(int jobId, InputStream stdin, PrintStream stdout, PrintStream stderr, long max) 
			throws Exception 
	{
		int rtr =0;
		long start = System.currentTimeMillis();
		while(true)
		{
			PBSJobStatusInteractive intStatus = (PBSJobStatusInteractive) c.getJobStatus(jobId, 4);
			interactiveStatus(intStatus);
			if (intStatus.getState() == '?')
			{
				System.err.println("Job " + jobId + " was lost");
				return -1;
			}
			if (intStatus.getState() == 'E')
			{
				System.err.println("Job " + jobId + " is ending");
				return -1;
			}
			if (intStatus.getState() == 'R' && intStatus.getHostname() != null)
			{
				rtr = interactiveConsole(
						intStatus.getHostname(), intStatus.getPort(), intStatus.getSecret(),
						stdin, stdout, stderr);
				break;
			}
			Thread.sleep(1000);
			if (max > 0 && System.currentTimeMillis() - start > max)
			{
				System.err.println("Timeout waiting to connect to node for interactive job " 
						+ jobId + " after " + max + "ms");
				return 2;
			}
		}
		return rtr;
	}
	
	/** 
	 * @return 0 for success; 1 for no connection after 5 seconds
	 * @throws Exception
	 */
	protected int interactiveConsole(String hostname, int port, String secret,
			InputStream stdin, PrintStream stdout, PrintStream stderr) throws Exception
	{
		SshClient client = ClientBuilder.builder().build();
		client.start();
		ConnectFuture cf = client.connect(System.getProperty("user.name"), hostname, port);
		cf.await(5000);
		if (! cf.isConnected())
		{
			return 1;
		}
		ClientSession session = cf.getSession();
		//session.addPublicKeyIdentity(new KeyPair(publicKey, privateKey))
		session.addPasswordIdentity(secret);
        session.auth().verify(1000);
 
        int rtr = 0;
		try(ClientChannel channel = session.createChannel(ClientChannel.CHANNEL_SHELL)) {
		    channel.setIn(new NoCloseInputStream(stdin));
		    channel.setOut(new NoCloseOutputStream(stdout));
		    channel.setErr(new NoCloseOutputStream(stderr));
		    channel.open();
		    channel.waitFor(Arrays.asList(ClientChannelEvent.CLOSED), 0);
		    //channel.waitFor(Arrays.asList(ClientChannelEvent.CLOSED, ClientChannelEvent.TIMEOUT), 0);
		} catch (Exception e) {
			System.err.println(e);
			rtr = 1;
		} finally {
			session.close(false);
		}
		client.stop();
		return rtr;
	}
	
	/** submits the job to the cluster via pbsclient 
	 * @return id of the submitted job */
	public int submitJob(PBSJob job) throws Exception
	{
		byte[] script = job.getInteractive() 
			? new byte[0]
			: Utils.slurpBytes(new File(job.getCommand()));
		int id = c.submitJob(job, script);		
		return id;
	}
	
	/** Populates a job from the commandline arguments to Qsub */
	public PBSJob createJob(String[] _args) throws Exception
	{
		CommandLine cmd = JobUtils.parseJobSubmission(_args);
		if (cmd.hasOption("help") || cmd.hasOption('h'))
		{
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("qsub", JobUtils.getOptions());
			return null;
		}
		if (cmd.hasOption('z'))
			quiet = true;
		if (cmd.hasOption('L'))
		{
			luaOnly = true;
			if (cmd.hasOption("withLuaHeaders"))
				luaHeaders = true;
			if (cmd.hasOption("np"))
				noProbos = true;
		}
		
		String[] args = cmd.getArgs();
		if (args.length > 1)
		{
			//System.err.println(cmd.hasOption("N"));
			throw new IllegalArgumentException("Only one file allowed, but received "+
					Arrays.deepToString(_args) +", found " + Arrays.deepToString(args));
		}
		//System.err.println("received "+
		//			Arrays.deepToString(_args) +", found " + Arrays.deepToString(args));
		String jobName;
		String jobFilename;
		if (cmd.hasOption('I'))
		{
			interactive = true;
			//we dont read files
			jobName = "Interactive";
			jobFilename = "Interactive";
		}
		else if (args.length == 0 || args[0].equals("-"))
		{
			File tempJobFile = File.createTempFile("jobScript", ".pbs");
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(tempJobFile));
			BufferedInputStream bis = new BufferedInputStream(System.in);
			IOUtils.copyBytes(bis, bos, 4096);
			bis.close(); bos.close();
			jobFilename = tempJobFile.toString();
			jobName = "STDIN";
			
		} else {
			jobFilename = args[0];
			jobName = new File(jobFilename).getName();
		}
		PBSJob job = JobUtils.createNewJob(jobName);
		
		String directivePrefix = Constants.DIRECTIVE_PREFIX;
		if (System.getenv("PBS_DPREFIX") != null)
		{
			directivePrefix = System.getenv("PBS_DPREFIX").trim();
		}
		if (cmd.hasOption('C'))
		{
			directivePrefix = cmd.getOptionValue('C').trim();
		}
		if (! interactive && directivePrefix.length() > 0 && ! directivePrefix.equalsIgnoreCase("null"))
		{
			String[] file = Utils.slurpString(new File(jobFilename));
			int i=0;
			for(String line : file)
			{
				if (i == 0 && ( line.startsWith(":") || line.startsWith("#!")))
					continue;
				if (line.startsWith(directivePrefix + " "))
				{
					String[] lineargs = line.replaceFirst("^" + directivePrefix + " ", "").split(" ");
					CommandLine jobCmdLine = JobUtils.parseJobSubmission(lineargs);
					JobUtils.parseArgs(jobCmdLine, job);
				}
				//Scanning will continue until the first executable line, that is a 
				//line that is not blank, not a directive line, nor a line whose first 
				//non white space character is "#". If directives occur on subsequent 
				//lines, they will be ignored.
				else if (! line.replaceFirst("^\\s+", "").startsWith("#"))
					break;
				i++;
			}
		}
		//commandline arguments overrule 
		JobUtils.parseArgs(cmd, job);
		job.setSubmit_args(_args);
		
		job.setCommand(new File(jobFilename).getCanonicalPath());
		JobUtils.finaliseJob(job);
		JobUtils.verifyJob(job);
		return job;
	}
	
	public static void main(String[] _args) throws Exception
	{
		int rc = ToolRunner.run(new Configuration(), new qsub(), _args);
		System.exit(rc);
	}




}
