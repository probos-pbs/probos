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
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.gla.terrier.probos.Constants;
import uk.ac.gla.terrier.probos.PBSClientFactory;
import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.api.PBSNodeStatus;

public class pbsnodes extends Configured implements Tool {

	public static void main(String[] _args) throws Exception
	{
		int rc = ToolRunner.run(new Configuration(), new pbsnodes(), _args);
		System.exit(rc);
	}
	
	private PBSClient c;
	private PrintStream out;
	
	public pbsnodes(PBSClient _c, PrintStream _output) throws IOException
	{
		this.c = _c;
		this.out = _output;
	}
	
	public pbsnodes() throws IOException
	{
		this(PBSClientFactory.getPBSClient(), System.out);
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		
		Options options = new Options();
		options.addOption("a", false, "Legacy option for default notation (DOES NOTHING).");//option does nothing
		options.addOption("l", false, "List nodes marked as DOWN, OFFLINE, or UNKNOWN.");
		options.addOption("q", false, "Supress all error messages.");
		for (String opt : Constants.ARGS_HELP_OPTIONS)
			options.addOption(opt, false, Constants.ARGS_HELP_MESSAGE);
		
		CommandLineParser parser = new GnuParser();
		CommandLine cmd = parser.parse( options, args);
		args = cmd.getArgs();
		if (cmd.hasOption(Constants.ARGS_HELP_OPTIONS[0])
				|| cmd.hasOption(Constants.ARGS_HELP_OPTIONS[1])
				|| cmd.hasOption(Constants.ARGS_HELP_OPTIONS[2]))
		{
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("pbsnodes", options);
			return 0;
		}
		
		final Set<String> hostnames = new HashSet<String>();
		boolean quiet = cmd.hasOption('q');
		boolean whitelist = false;
		boolean offline = cmd.hasOption('l');
		for(String h : args)
		{
			whitelist = true;
			hostnames.add(h);
		}
		PBSNodeStatus[] nodes = c.getNodesStatus();
		for(PBSNodeStatus n : nodes)
		{
			if (whitelist && hostnames.contains(n.getNodeName()))
			{
				if (offline && ! n.getState().equals("RUNNING"))
				{
					out.println(n.getNodeName() + "\t" + n.getState());
				}
				else
				{
					out.println(getStandardFormat(n));
				}
				hostnames.remove(n.getNodeName());
			}
			else if (! whitelist)
			{
				if (offline && ! n.getState().equals("RUNNING"))
				{
					out.println(n.getNodeName() + "\t" + n.getState());
				}
				else
				{
					out.println(getStandardFormat(n));
				}
			}
		}
		if (!quiet && hostnames.size() > 0)
		{
			for (String missing : hostnames)
			{
				System.err.println("pbsnodes: Unknown node " + missing);
			}
			return 1;
		}
		return 0;
	}
	
	static String getStandardFormat(PBSNodeStatus n)
	{
		StringBuilder rtr = new StringBuilder();
		rtr.append(n.getNodeName());
		rtr.append("\n\tstate = " + n.getState());
		rtr.append("\n\tnp = " + n.getNp());
		
		int[] jobs = n.getJobs();
		String[] strJ = new String[jobs.length];
		for(int i=0;i<jobs.length;i++)
			strJ[i] = String.valueOf(jobs[i]);
		rtr.append("\n\tjobs = "      + StringUtils.join(",", strJ));
		
		rtr.append("\n\tlabels = "    + StringUtils.join(",", n.getNodeLabels()));
		rtr.append("\n\track = "      + n.getRack());
		rtr.append("\n\tyarnState = " + n.getYarnState());
		rtr.append("\n\thealth = "    + n.getHealth());
		rtr.append("\n\tstatus = "    + n.getStatus());
		rtr.append('\n');
		return rtr.toString();
	}
}
