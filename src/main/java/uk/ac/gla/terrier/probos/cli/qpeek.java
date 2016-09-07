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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.gla.terrier.probos.Constants;
import uk.ac.gla.terrier.probos.PBSClientFactory;
import uk.ac.gla.terrier.probos.api.PBSClient;

public class qpeek extends Configured implements Tool {

	public PBSClient c;
	
	
	public qpeek() throws IOException
	{
		c = PBSClientFactory.getPBSClient();
	}
	
	@Override
	public int run(String[] args) throws Exception {

		boolean stdout = true;
		boolean url = false;
		
		Options options = new Options();
		options.addOption("e", false, "Show the stderr.");
		options.addOption("o", false, "Show the stdout.");
		options.addOption("u", false, "Instead of the content of the log, show the URL that the log can be found at.");	
		for (String opt : Constants.ARGS_HELP_OPTIONS)
			options.addOption(opt, false, Constants.ARGS_HELP_MESSAGE);
		
		//options.addOption("v", false, "Be verbose");
		CommandLineParser parser = new GnuParser();
		CommandLine cmd = parser.parse(options, args);

		if (cmd.hasOption(Constants.ARGS_HELP_OPTIONS[0])
				|| cmd.hasOption(Constants.ARGS_HELP_OPTIONS[1])
				|| cmd.hasOption(Constants.ARGS_HELP_OPTIONS[2]))
		{
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("qpeek", options);
			return 0;
		}
		
		if (cmd.hasOption('e'))
			stdout = false;
		if (cmd.hasOption('o'))
			stdout = true;
		url = cmd.hasOption('u');
		
//		if (cmd.hasOption('v'))
//			verbose = true;
		if (cmd.hasOption('e') && cmd.hasOption('o'))
		{
			System.err.println("Cannot use -o and -e");
			return 1;
		}
		args = cmd.getArgs();
		if (args.length == 0)
		{
			System.err.println("No jobid specified");
			return 1;
		}
		for(String sId : args)
		{
			int jobId;
			int arId = 0;
			if (sId.contains("[")) //it could specify an array id
			{
				String [] parts = sId.split("\\[|\\]");
				jobId = Integer.parseInt(parts[0]);
				arId = Integer.parseInt(parts[1]);
			}
			else
			{
				jobId = Integer.parseInt(sId);
			}
			byte[] log = c.jobLog(jobId, arId, stdout, (long)0, url);
			System.out.write(log);
		}
		return 0;
	}
	public static void main(String[] _args) throws Exception
	{
		int rc = ToolRunner.run(new Configuration(), new qpeek(), _args);
		System.exit(rc);
	}

}
