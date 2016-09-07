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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.gla.terrier.probos.Constants;
import uk.ac.gla.terrier.probos.PBSClientFactory;
import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.api.PBSJobAttribute;
import uk.ac.gla.terrier.probos.api.PBSJobAttributeOperand;
import uk.ac.gla.terrier.probos.api.PBSJobSelector;

import com.google.common.annotations.VisibleForTesting;

public class qselect extends Configured implements Tool  {
	
	public PBSClient c;

	public qselect() throws IOException
	{
		c = PBSClientFactory.getPBSClient();
	}
	
	@Override
	public int run(String[] args) throws Exception {

		PBSJobSelector[] selectors = parseSelectCommandLine(args);
		
		//break for help message
		if (selectors == null)
			return 0;
		
		for (int jid : c.selectJobs(selectors))
		{
			System.out.println(jid);
		}
		
		return 0;
	}
	
	static final char[] stringAttributeArgs = new char[]{
		 'A', 'e', 'g', 'j', 'M', 'm','N','o', 'q', 'S', 's', 'u', 
	};
	static final PBSJobAttribute[] stringAttributeAttr = new PBSJobAttribute[]{
		PBSJobAttribute.ATTR_A,
		PBSJobAttribute.ATTR_e,		
		PBSJobAttribute.ATTR_g,
		PBSJobAttribute.ATTR_j,		
		PBSJobAttribute.ATTR_M,
		PBSJobAttribute.ATTR_m,
		PBSJobAttribute.ATTR_N,
		PBSJobAttribute.ATTR_o,		
		PBSJobAttribute.ATTR_q,
		PBSJobAttribute.ATTR_S,
		PBSJobAttribute.ATTR_state,
		PBSJobAttribute.ATTR_u,
	};

	@VisibleForTesting
	public static PBSJobSelector[] parseSelectCommandLine(String[] args)
			throws ParseException {
		Options options = new Options();
		options.addOption("A", true, "Restricts job selection to those with the user specified account value.");
		options.addOption("e", true, "Restricts job selection to those with the error file.");
		options.addOption("g", true, "Restricts job selection to those with the specified group.");
		
		options.addOption("j", true, "Restricts job selection to those with the join option.");
		
		options.addOption("M", true, "Restricts selection to jobs with the mailing address");
		options.addOption("m", true, "Restricts selection to jobs with the mail option");
		options.addOption("N", true, "Restricts selection to jobs with the specified name");
		options.addOption("o", true, "Restricts job selection to those with the output file.");
		
		options.addOption("q", true, "Restricts selection to jobs submitted to the specified queue (NOT their current queue)");
		options.addOption("S", true, "Restricts job selection to those with the specified shell.");
		options.addOption("s", true, "Restricts job selection to those in the specified states.");
		options.addOption("u", true, "Restricts selection to jobs owned by the specified user name");
		
		for (String opt : Constants.ARGS_HELP_OPTIONS)
			options.addOption(opt, false, Constants.ARGS_HELP_MESSAGE);
		
		
		
		
		CommandLineParser parser = new GnuParser();
		CommandLine cmd = parser.parse( options, args);
		
		if (cmd.hasOption(Constants.ARGS_HELP_OPTIONS[0])
				|| cmd.hasOption(Constants.ARGS_HELP_OPTIONS[1])
				|| cmd.hasOption(Constants.ARGS_HELP_OPTIONS[2]))
		{
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("qselect", options);
			return null;
		}
				
		List<PBSJobSelector> selectors = new ArrayList<PBSJobSelector>();
		
		assert stringAttributeArgs.length == stringAttributeAttr.length;
		
		for(int i=0;i<stringAttributeArgs.length;i++)
		{			
			char opt = stringAttributeArgs[i];		
			assert options.hasOption(String.valueOf(opt)) : "Options parser is missing " + opt;
			
			if (cmd.hasOption(opt))
			{
				PBSJobSelector sel = new PBSJobSelector(
						stringAttributeAttr[i], 
						null, 
						cmd.getOptionValue(opt), 
						PBSJobAttributeOperand.EQ);
				selectors.add(sel);
			}
		}
		
		//TODO: l, h, p, r, 
		//TODO: advanced operators
		
		return selectors.toArray(new PBSJobSelector[selectors.size()]);
	}
	
	public static void main(String[] _args) throws Exception
	{
		int rc = ToolRunner.run(new Configuration(), new qselect(), _args);
		System.exit(rc);
	}
}
