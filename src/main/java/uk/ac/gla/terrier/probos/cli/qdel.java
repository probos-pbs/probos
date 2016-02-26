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

public class qdel extends Configured implements Tool {

	boolean quiet = false;
	public PBSClient c;
	
	
	public qdel() throws IOException
	{
		c = PBSClientFactory.getPBSClient();
	}
	
	@Override
	public int run(String[] args) throws Exception
	{
		Options options = new Options();
		options.addOption("p", false, "Purge the job, regardless of deletion success.");	
		for (String opt : Constants.ARGS_HELP_OPTIONS)
			options.addOption(opt, false, Constants.ARGS_HELP_MESSAGE);
		
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
		
		boolean purge = false;
		purge = cmd.hasOption('p');
		args = cmd.getArgs();
		for(String sId : args)
		{
			int rtr = c.killJob(Integer.parseInt(sId), purge);
			if (rtr == -1)
			{
				System.err.println("Could not delete job " + sId + " : no such job");
			} 
			else if (rtr == -2)
			{
				System.err.println("Could not delete job " + sId + " : timed out");
			}
		}
		return 0;
	}
	public static void main(String[] _args) throws Exception
	{
		int rc = ToolRunner.run(new Configuration(), new qdel(), _args);
		System.exit(rc);
	}




}
