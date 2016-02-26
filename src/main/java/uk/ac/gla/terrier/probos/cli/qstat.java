package uk.ac.gla.terrier.probos.cli;

import gnu.trove.list.array.TIntArrayList;

import java.io.IOException;
import java.io.PrintStream;

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
import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.api.PBSJob;
import uk.ac.gla.terrier.probos.api.PBSJobArrayStatusLight;
import uk.ac.gla.terrier.probos.api.PBSJobStatusFat;
import uk.ac.gla.terrier.probos.api.PBSJobStatusLight;
import uk.ac.gla.terrier.probos.api.PBSJobStatusNodes;

public class qstat extends Configured implements Tool {

	private PBSClient c;
	private PrintStream out;
	
	public qstat(PBSClient _c, PrintStream _output) throws IOException
	{
		this.c = _c;
		this.out = _output;
	}
	
	public qstat() throws IOException
	{
		this(PBSClientFactory.getPBSClient(), System.out);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		int[] jobids;
		Options options = new Options();
		options.addOption("n", false, "In addition to the basic information, nodes allocated to a job are listed.");
		options.addOption("f", false, "Specifies that a full status display be written to standard out.");
		options.addOption("t", false, "Specifies that output is extended to display the entire status of arrays.");
		options.addOption("r", false, "Does nothing (default mode)");
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
			formatter.printHelp("qstat", options);
			return 0;
		}
		
		boolean nodes = cmd.hasOption('n');		
		boolean full = cmd.hasOption('f');
		boolean arrays = cmd.hasOption('t');
		boolean specified = false;
		if (args.length == 0)
		{
			jobids = c.getJobs();
		}
		else
		{
			TIntArrayList _ids = new TIntArrayList();
			for(String a : args)
			{
				specified = true;//we specify even if its turns out to be invalid
				a = a.replaceFirst("\\[\\]$", ""); //drop the array suffix denotation
				try {
					_ids.add(Integer.parseInt(a));
				} catch (NumberFormatException e) {
					System.err.println("Ignoring invalid jobid '"+a+"'");
				}
			}
			jobids = _ids.toArray();			
		}
		boolean err = false;
		if (! full)
		{
			out.println("Job id              Name             User            Time Use S Queue");
			out.println("------------------- ---------------- --------------- -------- - -------");
		}
		for(int id : jobids)
		{			
			PBSJobStatusLight jStatus = c.getJobStatus(id, full ? 2 : nodes ? 1 : arrays ? 3 : 0);
			char state = jStatus.getState();
			if (state == '?')
			{
				System.err.println("qstat: Unknown Job Id " + id);
				err = true;
				continue;
			}
			
			if (full)
			{
				out.println(renderFullJob((PBSJobStatusFat) jStatus));
			}
			else
			{
				String jobId = String.valueOf(id);
				if (jStatus.hasArray())
					jobId = jobId + "[]";
				out.println(
					Utils.padRight(jobId, 19) + ' ' 
					+ Utils.padRight(jStatus.getJob_Name(), 16) + ' ' 
					+ Utils.padRight(jStatus.getJob_Owner().replaceFirst("@.+$", ""), 15) + ' '
					+ Utils.padLeft(jStatus.getTimeUse(), 8) + ' '
					+ state + ' ' 
					+ Utils.padRight(jStatus.getQueue(), 7));
				if (nodes)
				{
					String nodeList = ((PBSJobStatusNodes)jStatus).getNodes();
					if (nodeList != null && nodeList.length() > 0)
						out.println(nodeList);
				} else if (arrays && jStatus.hasArray()) {
					PBSJobArrayStatusLight arStatus = (PBSJobArrayStatusLight)jStatus;
					char[] aStates = arStatus.getArrayStates();
					int i=0;
					for(int aid : arStatus.getArrayIds())
					{
						String suffix = '[' + String.valueOf(aid) + ']';
						out.println(
								Utils.padRight(String.valueOf(id) + suffix, 19) + ' ' 
								+ Utils.padRight(jStatus.getJob_Name() + suffix, 16) + ' ' 
								+ Utils.padRight(jStatus.getJob_Owner().replaceFirst("@.+$", ""), 15) + ' '
								+ Utils.padLeft("", 8) + ' '
								+ aStates[i] + ' ' 
								+ Utils.padRight(jStatus.getQueue(), 7));
						i++;
					}
					
				}
			}
		}
		if (specified && err || (specified && jobids.length == 0))
			return 1;
		return 0;
	}
	
	static String renderFullJob(PBSJobStatusFat status)
	{
		StringBuilder s = new StringBuilder();
		PBSJob job = status.getJob();
		String jobId = String.valueOf(status.getJobId());
		if (status.hasArray())
			jobId = jobId + "[]";
		s.append("Job Id:" + jobId);
		s.append("\n\tJob_Name = " + job.getJob_Name());
		s.append("\n\tJob_Owner = " + job.getJob_Owner());
		s.append("\n\tjob_state = " + status.getState());
		s.append("\n\texec_host = " + status.getNodes());
		s.append("\n\tError_Path = " + job.getError_Path());
		s.append("\n\tOutput_Path = " + job.getOutput_Path());
		s.append("\n\tresources_used.cput = " + status.getTimeUse());
		s.append("\n\texec_host = " + status.getNodes());
		s.append("\n\tMail_Points = " + job.getMailEvent());
		s.append("\n\tJoin_Path = " + job.getJoin());
		s.append("\n\tsubmit_args = " + StringUtils.join(" ", job.getSubmit_args()));
		s.append("\n\tqueue = " + job.getQueue());
		s.append("\n\tapplicationId = " + status.getApplicationId());
		s.append("\n\tMaster_Container = " + stringOrEmpty(status.getMasterContainerId()));
		s.append("\n\tTask_Containers = " + (status.getTaskContainerIds() != null 
				? StringUtils.join(StringUtils.COMMA_STR, status.getTaskContainerIds()) : ""));
		s.append("\n\tTracking_URL = " + stringOrEmpty(status.getTrackingURL()));
		
		return s.toString();
	}
	
	public static String stringOrEmpty(String s)
	{
		if (s == null)
			return "";
		return s;
	}
	
	public static void main(String[] _args) throws Exception
	{
		int rc = ToolRunner.run(new Configuration(), new qstat(), _args);
		System.exit(rc);
	}
}
