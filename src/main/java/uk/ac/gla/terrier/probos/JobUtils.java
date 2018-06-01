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

package uk.ac.gla.terrier.probos;

import gnu.trove.list.array.TIntArrayList;

import java.io.File;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.mortbay.log.Log;

import uk.ac.gla.terrier.probos.api.PBSJob;

public class JobUtils {
	
	/** env vars that should be copied into the job with PBS_O_ prefix, according to the PBS spec */
	public static final String[] COPY_VARS = {"HOME", "LANG", "LOGNAME", "PATH", "MAIL", "SHELL", "TZ" };
	
	/** variables that should not be copied from the qsub environment */
	public static final String[] BLACKLIST_VARS = {"TERMCAP"};
	
	/** variables that should be overridden from the yarn environment */
	public static final String[] REMOVE_VARS = {"XDG_RUNTIME_DIR"};
	
	/** these are variables that should be set in the job based on the node being run on,
	 * according to what login(3) would get from the passwd entry. See also getpwnam().
	 * We dont have acces to this, so we copy from the qsub environment.
	 */
	public static final String[] COPY_PLAIN_VARS = {"HOME", "USER", "SHELL",
			//"PATH"
			//"LOGNAME", -- essentially deprecated
			"MAIL" 
	};
	
	/** qsub options not supported by ProBoS */
	public static final char[] UNSUPPORTED_ARGS = {
		//NOT POSSIBLE
		'a', 'b', 'c', 'D', 'W', 'X',
		//IMPLEMENT(?) LATER
		'k', 'p', 'u'
	};
	
	
	public static final String DEFAULT_QUEUE = "default";

	/** Get the command line options for qsub */
	public static final Options getOptions()
	{
		// create Options object
		Options options = new Options();
		for (String opt : Constants.ARGS_HELP_OPTIONS)
			options.addOption(opt, false, Constants.ARGS_HELP_MESSAGE);
		
		options.addOption("A", true, "Defines the account string associated with the job.");
		options.addOption("C", true, "Define the prefix that declares a directive to the qsub utility within the script. Defaults to " + Constants.DIRECTIVE_PREFIX);
		options.addOption("d", true, "Defines the working directory path to be used for the job. Defaults to the users' home directory");
		options.addOption("e", true, "Defines the path to be used for the standard error stream of the batch job.");
		options.addOption("H", false, "Specifies that a user hold is applied to the job at submission time NB: THIS CHANGES FROM SPEC -h to -H.");
		options.addOption("I", false, "Declares that the job is to be run \"interactively\".");
		
		options.addOption("j", true, "Declares if the standard error stream of the job will be merged with the standard output stream of the job (oe) or vice versa (eo).");
		options.addOption("L", false, "Instead of submitting the job, simply return the Kitten definition of the job in lua");
		options.addOption("l", true, "Defines the resources that are required by the job and establishes a limit to the amount of resource that can be consumed.");
		options.addOption("M", true, "Declares the list of users to whom mail is sent by ProBoS when it sends mail about the job.");
		options.addOption("m", true, "Defines the set of conditions under which the execution server will send a mail message about the job. ");
		options.addOption("N", true, "Declares a name for the job.");
		options.addOption("o", true, "Defines the path to be used for the standard output stream of the batch job.");
		options.addOption("q", true, "Defines the destination queue of the job.");
		options.addOption("S", true, "Declares the shell that interprets the job script.");
		options.addOption("t", true, "ARRAYS");
		options.addOption("V", false, "Declares that all environment variables in the qsub command's environment are to be exported to the batch job.");
		options.addOption("v", true, "Expands the list of environment variables that are exported to the job.");
		options.addOption("z", false, "Directs that the qsub command is not to write the job identifier assigned to the job to the command's standard output.");
	
		options.addOption("withLuaHeaders", false, "Only valid with -L; Prepend the Kitten lua specification.");
		options.addOption("np", false, "Only valid with -L; Remove all ProBoS from Kitten lua file.");
		
		for(char c : JobUtils.UNSUPPORTED_ARGS)
			options.addOption(String.valueOf(c), false, "Unsupported");
		return options;
	}

	/** Parse the qsub arguments */
	public static CommandLine parseJobSubmission(String[] _args) throws ParseException
	{
		Options options = getOptions();
	
		CommandLineParser parser = new GnuParser();
		CommandLine cmd = parser.parse( options, _args);
		for(char c : JobUtils.UNSUPPORTED_ARGS)
		{
			if (cmd.hasOption(String.valueOf(c)))
			{
				throw new IllegalArgumentException("Option " + c + " is not supported");
			}
		}
	
		return cmd;
	}

	/** Get the full list of array task ids for the given job */
	public static int[] getTaskArrayItems(PBSJob job)
	{
		int[][] idExpression = job.getArrayTaskIds();
		return JobUtils.getTaskArrayItems(idExpression);
	}

	/** Get the full list of array task ids for the given sparse representation */
	public static int[] getTaskArrayItems(int[][] idExpression)
	{
		if (idExpression == null)
			return null;
		TIntArrayList ids = new TIntArrayList();
		for(int[] x : idExpression)
		{
			if (x.length == 1)
			{
				ids.add(x[0]);
			}
			else
			{
				for(int i=x[0];i<=x[1];i++)
				{
					ids.add(i);
				}
			}
		}
		return ids.toArray();
	}

	/** Count the number of array items in a job */
	public static int countTaskArrayItems(PBSJob job)
	{
		int[][] idExpression = job.getArrayTaskIds();
		if (idExpression == null)
			return 1;
		int counter = 0;
		for(int[] x : idExpression)
		{
			if (x.length == 1)
			{
				counter++;
			}
			else
			{
				for(int i=x[0];i<=x[1];i++)
				{
					counter++;
				}
			}
		}
		return counter;
	}


	/** add the environment to a job */
	public static void setEnvironment(PBSJob j, Map<String,String> env) {
		for(Entry<String,String> e : j.getVariable_List().entrySet())
		{
			env.put(e.getKey(), e.getValue());
		}
	
	}

	/** Parse command a line option for a job */
	public static void parseArgs(CommandLine cmd, PBSJob job) throws UnknownHostException {
	
		//Job name
		if (cmd.hasOption('N'))
		{
			String job_Name = cmd.getOptionValue('N');
			if (job_Name.length() > 15)
				job_Name = job_Name.substring(0, 15);
			job.setJob_Name(job_Name);
		}
		
		//Account string
		if (cmd.hasOption('A'))
		{
			job.setAccount(cmd.getOptionValue('A'));
		}
		
		//interactive
		if (cmd.hasOption('I'))
		{
			job.setInteractive(true);
			job.getVariable_List().put("PBS_ENVIRONMENT", "PBS_INTERACTIVE");
		}
		
		//job's working directory
		if (cmd.hasOption('d'))
		{
			job.getVariable_List().put("PBS_O_INITDIR", cmd.getOptionValue('d'));
		}
		
		//user hold
		if (cmd.hasOption('H'))
		{
			job.setUserHold(true);
		}
			
		//output and error paths
		if (cmd.hasOption('o'))
		{
			job.setOutput_Path(cmd.getOptionValue('o'));
		}
		if (cmd.hasOption('e'))
		{
			job.setOutput_Path(cmd.getOptionValue('e'));
		}
		
		if (cmd.hasOption('j'))
		{
			String joinOpt = cmd.getOptionValue('j');
			if (joinOpt.equals("oe") || joinOpt.equals("eo"))
			{
				job.setJoin(joinOpt);
			}else {
				System.err.println("Invalid value for -j " + joinOpt);
			}
		}
		
		//mail
		if (cmd.hasOption('m'))
		{
			String desc = cmd.getOptionValue('m');
			if (! desc.matches("^[abe]+$"))
			{
				System.err.println("Invalid value for -m " + desc);
			}
			else
			{
				job.setMailEvent(desc);
			}
		}
		if (cmd.hasOption('M'))
		{
			String desc = cmd.getOptionValue('M');
			job.setMailUser(desc);
		}
		
		//queue
		if (cmd.hasOption('q'))
		{
			job.setQueue(cmd.getOptionValue('q'));
		}
		
		//is job rerunnable
		if (cmd.hasOption('r'))
		{
			boolean yes;
			String value = cmd.getOptionValue('r');
			if (value.equals("y"))
				yes = true;
			else
				yes = false;
			job.setRerunnable(yes);
		}
	
		//job array support
		if (cmd.hasOption('t'))
		{
			if (job.getInteractive())
			{
				throw new IllegalArgumentException("-t and -I options cannot be mixed");
			}
			int slotLimit = 0;
			String[] parts = cmd.getOptionValue('t').split("%", 2);
			if (parts.length > 1)
			{
				slotLimit = Integer.parseInt(parts[1]);
			}
			List<int[]> idRanges = new ArrayList<int[]>();
			String[] ranges = parts[0].split(",");
			for(String r : ranges)
			{
				String[] rs = r.split("-");
				if (rs.length == 1)
				{
					idRanges.add(new int[]{Integer.parseInt(rs[0])});
				}
				else
				{
					idRanges.add(new int[]{Integer.parseInt(rs[0]),Integer.parseInt(rs[1])});
				}
			}
			job.setArrayTaskIds(idRanges.toArray(new int[0][]), slotLimit);
		}
	
		//shell
		if (cmd.hasOption('S'))
		{
			job.setShell(cmd.getOptionValue('S'));
		}
	
		//build the resource list
		Map<String,String> res = job.getResource_List();
		if (cmd.hasOption('l'))
			for(String l_value : cmd.getOptionValues('l'))
			{
				String[] kvs = l_value.split(",");
				for(String kv_ : kvs)
				{
					String[] kv = kv_.split("=", 2);
					res.put(kv[0], kv[1]);
				}
			}
	
		if (cmd.hasOption('V'))
		{
			job.getVariable_List().putAll(System.getenv());
		}
	
		if (cmd.hasOption('v'))
		{
			for(String varDeclarations : cmd.getOptionValues('v'))
			{
				for(String v : varDeclarations.split(","))
				{
					if (v.contains("="))
					{
						String[] kv = v.split("=", 2);
						job.getVariable_List().put(kv[0], kv[1]);
					}
					else
					{
						job.getVariable_List().put(v, System.getenv(v));
					}
				}
			}
		}
	}

	/** Set the default options for a job that need other expressions */
	public static void finaliseJob(PBSJob job) throws Exception 
	{
		String job_Name = job.getJob_Name();
		String cwd = job.getVariable_List().get("PBS_O_WORKDIR");
		String hostname = Utils.getHostname();
		
		//set the default output and error paths
		//resolve to hostnames if necessary
		if (job.getOutput_Path() == null)
		{
			job.setOutput_Path(hostname + ":" + cwd + File.separatorChar + job_Name + ".o");
		}
		else
		{
			job.setOutput_Path(hostname + ":" + job.getOutput_Path());
		}
		if (job.getError_Path() == null)
		{
			job.setError_Path(hostname + ":" + cwd + File.separatorChar + job_Name + ".e");
		}
		else
		{
			job.setError_Path(hostname + ":" + job.getError_Path());
		}
		job.getVariable_List().put("PBS_O_QUEUE", job.getQueue());
		job.getVariable_List().put("PBS_JOBNAME", job_Name);
		
		for (String k : BLACKLIST_VARS)
		{
			job.getVariable_List().remove(k);
		}
		for (String k : REMOVE_VARS)
		{
			job.getVariable_List().put(k, "");
		}
		
		if (job.getMailUser() == null)
			job.setMailUser(job.getEuser());
		
	}
	
	/** Run some sanity checks on the job specification. */
	public static void verifyJob(PBSJob job) throws Exception
	{
		//(1) if the PATH env var is overridden, we verify that /bin 
		// and /usr/bin are present, as YARN shell scripts will fail without these.
		String path = job.getVariable_List().get("PATH");
		if (path != null)
		{
			String[] components = path.split(":");
			boolean foundBin = false;
			boolean foundUsrBin = false;
			for(String c : components)
			{
				if (c.equals("/bin") || c.equals("/bin/"))
					foundBin = true;
				if (c.equals("/usr/bin") || c.equals("/usr/bin/"))
					foundUsrBin = true;
			}
			if (! foundBin)
				throw new IllegalArgumentException("PATH was overridden, but /bin not found");
			if (! foundUsrBin)
				throw new IllegalArgumentException("PATH was overridden, but /usr/bin not found");
		}
		
		//(2) check filenames do not contain colons, and produce warnings for spaces too
		String hostname = Utils.getHostname();
		for(String jobOutputFile : new String[]{job.getOutput_Path(), job.getError_Path()})
		{
			String file = jobOutputFile.replaceAll("^" + hostname + ":", "");
			if (file.contains(":"))
				throw new IllegalArgumentException("Job output files with colons not yet supported: " + file);
			if (file.contains(" "))
				Log.warn("Job output files with spaces might not be supported: " + file);
		}
	}
	
	
	/** create a new job */
	public static PBSJob createNewJob(String defaultJobName) throws Exception {
	
		String username = System.getProperty("user.name");
		String hostname = Utils.getHostname();
		String job_Name = defaultJobName;
		
		String egroup = Utils.getGroup();
		final String cwd = System.getProperty("user.dir");
	
		//now build the job
		PBSJob j = new PBSJob(
				username + '@' + hostname,
				null,
				null,
				username,
				egroup,
				new String[0],
				job_Name,
				null, null);
		
		//now build the environment
		Map<String,String> env = j.getVariable_List();
		for(String varName : JobUtils.COPY_VARS)
		{
			if (System.getenv(varName) != null)
				env.put("PBS_O_"+ varName, System.getenv(varName));
		}
		for(String varName : JobUtils.COPY_PLAIN_VARS)
		{
			if (System.getenv(varName) != null)
				env.put(varName, System.getenv(varName));
		}
	
		env.put("PBS_ENVIRONMENT", "PBS_BATCH");
		env.put("PBS_O_HOST", hostname);
		env.put("PBS_O_WORKDIR", cwd);
	
		return j;
	}

	

}
