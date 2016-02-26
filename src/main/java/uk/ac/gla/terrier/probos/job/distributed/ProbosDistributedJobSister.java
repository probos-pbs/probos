package uk.ac.gla.terrier.probos.job.distributed;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.CommandFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.scp.ScpCommandFactory;
import org.apache.sshd.server.shell.ProcessShellFactory;

import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.api.PBSInteractiveClient;
import uk.ac.gla.terrier.probos.job.ProbosJobService;

public class ProbosDistributedJobSister extends ProbosJobService {

	final String shell = System.getenv("SHELL");
	
	@Override
	protected SshServer getSSHServer(String secret) {
		SshServer sshd = super.getSSHServer(secret);
		sshd.setCommandFactory(
				new ScpCommandFactory.Builder().withDelegate(new CommandFactory() {
				      public Command createCommand(String command) {
				    	  String[] finalCmd = createShellCommand(command, shell);
				    	  LOG.info("Running cmd="+ Arrays.toString(finalCmd));
					      return new ProcessShellFactory(finalCmd).create();
					   }
				})
			.build());
		return sshd;
	}
	
	@Override
	protected void informController(final String secret, int port,
			int jobId, PBSInteractiveClient client) throws IOException {
		client.distributedDaemonStarted(jobId, Utils.getHostname(), port, secret);
	}
	
	public static void main(String[] _args) throws Exception
	{
		int rc = ToolRunner.run(new Configuration(), new ProbosDistributedJobSister(), _args);
		System.exit(rc);
	}

}
