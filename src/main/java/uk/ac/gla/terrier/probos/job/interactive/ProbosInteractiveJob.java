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

package uk.ac.gla.terrier.probos.job.interactive;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.shell.InvertedShell;
import org.apache.sshd.server.shell.InvertedShellWrapper;
import org.apache.sshd.server.shell.ProcessShellFactory;

import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.api.PBSInteractiveClient;
import uk.ac.gla.terrier.probos.job.ProbosJobService;

public class ProbosInteractiveJob extends ProbosJobService  {

	public ProbosInteractiveJob()
	{
		super();
	}
	
	@Override
	protected SshServer getSSHServer(String secret) {		
		SshServer sshd = super.getSSHServer(secret);
		String shellName = System.getenv("PBS_O_SHELL");
		sshd.setShellFactory(new ProcessShellFactory(
				"/bin/bash", "-c", "export HOME=$PBS_O_HOME; cd $PBS_O_WORKDIR; "+shellName + " -i -l" )
		{			
			@Override
			protected InvertedShell createInvertedShell() {
				LOG.info("Starting InvertedShell..." + this.getCommand().toString());
				return super.createInvertedShell();
			}

			@Override
		    public Command create() {
		        return new InvertedShellWrapper(createInvertedShell())
		        {
					@Override
					public synchronized void destroy() throws Exception {
						super.destroy();
						running.set(false);
					}		        	
		        };
		    }
		});
		return sshd;
	}
	
	@Override
	protected void informController(final String secret, int port,
			int jobId, PBSInteractiveClient client) throws IOException {
		client.interactiveDaemonStarted(jobId, Utils.getHostname(), port, secret);
	}

	public static void main(String[] _args) throws Exception
	{
		int rc = ToolRunner.run(new Configuration(), new ProbosInteractiveJob(), _args);
		System.exit(rc);
	}
}
