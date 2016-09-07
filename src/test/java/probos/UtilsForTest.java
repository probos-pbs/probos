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

package probos;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.ipc.ProtocolSignature;

import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.api.PBSJob;
import uk.ac.gla.terrier.probos.api.PBSJobSelector;
import uk.ac.gla.terrier.probos.api.PBSJobStatusLight;
import uk.ac.gla.terrier.probos.api.PBSNodeStatus;
import uk.ac.gla.terrier.probos.cli.qsub;

public class UtilsForTest {

	
	public static class PBSClientBase implements PBSClient {

		@Override
		public long getProtocolVersion(String protocol, long clientVersion)
				throws IOException {
			return 0;
		}

		@Override
		public ProtocolSignature getProtocolSignature(String protocol,
				long clientVersion, int clientMethodsHash) throws IOException {
			return null;
		}

		@Override
		public int submitJob(PBSJob job, byte[] source) throws IOException {
			return 0;
		}

		@Override
		public String kittenSpecification(PBSJob job, boolean luaHeaders, boolean noProbos)
				throws IOException {
			return null;
		}

		@Override
		public int killJob(int jobId, boolean purge) throws Exception {
			return 0;
		}

		@Override
		public PBSJob getJob(int jobId) {
			return null;
		}

		@Override
		public int[] getJobs() {
			return null;
		}

		@Override
		public PBSJobStatusLight getJobStatus(int jobId, int requestType)
				throws Exception {
			return null;
		}

		@Override
		public PBSNodeStatus[] getNodesStatus() throws Exception {
			return null;
		}

		@Override
		public int[] selectJobs(PBSJobSelector[] selectors) throws Exception {
			return null;
		}

		@Override
		public int releaseJob(int jobId) throws Exception {
			return 0;
		}

		@Override
		public byte[] jobLog(int jobId, int arrayId, boolean stdout, long start, boolean url)
				throws Exception {
			return null;
		}		
	}
	public static File createJobScript(String command) throws Exception
	{
		File jobScript = File.createTempFile("test", ".sh");
		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(jobScript)));
		pw.println("#!/bin/bash");
		pw.println(command);
		pw.close();
		return jobScript;
	}
	
	public static PBSJob getSimpleJob(String name, String command) throws Exception
	{
		File jobScript = createJobScript(command);
				
		qsub q = new qsub();
		PBSJob job1 = q.createJob(new String[]{"-N", name, jobScript.toString()});
		return job1;
	}
	
}
