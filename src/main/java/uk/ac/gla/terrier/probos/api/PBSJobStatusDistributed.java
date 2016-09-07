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

package uk.ac.gla.terrier.probos.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import uk.ac.gla.terrier.probos.Utils;

public class PBSJobStatusDistributed extends PBSJobStatusLight {

	String[] hostnames;
	int[] ports;
	String secret;
	
	public PBSJobStatusDistributed() {
		super();
	}

	public PBSJobStatusDistributed(int jobId, boolean isArray, String jobName,
			String jobOwner, String timeUse, char state, String queue, String trackingURL,
			String[] hostnames, int[] ports, String secret) {
		super(jobId, isArray, jobName, jobOwner, timeUse, state, queue, trackingURL);
		this.hostnames = hostnames;
		this.ports = ports;
		this.secret = secret;
		assert this.hostnames.length == this.ports.length;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		int l = in.readInt();
		hostnames = new String[l];
		ports = new int[l];
		for (int i=0;i<l;i++)
		{
			hostnames[i] = Utils.readStringOrNull(in);
			ports[i] = in.readInt();
		}		
		secret = Utils.readStringOrNull(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(hostnames.length);
		for(int i=0;i<hostnames.length;i++)
		{
			Utils.writeStringOrNull(out, hostnames[i]);
			out.writeInt(ports[i]);
		}
		Utils.writeStringOrNull(out, secret);
	}

	public String[] getHostnames() {
		return hostnames;
	}

	public int[] getPorts() {
		return ports;
	}

	public String getSecret() {
		return secret;
	}

}
