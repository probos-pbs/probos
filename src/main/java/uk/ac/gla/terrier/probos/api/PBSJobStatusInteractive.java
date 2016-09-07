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

public class PBSJobStatusInteractive extends PBSJobStatusLight {

	String hostname;
	int port;
	String secret;
	
	public PBSJobStatusInteractive() {
		super();
	}

	public PBSJobStatusInteractive(int jobId, boolean isArray, String jobName,
			String jobOwner, String timeUse, char state, String queue, String trackingURL,
			String hostname, int port, String secret) {
		super(jobId, isArray, jobName, jobOwner, timeUse, state, queue, trackingURL);
		this.hostname = hostname;
		this.port = port;
		this.secret = secret;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		hostname = Utils.readStringOrNull(in);
		port = in.readInt();
		secret = Utils.readStringOrNull(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		Utils.writeStringOrNull(out, hostname);
		out.writeInt(port);
		Utils.writeStringOrNull(out, secret);
	}

	public String getHostname() {
		return hostname;
	}

	public int getPort() {
		return port;
	}

	public String getSecret() {
		return secret;
	}

}
