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

/** Information about a job for qstat, including the nodes in use */
public class PBSJobStatusNodes extends PBSJobStatusLight {

	String nodes;

	public PBSJobStatusNodes() {
		super();
	}

	public PBSJobStatusNodes(int jobId, boolean isArray, String jobName, String jobOwner,
			String timeUse, char state, String queue, String trackingURL, String nodes) {
		super(jobId, isArray, jobName, jobOwner, timeUse, state, queue, trackingURL);
		this.nodes = nodes;
	}
	
	
	public String getNodes() {
		return nodes;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		nodes = Utils.readStringOrNull(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		Utils.writeStringOrNull(out, nodes);
	}

}
