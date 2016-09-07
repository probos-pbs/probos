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
import java.util.Set;

import org.apache.hadoop.io.Writable;

import uk.ac.gla.terrier.probos.Utils;

/** Contains information a nodes from YARN for display by pbsnodes */
public class PBSNodeStatus implements Writable {

	String nodeName;
	String state;
	String status;
	String tracker;
	String health;
	String rack;
	String yarnState;
	String[] nodelLabels;
	int[] jobs;

	int np;
	
	public PBSNodeStatus(){}

	public PBSNodeStatus(String nodeName, String state, String status, int[] jobs,
			String tracker, String health, String rack, String yarnState, int np, Set<String> nodeLabels) {
		super();
		this.nodeName = nodeName;
		this.state = state;
		this.status = status;
		this.jobs = jobs;
		this.tracker = tracker;
		this.health = health;
		this.rack = rack;
		this.yarnState = yarnState;
		this.np = np;
		this.nodelLabels = nodeLabels.toArray(new String[nodeLabels.size()]);
	}

	public String getNodeName() {
		return nodeName;
	}

	public String getState() {
		return state;
	}

	public String getStatus() {
		return status;
	}
	
	public int[] getJobs() {
		return jobs;
	}

	public String getTracker() {
		return tracker;
	}

	public String getHealth() {
		return health;
	}

	public String getRack() {
		return rack;
	}
	
	public String getYarnState() {
		return yarnState;
	}

	public int getNp() {
		return np;
	}
	
	public String[] getNodeLabels() {
		return nodelLabels;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Utils.writeStringOrNull(out, nodeName);
		Utils.writeStringOrNull(out, state);
		Utils.writeStringOrNull(out, status);
		out.writeInt(jobs.length);
		for(int j : jobs)
			out.writeInt(j);
		Utils.writeStringOrNull(out, tracker);
		Utils.writeStringOrNull(out, health);
		Utils.writeStringOrNull(out, rack);
		Utils.writeStringOrNull(out, yarnState);
		out.writeInt(np);
		out.writeInt(nodelLabels.length);
		for(String l : nodelLabels)
			out.writeUTF(l);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nodeName = Utils.readStringOrNull(in);
		state = Utils.readStringOrNull(in);
		status = Utils.readStringOrNull(in);
		jobs = new int[in.readInt()];
		for(int i=0;i<jobs.length;i++)
			jobs[i] = in.readInt();
		tracker = Utils.readStringOrNull(in);
		health = Utils.readStringOrNull(in);
		rack = Utils.readStringOrNull(in);
		yarnState = Utils.readStringOrNull(in);
		np = in.readInt();
		nodelLabels = new String[in.readInt()];
		for(int i=0;i<nodelLabels.length;i++)
			nodelLabels[i] = in.readUTF();
	}
	
	
	
}
