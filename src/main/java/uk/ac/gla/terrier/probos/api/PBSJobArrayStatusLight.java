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

public class PBSJobArrayStatusLight extends PBSJobStatusLight {
	
	int[] arrayIds;
	char[] states;
	//String[] walltimes;

	public PBSJobArrayStatusLight() {
		super();
	}
	
	public PBSJobArrayStatusLight(int jobId, String jobName,
			String jobOwner, String timeUse, char state, String queue, String trackingURL, int[] arrayIds, char[] states /*, String[] walltimes */) {
		super(jobId, true, jobName, jobOwner, timeUse, state, queue, trackingURL);
		this.arrayIds = arrayIds;
		this.states = states;
		//this.walltimes = walltimes;
	}
	
	public int[] getArrayIds() {
		return arrayIds;
	}
	
	public char[] getArrayStates() {
		return states;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		int l = in.readInt();
		arrayIds = new int[l];
		states = new char[l];
		for(int i=0;i<l;i++)
			arrayIds[i] = in.readInt();
		for(int i=0;i<l;i++)
			states[i] = in.readChar();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(arrayIds.length);
		for (int i : arrayIds)
			out.writeInt(i);
		for(char c : states)
			out.writeChar(c);
	}

}
