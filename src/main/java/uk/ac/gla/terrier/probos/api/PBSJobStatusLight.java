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

import org.apache.hadoop.io.Writable;

import uk.ac.gla.terrier.probos.Utils;

/** Basic information about a job for qstat */
public class PBSJobStatusLight implements Writable {

	int FORMAT_VERSION = 0;
	int jobId;
	boolean array;
	String jobName;
	String jobOwner;
	String timeUse;
	char state;
	String queue;
	String trackingURL = null;
	
	public PBSJobStatusLight(){}
	
	public PBSJobStatusLight(int jobId, boolean isArray, String jobName, String jobOwner,
			String timeUse, char state, String queue, String trackingURL) {
		super();
		this.jobId = jobId;
		this.array = isArray;
		this.jobName = jobName;
		this.jobOwner = jobOwner;
		this.timeUse = timeUse;
		this.state = state;
		this.queue = queue;
		this.trackingURL = trackingURL;
	}	
	public int getJobId() {
		return jobId;
	}
	
	public boolean hasArray()
	{
		return array;
	}
	
	public String getJob_Name() {
		return jobName;
	}
	public String getJob_Owner() {
		return jobOwner;
	}
	public String getTimeUse() {
		return timeUse;
	}
	
	public String getTrackingURL()
	{
		return trackingURL;
	}
	
	public char getState() {
		return state;
	}
	public String getQueue() {
		return queue;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {

		if (in.readByte() != FORMAT_VERSION)
		{
			throw new IllegalArgumentException("Status format is unsupported");
		}
		jobId = in.readInt();
		array = in.readBoolean();
		jobName =  Utils.readStringOrNull(in);
		jobOwner =  Utils.readStringOrNull(in);
		queue = Utils.readStringOrNull(in);	
		timeUse = Utils.readStringOrNull(in);
		trackingURL = Utils.readStringOrNull(in);
		state = in.readChar();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(FORMAT_VERSION);
		out.writeInt(jobId);
		out.writeBoolean(array);
		Utils.writeStringOrNull(out, jobName);
		Utils.writeStringOrNull(out, jobOwner);
		Utils.writeStringOrNull(out, queue);
		Utils.writeStringOrNull(out, timeUse);
		Utils.writeStringOrNull(out, trackingURL);
		out.writeChar(state);
	}
}
