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

/** Exhaustive information about a job */
public class PBSJobStatusFat extends PBSJobStatusNodes {

	PBSJob job;
	String masterContainer = null;
	String yarnAppId = null;
	String[] taskContainerIds = null;
	String diagnosticMsg = null;
	
	public PBSJobStatusFat(){}
	
	public PBSJobStatusFat(int jobId, boolean isArray, String jobName, String jobOwner,
			String timeUse, char state, String queue, String execnodes, String masternode, PBSJob job, 
			String mContainer, String[] tContainers, String trackingURL, String yarnAppId, String diagnosticMsg) {
		super(jobId, isArray, jobName, jobOwner, timeUse, state, queue, trackingURL, execnodes, masternode);
		this.job = job;
		this.masterContainer = mContainer;
		this.taskContainerIds = tContainers;
		this.trackingURL = trackingURL;
		this.yarnAppId = yarnAppId;
		this.diagnosticMsg = diagnosticMsg;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		
		if (in.readBoolean())
		{
			job = new PBSJob();
			job.readFields(in);
		}
		masterContainer = Utils.readStringOrNull(in);
		int l = in.readInt();
		taskContainerIds = new String[l];
		for(int i=0;i<l;i++)
			taskContainerIds[i] = Utils.readStringOrNull(in);
		trackingURL = Utils.readStringOrNull(in);
		yarnAppId = Utils.readStringOrNull(in);
		diagnosticMsg = Utils.readStringOrNull(in);
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		if (job != null)
		{
			out.writeBoolean(true);
			job.write(out);
		} else {
			out.writeBoolean(false);
		}
		Utils.writeStringOrNull(out, masterContainer);
		out.writeInt(taskContainerIds.length);
		for(String id : taskContainerIds)
			Utils.writeStringOrNull(out, id);
		Utils.writeStringOrNull(out, trackingURL);
		Utils.writeStringOrNull(out, yarnAppId);
		Utils.writeStringOrNull(out, diagnosticMsg);
	}
	
	public String getMasterContainerId() {
		return masterContainer;
	}
	
	public String[] getTaskContainerIds() {
		return taskContainerIds;
	}
	
	public String getTrackingURL() {
		return trackingURL;
	}
	
	public PBSJob getJob()
	{
		return job;
	}
	
	public String getApplicationId()
	{
		return this.yarnAppId;
	}
	
	public String getDiagnosticMessage()
	{
		return this.diagnosticMsg;
	}
	
}
