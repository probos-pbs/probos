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
	String trackingURL = null;
	
	public PBSJobStatusFat(){}
	
	public PBSJobStatusFat(int jobId, boolean isArray, String jobName, String jobOwner,
			String timeUse, char state, String queue, String nodes, PBSJob job, 
			String mContainer, String[] tContainers, String trackingURL, String yarnAppId) {
		super(jobId, isArray, jobName, jobOwner, timeUse, state, queue, nodes);
		this.job = job;
		this.masterContainer = mContainer;
		this.taskContainerIds = tContainers;
		this.trackingURL = trackingURL;
		this.yarnAppId = yarnAppId;
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
	
}
