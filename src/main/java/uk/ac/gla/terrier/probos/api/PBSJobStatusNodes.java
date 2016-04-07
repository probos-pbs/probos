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
