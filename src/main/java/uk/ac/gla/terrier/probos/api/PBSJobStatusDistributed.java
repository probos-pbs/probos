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
			String jobOwner, String timeUse, char state, String queue,
			String[] hostnames, int[] ports, String secret) {
		super(jobId, isArray, jobName, jobOwner, timeUse, state, queue);
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
