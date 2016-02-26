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
			String jobOwner, String timeUse, char state, String queue,
			String hostname, int port, String secret) {
		super(jobId, isArray, jobName, jobOwner, timeUse, state, queue);
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
