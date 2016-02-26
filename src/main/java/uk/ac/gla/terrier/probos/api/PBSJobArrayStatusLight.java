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
			String jobOwner, String timeUse, char state, String queue, int[] arrayIds, char[] states /*, String[] walltimes */) {
		super(jobId, true, jobName, jobOwner, timeUse, state, queue);
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
