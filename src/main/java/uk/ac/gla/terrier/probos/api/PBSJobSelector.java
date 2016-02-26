package uk.ac.gla.terrier.probos.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import uk.ac.gla.terrier.probos.Utils;

/** Defines how jobs can be selected for qselect */
public class PBSJobSelector implements Writable {

	PBSJobAttribute name;
	String resource;
	String value;
	PBSJobAttributeOperand op;
	
	public PBSJobSelector() {}
	
	public PBSJobSelector(PBSJobAttribute name, String resource, String value,
			PBSJobAttributeOperand op) {
		super();
		this.name = name;
		this.resource = resource;
		this.value = value;
		this.op = op;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Utils.writeStringOrNull(out, name != null ? name.name() : null);
		Utils.writeStringOrNull(out, resource);
		Utils.writeStringOrNull(out, value);
		out.writeUTF(op.name());
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		String enumValue = Utils.readStringOrNull(in);
		if (enumValue == null)
			name = null;
		else
			name = Enum.valueOf(PBSJobAttribute.class, enumValue);
		resource = Utils.readStringOrNull(in);
		value = Utils.readStringOrNull(in);		
		op = Enum.valueOf(PBSJobAttributeOperand.class, in.readUTF());
	}
	
	public boolean intMatch(int value)
	{
		boolean rtr = false;
		final int myValue = Integer.parseInt(this.value);
		switch (op)
		{
		case EQ:
			rtr = value == myValue;
			break;
		case GE:
			rtr = myValue >= value;
			break;
		case GT:
			rtr = myValue > value;
			break;
		case LE:
			rtr = myValue <= value;
			break;
		case LT:
			rtr = myValue < value;
			break;
		case NE:
			rtr = value != myValue;
			break;
		default:
			throw new IllegalArgumentException("Cannot compare int attribute values using operator " + op.toString());		
		};
		return rtr;
	}
	
	public boolean stringMatch(String attributeValue)
	{
		if (attributeValue == null)
			return false;
		boolean rtr = false;
		switch (op)
		{
		case EQ:
			rtr = (attributeValue.equals(this.value));
			break;
		case NE:
			rtr = ! (attributeValue.equals(this.value));
			break;
		case RE:
			rtr = attributeValue.matches(this.value);
			break;
		default:
			throw new IllegalArgumentException("Cannot compare string attribute values using operator " + op.toString());	
		}
		return rtr;
	}
	
	public boolean matches(PBSClient server, int jobId, PBSJob j) throws Exception {
		boolean rtr = false;
		switch (name)
		{
		case ATTR_A:
			rtr = stringMatch(j.getAccount());
			break;
		case ATTR_N:
			rtr = stringMatch(j.getJob_Name());
			break;
		case ATTR_S:
			rtr = stringMatch(j.getShell());
			break;
		//case ATTR_a:
		//	break;
		case ATTR_e:
			rtr = stringMatch(j.getError_Path());
			break;
		case ATTR_g:
			rtr = stringMatch(j.getEgroup());
			break;
		//case ATTR_h:
		//	break;
		case ATTR_j:
			rtr = stringMatch(j.getJoin());
			break;
		//case ATTR_k:
		//	break;
		case ATTR_l:
			String resourceValue = j.getResource_List().get(this.resource);
			rtr = stringMatch(resourceValue);
			break;
		case ATTR_m:
			rtr = stringMatch(j.getMailEvent());
			break;
		case ATTR_M:
			rtr = stringMatch(j.getMailUser());
			break;
		case ATTR_o:
			rtr = stringMatch(j.getOutput_Path());
			break;
		//case ATTR_p:
		//	break;
		case ATTR_q:
			rtr = stringMatch(j.getQueue());
			break;
		//case ATTR_r:
		//	break;
		case ATTR_u:
			rtr = stringMatch(j.getEuser());
			break;
		//case ATTR_v:
		//	break;
		case ATTR_state:
			char state = server.getJobStatus(jobId, 0).getState();
			rtr = stringMatch(String.valueOf(state));
			break;
		default:
			break;
		
		}
		return rtr;
	}
	
}
