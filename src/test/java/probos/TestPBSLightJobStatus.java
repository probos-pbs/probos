package probos;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.junit.Test;

import uk.ac.gla.terrier.probos.api.PBSJobStatusLight;
public class TestPBSLightJobStatus {

	protected void testWritable(PBSJobStatusLight status) throws Exception
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		status.write(dos);
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
		PBSJobStatusLight newStatus = status.getClass().newInstance();
		newStatus.readFields(dis);
		assertEquals(status.getJobId(), newStatus.getJobId());
		assertEquals(status.getJob_Name(), newStatus.getJob_Name());
		assertEquals(status.getJob_Owner(), newStatus.getJob_Owner());
		assertEquals(status.getQueue(), newStatus.getQueue());
		assertEquals(status.getTimeUse(), newStatus.getTimeUse());
		assertEquals(status.getState(), newStatus.getState());
		
	}
	
	@Test public void testWritable() throws Exception
	{
		testWritable(new PBSJobStatusLight());
		testWritable(new PBSJobStatusLight(1, false, "test", "me", "0", 'E', "default"));
		
	}
	
}
