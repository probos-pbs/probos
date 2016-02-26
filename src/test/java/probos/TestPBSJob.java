package probos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.junit.Test;

import uk.ac.gla.terrier.probos.api.PBSJob;

public class TestPBSJob {
	
	public void checkWritable(PBSJob job1) throws Exception {
		File jobFile = File.createTempFile("test", ".sh");
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(jobFile));
		job1.write(dos);
		dos.close();
		
		
		PBSJob job2 = new PBSJob();
		
		DataInputStream dis = new DataInputStream(new FileInputStream(jobFile));
		job2.readFields(dis);
		dis.close();
		
		assertEquals(job1.getJob_Name(), job2.getJob_Name());
		
	}
	
	@Test
	public void testJobCreation() throws Exception {
		PBSJob job1 = UtilsForTest.getSimpleJob("testHostname", "hostname");
		assertNotNull(job1.getVariable_List().get("PBS_O_HOST"));
		assertFalse(job1.getOutput_Path().startsWith("null:"));
		assertFalse(job1.getError_Path().startsWith("null:"));
	}
	
	@Test
	public void testSimpleWritable() throws Exception {
		
		PBSJob job1 = UtilsForTest.getSimpleJob("testHostname", "hostname");
		checkWritable(job1);
	}

}
