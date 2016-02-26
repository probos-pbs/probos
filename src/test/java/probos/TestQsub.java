package probos;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import uk.ac.gla.terrier.probos.api.PBSJob;
import uk.ac.gla.terrier.probos.cli.qsub;

public class TestQsub {

	@Test public void testCommandLineWithJobScript() throws Exception
	{
		qsub qs = new qsub();
		File jobScript = UtilsForTest.createJobScript("hostname");
		PBSJob job = qs.createJob(new String[]{"-N", "hostname", jobScript.toString()});
		assertEquals("hostname", job.getJob_Name());
	}
	
	@Test public void testCommandLineWithoutJobScript() throws Exception
	{
		qsub qs = new qsub();
		InputStream in = System.in;
		System.setIn(IOUtils.toInputStream("hostname", "UTF-8"));		
		PBSJob job = qs.createJob(new String[]{"-N", "hostname"});
		assertEquals("hostname", job.getJob_Name());
		System.setIn(in);
	}
	
}
