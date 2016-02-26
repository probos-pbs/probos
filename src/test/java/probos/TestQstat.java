package probos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import uk.ac.gla.terrier.probos.PBSClientFactory;
import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.api.PBSJobArrayStatusLight;
import uk.ac.gla.terrier.probos.api.PBSJobStatusLight;
import uk.ac.gla.terrier.probos.api.PBSJobStatusNodes;
import uk.ac.gla.terrier.probos.cli.qstat;

public class TestQstat {

	static PBSClient oneJob = new UtilsForTest.PBSClientBase() {
		@Override
		public int[] getJobs() {
			return new int[]{1};
		}
		
		@Override
		public PBSJobStatusLight getJobStatus(int jobId, int requestType)
				throws Exception {
			if (jobId == 1)
				if (requestType == 0)
					return new PBSJobStatusLight(1, false, "testJob", System.getProperty("user.name"), "00:01:59", 'R', "default");
				else if (requestType == 1)
					return new PBSJobStatusNodes(1, false, "testJob", System.getProperty("user.name"), "00:01:59", 'R', "default", 
						"node01");
			return new PBSJobStatusLight(1, false, null, null, null, '?', null);
		}
	};
	
	static PBSClient oneArrayJob = new UtilsForTest.PBSClientBase() {
		@Override
		public int[] getJobs() {
			return new int[]{1};
		}
		
		@Override
		public PBSJobStatusLight getJobStatus(int jobId, int requestType)
				throws Exception {
			if (jobId == 1)
				if (requestType == 0)
					return new PBSJobStatusLight(1, true, "testJob", System.getProperty("user.name"), "00:01:59", 'R', "default");
				else if (requestType == 1)
					return new PBSJobStatusNodes(1, true, "testJob", System.getProperty("user.name"), "00:01:59", 'R', "default", 
						"node01");
				else if (requestType == 3)
					return new PBSJobArrayStatusLight(1, "testJob", System.getProperty("user.name"), "00:01:59", 'R', "default", 
							new int[]{5,6,7}, new char[]{'C', 'R', 'Q'});
			return new PBSJobStatusLight(1, false, null, null, null, '?', null);
		}
	};
	
	int lastExitCode = Integer.MAX_VALUE;
	
	protected String performQstat(PBSClient c, String[] args) throws Exception
	{
		PBSClientFactory.forceClient(c);
		PrintStream originalStdOut = System.out;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		System.setOut(new PrintStream(baos));
		lastExitCode = new qstat().run(args);
		String lines = new String(baos.toByteArray());
		
		System.setOut(originalStdOut);
		PBSClientFactory.forceClient(null);
		return lines;
	}
	

	@Test public void testSingleJob() throws Exception
	{
		PBSClient c = oneJob;
		String lines;
		
		lines = performQstat(c, new String[0]);
		assertEquals(0, lastExitCode);
		assertTrue(lines.contains("testJob"));
		
		lines = performQstat(c, new String[]{"-r"});
		assertEquals(0, lastExitCode);
		assertTrue(lines.contains("testJob"));
		
		lines = performQstat(c, new String[]{"1"});
		assertEquals(0, lastExitCode);
		assertTrue(lines.contains("testJob"));
		
		lines = performQstat(c, new String[]{"-n", "1"});
		assertEquals(0, lastExitCode);
		assertTrue(lines.contains("testJob"));
		assertTrue(lines.contains("\nnode01\n"));
		
		lines = performQstat(c, new String[]{"1[]"});
		assertEquals(0, lastExitCode);
		assertTrue(lines, lines.contains("testJob"));
		
		lines = performQstat(c, new String[]{"2"});
		assertEquals(1, lastExitCode);
		assertFalse(lines.contains("testJob"));
	}
	
	@Test public void testSingleArrayJob() throws Exception
	{
		PBSClient c = oneArrayJob;
		String lines;
		
		lines = performQstat(c, new String[0]);
		assertEquals(0, lastExitCode);
		assertTrue(lines.contains("testJob"));
		
		lines = performQstat(c, new String[]{"1"});
		assertEquals(0, lastExitCode);
		assertTrue(lines.contains("testJob"));
		
		lines = performQstat(c, new String[]{"-n", "1"});
		assertEquals(0, lastExitCode);
		assertTrue(lines.contains("testJob"));
		assertTrue(lines.contains("\nnode01\n"));
		
		lines = performQstat(c, new String[]{"1[]"});
		assertEquals(0, lastExitCode);
		assertTrue(lines, lines.contains("testJob"));
		
		lines = performQstat(c, new String[]{"-t", "1[]"});
		assertEquals(0, lastExitCode);
		assertTrue(lines, lines.contains("testJob"));
		assertTrue(lines, lines.contains("1[]"));
		assertTrue(lines, lines.contains("1[5]"));
		assertTrue(lines, lines.contains("1[6]"));
		assertTrue(lines, lines.contains("1[7]"));	
		
		//System.err.println("match = "+  grepOne(lines,"1\\[5\\]"));
		assertEquals(1,  grep(lines, "1\\[5\\]").length);
		assertTrue(lines, grep(lines, "1\\[5\\]")[0].contains("C"));
		assertTrue(lines, grep(lines, "1\\[6\\]")[0].contains("R"));
		assertTrue(lines, grep(lines, "1\\[7\\]")[0].contains("Q"));
		
		assertTrue(lines, lines.contains("testJob[5]"));
		assertTrue(lines, lines.contains("testJob[6]"));
		assertTrue(lines, lines.contains("testJob[7]"));
		
		lines = performQstat(c, new String[]{"2"});
		assertEquals(1, lastExitCode);
		assertFalse(lines.contains("testJob"));
	}
	
	static String[] grep(String _lines, String match) {
		String[] lines = _lines.split("\n");
		List<String> m = new ArrayList<String>();
		for(String l : lines)
		{
			if (l.matches("^.*"+match+".*$"))
				m.add(l);
		}
		return m.toArray(new String[m.size()]);
	}
	
}
