package probos;

import static org.junit.Assert.assertTrue;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.Arrays;

import org.junit.Test;

import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.api.PBSJob;
import uk.ac.gla.terrier.probos.api.PBSJobAttribute;
import uk.ac.gla.terrier.probos.api.PBSJobAttributeOperand;
import uk.ac.gla.terrier.probos.api.PBSJobSelector;
import uk.ac.gla.terrier.probos.cli.qselect;
import uk.ac.gla.terrier.probos.controller.ControllerServer;
import uk.ac.gla.terrier.probos.controller.ControllerServer.JobInformation;

public class TestSelectors {

	@Test public void testMatch() throws Exception
	{
		PBSJobSelector js = new PBSJobSelector(
				PBSJobAttribute.ATTR_u, 
				null, System.getProperty("user.name"), PBSJobAttributeOperand.EQ);
		PBSJob j = UtilsForTest.getSimpleJob("testJob", "hostname");
		assertTrue(js.matches(TestQstat.oneJob, 1, j));
	}
	
	@Test public void testSelectors() throws Exception
	{
		PBSClient c = TestQstat.oneJob;
		PBSJobSelector[] sel;
		TIntObjectHashMap<JobInformation> jobArray = new TIntObjectHashMap<JobInformation>();
		JobInformation ji = new JobInformation(1, UtilsForTest.getSimpleJob("testJob", "hostname"));
		jobArray.put(1, ji);
		
		sel = qselect.parseSelectCommandLine(new String[]{"-u", "notme"});
		assertDeepEquals(new int[]{}, ControllerServer.selectJobs(jobArray.iterator(), sel, c));
		
		sel = qselect.parseSelectCommandLine(new String[]{"-u", System.getProperty("user.name")});
		assertDeepEquals(new int[]{1}, ControllerServer.selectJobs(jobArray.iterator(), sel, c));
		
		sel = qselect.parseSelectCommandLine(new String[]{"-s", "R"});
		assertDeepEquals(new int[]{1}, ControllerServer.selectJobs(jobArray.iterator(), sel, c));
		
		sel = qselect.parseSelectCommandLine(new String[]{"-s", "Q"});
		assertDeepEquals(new int[]{}, ControllerServer.selectJobs(jobArray.iterator(), sel, c));
		
		sel = qselect.parseSelectCommandLine(new String[]{"-N", "testJob"});
		assertDeepEquals(new int[]{1}, ControllerServer.selectJobs(jobArray.iterator(), sel, c));
		
		sel = qselect.parseSelectCommandLine(new String[]{"-N", "testJo"});
		assertDeepEquals(new int[]{}, ControllerServer.selectJobs(jobArray.iterator(), sel, c));
		
		sel = qselect.parseSelectCommandLine(new String[]{"-N", "testJob12"});
		assertDeepEquals(new int[]{}, ControllerServer.selectJobs(jobArray.iterator(), sel, c));
		
		sel = qselect.parseSelectCommandLine(new String[]{"-A", "blabla"});
		assertDeepEquals(new int[]{}, ControllerServer.selectJobs(jobArray.iterator(), sel, c));
	}

	private void assertDeepEquals(int[] i1, int[] i2) {
		assertTrue("Expected " + Arrays.toString(i1)+ " but found " + Arrays.toString(i2), Arrays.equals(i1, i2));
	}
	
}
