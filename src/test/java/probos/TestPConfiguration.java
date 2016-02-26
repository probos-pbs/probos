package probos;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import uk.ac.gla.terrier.probos.PConfiguration;

public class TestPConfiguration {

	@Test public void testDefault()
	{
		PConfiguration pconf = new PConfiguration();
		assertEquals("localhost", pconf.get(PConfiguration.KEY_CONTROLLER_HOSTNAME));	
	}
}
