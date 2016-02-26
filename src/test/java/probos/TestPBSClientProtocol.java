package probos;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.ipc.ProtocolInfo;
import org.junit.Test;

import uk.ac.gla.terrier.probos.api.PBSClient;

public class TestPBSClientProtocol {

	
	@Test public void testAnnotation()
	{
		assertEquals(PBSClient.class.getName(), PBSClient.class.getAnnotation(ProtocolInfo.class).protocolName());
	}
}
