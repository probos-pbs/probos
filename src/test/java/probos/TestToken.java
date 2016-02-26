package probos;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Test;

import uk.ac.gla.terrier.probos.api.ProbosDelegationTokenIdentifier;

public class TestToken {

	@Test public void testServiceLoader()
	{
		boolean found = false;
		Iterator<TokenIdentifier> i = ServiceLoader.load(TokenIdentifier.class).iterator();
		while(i.hasNext())
		{
			TokenIdentifier ti = i.next();
			if (ti.getClass().equals(ProbosDelegationTokenIdentifier.class))
				found = true;
		}
		assertTrue(found);
	}
	
}
