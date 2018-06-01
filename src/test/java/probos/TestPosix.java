package probos;

import static org.junit.Assert.*;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;

import org.junit.Test;

public class TestPosix {
	//static final 

	@Test public void testGroup()
	{
		POSIX posix = POSIXFactory.getPOSIX();
		System.err.println(posix.isNative());
		String egroup = posix.getgrgid(posix.getegid()).getName();
		assertNotNull(egroup);
	}
}
