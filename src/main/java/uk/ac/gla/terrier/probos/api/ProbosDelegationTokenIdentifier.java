package uk.ac.gla.terrier.probos.api;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

public class ProbosDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {

	public static final Text KIND_NAME = new Text("PROBOS_DELEGATION_TOKEN");
	
	public ProbosDelegationTokenIdentifier(){}
	
	public ProbosDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
	    super(owner, renewer, realUser);
	}
	
	@Override
	public Text getKind() {
		return KIND_NAME;
	}

}