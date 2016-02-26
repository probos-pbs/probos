package uk.ac.gla.terrier.probos.controller;

import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;

import uk.ac.gla.terrier.probos.api.ProbosDelegationTokenIdentifier;

public class ProbosDelegationTokenSelector extends
		AbstractDelegationTokenSelector<ProbosDelegationTokenIdentifier> {

	public ProbosDelegationTokenSelector() {
		super(ProbosDelegationTokenIdentifier.KIND_NAME);
	}

}
