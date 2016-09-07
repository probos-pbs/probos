/**
 * Copyright (c) 2016, University of Glasgow. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

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
