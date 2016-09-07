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
