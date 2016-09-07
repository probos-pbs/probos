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


/** To use this test suite, you must have the following vars set:
 * HADOOP_HOME, JAVA_HOME
 */
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestSelectors.class,
	TestKittenUtils2.class,
	TestPBSJob.class,
	TestPBSLightJobStatus.class,
	TestPConfiguration.class,
	TestStaticMethods.class,
	TestPBSClientProtocol.class,
	TestQstat.class,	
	TestToken.class,
	TestEndToEnd.class,
	TestQsub.class,
})
public class ProbosSuite {

}
