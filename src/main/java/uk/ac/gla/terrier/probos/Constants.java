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

package uk.ac.gla.terrier.probos;

import java.io.File;

public class Constants {

	public static final String PRODUCT_NAME = "probos";
	public static final String PRODUCT_VERSION = Version.VERSION;
	public static final String DIRECTIVE_PREFIX = "#PBS";
	public static final int DEFAULT_CONTROLLER_PORT = 8027;
	public static final int CONTROLLER_MASTER_PORT_OFFSET = 1; //8028
	public static final int CONTROLLER_INTERACTIVE_PORT_OFFSET = -1; //8026
	public static final int CONTROLLER_HTTP_PORT_OFFSET = 2; //8029
		
	
	public static final String HADOOP_HOME = System.getenv("HADOOP_HOME"); 
	public static final String PROBOS_HOME = System.getProperty("probos.home", System.getProperty("user.dir"));
	
	public static final String PACKAGED_KITTEN_JAR_ON_SERVER = 
			System.getProperty("probos.kitten.master.jar", 
			new File(PROBOS_HOME, "lib/kitten-master-0.3.0-jar-with-dependencies.jar").getAbsolutePath());
	public static final String PACKAGED_PROBOS_JAR_ON_SERVER = 
			System.getProperty("probos.probos.master.jar", 
			new File(PROBOS_HOME, "target/"+PRODUCT_NAME+"-"
					+PRODUCT_VERSION+".jar").getAbsolutePath());
					//"-jar-with-dependencies"
	
	public static final String[] ARGS_HELP_OPTIONS = new String[]{"h", "help", "?"};
	public static final String ARGS_HELP_MESSAGE = "Prints this help message";
}
