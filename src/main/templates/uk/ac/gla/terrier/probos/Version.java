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
/** 
 * Maven automatically populates this with the version number, etc.
 * See http://stackoverflow.com/questions/2469922/generate-a-version-java-file-in-maven 
 */
public class Version {

	public static String VERSION = "${project.version}";
	public static String BUILD_TIME = "${timestamp}";
	public static String KITTEN_VERSION = "${kitten.version}";

	public static void main(String[] args) {
		System.out.println(VERSION);
	}
}
