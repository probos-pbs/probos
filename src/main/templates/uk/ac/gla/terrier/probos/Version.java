package uk.ac.gla.terrier.probos;
/** 
 * Maven automatically populates this with the version number, etc.
 * See http://stackoverflow.com/questions/2469922/generate-a-version-java-file-in-maven 
 */
public class Version {

	public static String VERSION = "${project.version}";

	public static void main(String[] args) {
		System.out.println(VERSION);
	}
}
