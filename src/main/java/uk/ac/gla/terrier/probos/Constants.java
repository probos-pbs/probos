package uk.ac.gla.terrier.probos;

import java.io.File;

public class Constants {

//	static {
////		if (System.getenv("JAVA_HOME") == null || System.getenv("JAVA_HOME").length() == 0)
////		{
////			throw new RuntimeException("Env var JAVA_HOME not defined");
////		}
//		if (System.getenv("HADOOP_HOME") == null || System.getenv("HADOOP_HOME").length() == 0)
//		{
//			throw new RuntimeException("Env var HADOOP_HOME not defined");
//		}
//	}
	
	public static final String PRODUCT_NAME = "probos";
	public static final String PRODUCT_VERSION = "0.2.10-SNAPSHOT";
	public static final String DIRECTIVE_PREFIX = "#PBS";
	public static final int DEFAULT_CONTROLLER_PORT = 8027;
		
	
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
