package uk.ac.gla.terrier.probos;

import java.io.File;

public class Constants {

	public static final String PRODUCT_NAME = "probos";
	public static final String PRODUCT_VERSION = Version.VERSION;
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
