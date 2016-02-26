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
