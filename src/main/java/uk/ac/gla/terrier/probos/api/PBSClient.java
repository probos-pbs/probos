package uk.ac.gla.terrier.probos.api;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;

import uk.ac.gla.terrier.probos.PConfiguration;
import uk.ac.gla.terrier.probos.controller.ProbosDelegationTokenSelector;

@ProtocolInfo(protocolName = "uk.ac.gla.terrier.probos.api.PBSClient", protocolVersion = 2)
@KerberosInfo(serverPrincipal = PConfiguration.KEY_CONTROLLER_PRINCIPAL)
@TokenInfo(ProbosDelegationTokenSelector.class)
/** Defines the publicly accessible protocol for {@link ControllerServer}. */
public interface PBSClient extends VersionedProtocol {

	final long PROTOCOL_VERSION = 2;
	
	/** Submit the job to the cluster
	 * @param job specification
	 * @param source of the script to run
	 * @return id of the submitted job, or -1 if could not be submitted.
	 * @throws IOException
	 */
	int submitJob(PBSJob job, byte[] script) throws IOException;
	
	/** Prepare a kitten specification for the the job, 
	 * but do NOT submit to the cluster
	 * @param job specification
	 * @param luaHeader whether the headers to allow a Lua shell to compile the job should be included
	 * @param noProbos whether the lua script should be runnable using Kitten without ProBoS
	 * @return Kitten lua definition of the job
	 * @throws IOException
	 */
	String kittenSpecification(PBSJob job, boolean luaHeaders, boolean noProbos) throws IOException;
	
	/** Kill the job. The caller must be the owner */
	int killJob(int jobId, boolean purge) throws Exception;
	
	/** Get the PBSJob object for the job. */
	PBSJob getJob(int jobId);
	
	/** Get ids of all jobs known to the ProBoS controller */
	int[] getJobs();
	
	/** Get the status of the named job.
	 * @param jobId
	 * @param requestType: 0 for {@link PBSJobStatusLight}, 1 for {@link PBSJobStatusNodes}, 2 for {@link PBSJobStatusFat}, 3 for {@link PBSJobArrayStatusLight}, 4 for {@link PBSJobStatusInteractive}
	 * @return PBSJobStatusLight or child class
	 * @throws Exception
	 */
	PBSJobStatusLight getJobStatus(int jobId, int requestType) throws Exception;
	
	/** get the status of all nodes in the YARN cluster */
	PBSNodeStatus[] getNodesStatus() throws Exception;
	
	/** equivalent of pbs_selectjob */
	int[] selectJobs(PBSJobSelector[] selectors) throws Exception;
		
	/** release a user-hold 
	 * @return 0: ok job now running; -1: unknown job; -2 permission denied (not user hold); -3 submission failed
	 */
	int releaseJob(int jobId) throws Exception;
	
	/** Returns the contents of the stdout/stderr for the task of the given jobId. 
	 * @param jobId specifies the job we want output of
	 * @param arrayId examined if the job is an array
	 * @param stdout true for stdout, false for stderr
	 * @param start offset to start reading the log 
	 * @param URLonly dont return the content, only return the URL
	 * @return bytes of the log.
	 * @throws Exception
	 */
	byte[] jobLog(int jobId, int arrayId, boolean stdout, long start, boolean URLonly) throws Exception;
	
}
