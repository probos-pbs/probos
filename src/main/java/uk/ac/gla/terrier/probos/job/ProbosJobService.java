package uk.ac.gla.terrier.probos.job;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.interfaces.RSAPublicKey;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;

import com.google.common.annotations.VisibleForTesting;

import uk.ac.gla.terrier.probos.PConfiguration;
import uk.ac.gla.terrier.probos.api.PBSInteractiveClient;

public abstract class ProbosJobService extends Configured implements Tool {

	protected static final Log LOG = LogFactory.getLog(ProbosJobService.class);
	
	Configuration c = null;
	protected final AtomicBoolean running = new AtomicBoolean();

	public ProbosJobService() {
		super();
		running.set(false);
	}

	public ProbosJobService(Configuration conf) {
		super(conf);
		running.set(false);
	}

	@Override
	public Configuration getConf() {
		if (c != null)
			return c;
		//replace the configuration with a ProBoS configuration
		return c = new PConfiguration(super.getConf());		
	}

	@Override
	public int run(String[] args) throws Exception {
			final String secret = System.getenv("PBS_SECRET");
			
			Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
			Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
	        LOG.info("Executing with tokens:");
	        while (iter.hasNext()) {
	          Token<?> token = iter.next();
	          LOG.info(token);
	          if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
	            iter.remove();
	          }
	        }
			
			SshServer sshd = getSSHServer(secret);
			
			sshd.start();
			running.set(true);
			
			int jobId = Integer.parseInt(System.getenv("PBS_JOBID"));
			String hostPort = System.getenv("PBS_CONTROLLER");
			String[] hostPortSplit = hostPort.split(":");
			final String serverHostname = hostPortSplit[0];
			final int serverPort = Integer.parseInt(hostPortSplit[1]);
			final InetSocketAddress server = new InetSocketAddress(
					serverHostname, 
					serverPort);
			
			Configuration conf = this.getConf();
			PBSInteractiveClient client = RPC.getProxy(PBSInteractiveClient.class,
			        RPC.getProtocolVersion(PBSInteractiveClient.class),
			        server, UserGroupInformation.getCurrentUser(), conf, 
			        NetUtils.getDefaultSocketFactory(conf));
			
			LOG.info("Sister for " + jobId + " started on " + sshd.getPort() + " with secret "+ secret);
			informController(secret, sshd.getPort(), jobId, client);
			while(running.get())
			{
				Thread.sleep(1000);
			}
			LOG.info("Ssh terminated by running variable");
			sshd.stop(true);
			RPC.stopProxy(client);
			return 0;
		}

	protected abstract void informController(final String secret, int port,
			int jobId, PBSInteractiveClient client) throws IOException;

	protected SshServer getSSHServer(final String secret) {
		SshServer sshd = SshServer.setUpDefaultServer();
		sshd.setPort(0);//0 means find a random free port
		sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());		
		sshd.setPasswordAuthenticator(new PasswordAuthenticator()
		{
			@Override
			public boolean authenticate(String username, String password,
					ServerSession session) {
				LOG.debug( "Received password " + password + " verifying with " + secret);
				return secret.equals(password);
			}
		});
		//see https://gist.github.com/jdennaho/5492130
		//	   sshd.setPublickeyAuthenticator(new PublickeyAuthenticator() {
		//	      public boolean authenticate(String username, PublicKey key, ServerSession session) {
		//	         if(key instanceof RSAPublicKey) {
		//	            String s1 = new String(encode((RSAPublicKey) key));
		//	            String s2 = new String(Base64.decodeBase64(secret.getBytes()));					
		//	            return s1.equals(s2); //Returns true if the key matches our known key, this allows auth to proceed.
		//	         }
		//	         return false; //Doesn't handle other key types currently.
		//	      }
		//	   });
		return sshd;
	}

	
	@VisibleForTesting
	public static String[] createShellCommand(String[] command, String shell) {
		return createShellCommand(StringUtils.join(" ", command), shell);
	}
	
	@VisibleForTesting
	public static String[] createShellCommand(String command, String shell) {
		return new String[]{shell, "-c", command};
	}

	public static byte[] encode(RSAPublicKey key) {
		   try {
		      ByteArrayOutputStream buf = new ByteArrayOutputStream();
		      byte[] name = "ssh-rsa".getBytes("US-ASCII");
		      write(name, buf);
		      write(key.getPublicExponent().toByteArray(), buf);
		      write(key.getModulus().toByteArray(), buf);
		      return buf.toByteArray();
		   }
		   catch(Exception e) {
		      e.printStackTrace();
		   }
		   return null;
		}

		private static void write(byte[] str, OutputStream os) throws IOException {
		   for (int shift = 24; shift >= 0; shift -= 8)
		      os.write((str.length >>> shift) & 0xFF);
		   os.write(str);
		}

}