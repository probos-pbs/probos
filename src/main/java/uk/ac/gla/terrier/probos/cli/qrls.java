package uk.ac.gla.terrier.probos.cli;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.gla.terrier.probos.PBSClientFactory;
import uk.ac.gla.terrier.probos.api.PBSClient;

public class qrls extends Configured implements Tool {

	boolean quiet = false;
	public PBSClient c;
	
	
	public qrls() throws IOException
	{
		c = PBSClientFactory.getPBSClient();
	}
	
	@Override
	public int run(String[] args) throws Exception {

		for(String sId : args)
		{
			int rtr = c.releaseJob(Integer.parseInt(sId));
			if (rtr == -1)
			{
				System.err.println("Could not release job " + sId + " : no such job");
			} 
			else if (rtr == -2)
			{
				System.err.println("Could not release job " + sId + " : permission denied");
			}
			else if (rtr == -3)
			{
				System.err.println("Could not release job " + sId + " : job did not submit");
			}
		}
		return 0;
	}
	public static void main(String[] _args) throws Exception
	{
		int rc = ToolRunner.run(new Configuration(), new qrls(), _args);
		System.exit(rc);
	}




}
