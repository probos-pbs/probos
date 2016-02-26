package uk.ac.gla.terrier.probos.master.webapp;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;

import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.common.BaseServlet;
import uk.ac.gla.terrier.probos.master.ProbosApplicationMasterServiceImpl;

public class ConfServlet extends BaseServlet {

	private static final long serialVersionUID = 1L;

	static final String NAME = "conf";
	
	ProbosApplicationMasterServiceImpl pams;
	
	public ConfServlet(String _uri, List<Entry<String,BaseServlet>> _servletNameSpace, PBSClient _pbsClient,
			ProbosApplicationMasterServiceImpl _pams)
			throws Exception {
		super(NAME, _uri, _servletNameSpace, _pbsClient);
		pams =_pams;
	}

	@Override
	protected void getPreformattedContent(HttpServletRequest req,
			HttpServletResponse resp, PrintStream ps) throws ServletException,
			IOException {
		
		Configuration.dumpConfiguration(pams.getConf(), new OutputStreamWriter(ps));		
	}

}
