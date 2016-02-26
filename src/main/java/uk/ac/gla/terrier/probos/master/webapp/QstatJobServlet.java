package uk.ac.gla.terrier.probos.master.webapp;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.cli.qstat;
import uk.ac.gla.terrier.probos.common.BaseServlet;
import uk.ac.gla.terrier.probos.master.ProbosApplicationMasterServiceImpl;

public class QstatJobServlet extends BaseServlet {

	private static final long serialVersionUID = 1L;

	static final String NAME = "qstatjob";
	
	ProbosApplicationMasterServiceImpl pams;
	
	public QstatJobServlet(String _uri,List<Entry<String,BaseServlet>> _servletNameSpace, PBSClient _pbsClient,
			ProbosApplicationMasterServiceImpl _pams)
			throws Exception {
		super(NAME, _uri, _servletNameSpace, _pbsClient);
		pams =_pams;
	}

	@Override
	protected void getPreformattedContent(HttpServletRequest req,
			HttpServletResponse resp, PrintStream ps) throws ServletException,
			IOException {
		try{
			new qstat(super.c, ps).run(new String[]{"-f", String.valueOf(pams.getJobId())});
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

}
