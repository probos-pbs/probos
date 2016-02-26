package uk.ac.gla.terrier.probos.controller.webapp;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.cli.pbsnodes;
import uk.ac.gla.terrier.probos.common.BaseServlet;

/** Renders pbsnodes output to HTTP */
public class PbsnodesServlet extends BaseServlet {
	
	private static final long serialVersionUID = 1L;
	
	static final String NAME = "pbsnodes";
	
	public PbsnodesServlet(String uri, List<Entry<String,BaseServlet>> _servletNameSpace, PBSClient _pbsClient) {
		super(NAME, uri, _servletNameSpace, _pbsClient);
	}
	
	@Override
	protected void getPreformattedContent(HttpServletRequest req,
			HttpServletResponse resp, PrintStream ps) throws ServletException, IOException {
		try{
			new pbsnodes(c, ps).run(new String[0]);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	

}
