package uk.ac.gla.terrier.probos.controller.webapp;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import uk.ac.gla.terrier.probos.Version;
import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.common.BaseServlet;

public class VersionServlet extends BaseServlet {

	private static final long serialVersionUID = 1L;
	static final String NAME = "version";
	
	public VersionServlet(String uri, List<Entry<String,HttpServlet>> _servletNameSpace, PBSClient _pbsClient) {
		super(NAME, uri, _servletNameSpace, _pbsClient);
	}
	
	@Override
	protected void getPreformattedContent(HttpServletRequest req,
			HttpServletResponse resp, PrintStream ps) throws ServletException,
			IOException {
		ps.println("Version: " + Version.VERSION);
		ps.println("Built: " + Version.BUILD_TIME);
	}

}
