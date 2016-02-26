package uk.ac.gla.terrier.probos.common;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import uk.ac.gla.terrier.probos.api.PBSClient;

public abstract class BaseServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected final PBSClient c;
	String name;
	String uri;
	List<Entry<String,BaseServlet>> servletNameSpace;
	
	public BaseServlet(String _name, String _uri, List<Entry<String,BaseServlet>> _servletNameSpace, PBSClient _pbsClient)
	{
		name = _name;
		uri = _uri;
		servletNameSpace = _servletNameSpace;
		c = _pbsClient;
	}
	
	abstract protected void getPreformattedContent(HttpServletRequest req, HttpServletResponse resp, PrintStream ps)
			throws ServletException, IOException;
	
	protected void printNav(PrintStream ps) throws IOException {
		for (Entry<String,BaseServlet> servlet : servletNameSpace)
		{
			if (this.equals(servlet.getValue()))
			{
				ps.println(name);
			}
			else
			{
				ps.println("<a href='"+servlet.getKey()+"'>"+servlet.getValue().name + "</a>");
			}
			ps.println("&nbsp;&nbsp;&nbsp;&nbsp;");
		}
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		
		resp.setContentType("text/html");
		PrintStream ps = new PrintStream(resp.getOutputStream());
		ps.println("<html><head><title>"+name+"</title></head><body>");
		this.printNav(ps);
		ps.println("<pre>");
		getPreformattedContent(req, resp, ps);
		ps.println("</pre></body></html>");
		ps.close();
		resp.flushBuffer();
	}
	
}
