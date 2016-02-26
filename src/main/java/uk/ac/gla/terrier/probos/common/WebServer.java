package uk.ac.gla.terrier.probos.common;

import java.net.URI;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/** Simple web-based UI, which servlets to an HTTP server.
 * @author craigm
 */
public class WebServer extends AbstractService {
	
	int port;
	Server server;
	List<Entry<String, BaseServlet>> servlets;
	
	public WebServer(String name, List<Entry<String, BaseServlet>> controllerServlets, int _port)
	{
		super(name);
		servlets = controllerServlets;
		port = _port;
	}

	@Override
	protected void serviceInit(Configuration conf) throws Exception {
		super.serviceInit(conf);
		server = new Server(port);
		ServletHandler handler = new ServletHandler();
		for (Entry<String,BaseServlet> s : servlets) {
			handler.addServletWithMapping(new ServletHolder(s.getValue()), s.getKey());
		}
		server.setHandler(handler);
	}

	public URI getURI()
	{
		return server.getURI();
	}

	@Override
	protected void serviceStart() throws Exception {
		server.start();
		super.serviceStart();
	}



	@Override
	protected void serviceStop() throws Exception {
		server.stop();
		super.serviceStop();
	}
}
