/**
 * Copyright (c) 2016, University of Glasgow. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package uk.ac.gla.terrier.probos.common;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.gla.terrier.probos.Constants;

/** Simple web-based UI, which servlets to an HTTP server.
 * @author craigm
 */
public class WebServer extends AbstractService {
	
	private static final Logger LOG = LoggerFactory.getLogger(WebServer.class);
	
	
	int port;
	Server server;
	List<Entry<String, HttpServlet>> servlets;
	
	public WebServer(String name, List<Entry<String, HttpServlet>> controllerServlets, int _port)
	{
		super(name);
		servlets = controllerServlets;
		port = _port;
	}

	@Override
	protected void serviceInit(Configuration conf) throws Exception {
		super.serviceInit(conf);
		server = new Server(port);
		
		ServletContextHandler context0 = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context0.setContextPath("/");
		for (Entry<String,HttpServlet> s : servlets) {
			context0.addServlet(new ServletHolder(s.getValue()), s.getKey());
		}
		
		ResourceHandler imageHandler = new ResourceHandler();
        imageHandler.setResourceBase(Constants.PROBOS_HOME + "/share/images/");
		LOG.info("Images folder is " + imageHandler.getResourceBase());
        ContextHandler imageContext = new ContextHandler();
        imageContext.setContextPath("/images");
        imageContext.setHandler(imageHandler);
		
		ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[]{context0,imageContext});
        
        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{contexts, new DefaultHandler() });
        
		server.setHandler(handlers);
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
	
	public static void main(String[] args) throws Exception {
		final List<Entry<String,HttpServlet>> controllerServlets = new ArrayList<>();
		controllerServlets.add(new MapEntry<String,HttpServlet>("/", new BaseServlet("name", "/", controllerServlets, null) {
			private static final long serialVersionUID = 1L;

			@Override
			protected void getPreformattedContent(HttpServletRequest req,
					HttpServletResponse resp, PrintStream ps) throws ServletException,
					IOException {
				ps.println("This is a test response");
				ps.println(getServletContext().getServerInfo());
			}
		}));
		WebServer ws = new WebServer("TestWebserver", controllerServlets, 0);
		ws.init(new Configuration());
		ws.start();
		System.out.println(ws.getURI());
		Thread.sleep(5*60*1000);
		ws.stop();
		ws.close();
	}
}
