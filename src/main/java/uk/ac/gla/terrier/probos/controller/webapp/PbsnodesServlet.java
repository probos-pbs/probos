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

package uk.ac.gla.terrier.probos.controller.webapp;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.cli.pbsnodes;
import uk.ac.gla.terrier.probos.common.BaseServlet;

/** Renders pbsnodes output to HTTP */
public class PbsnodesServlet extends BaseServlet {
	
	private static final long serialVersionUID = 1L;
	
	static final String NAME = "pbsnodes";
	
	public PbsnodesServlet(String uri, List<Entry<String,HttpServlet>> _servletNameSpace, PBSClient _pbsClient) {
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
