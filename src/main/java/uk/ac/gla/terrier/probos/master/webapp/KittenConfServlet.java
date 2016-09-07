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

package uk.ac.gla.terrier.probos.master.webapp;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;

import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.common.BaseServlet;
import uk.ac.gla.terrier.probos.master.ProbosApplicationMasterServiceImpl;

import com.cloudera.kitten.lua.LuaFields;
import com.cloudera.kitten.util.LocalDataHelper;

public class KittenConfServlet extends BaseServlet {

	private static final long serialVersionUID = 1L;

	static final String NAME = "kittenconf";
	
	ProbosApplicationMasterServiceImpl pams;
	
	public KittenConfServlet(String _uri, List<Entry<String,HttpServlet>> _servletNameSpace, PBSClient _pbsClient,
			ProbosApplicationMasterServiceImpl _pams)
			throws Exception {
		super(NAME, _uri, _servletNameSpace, _pbsClient);
		pams =_pams;
	}

	@Override
	protected void getPreformattedContent(HttpServletRequest req,
			HttpServletResponse resp, PrintStream ps) throws ServletException,
			IOException 
	{	
		final InputStream kittenConfigStream = LocalDataHelper.getFileOrResource( LuaFields.KITTEN_LUA_CONFIG_FILE );
		final StringWriter writer = new StringWriter();
		IOUtils.copy(kittenConfigStream, writer, "UTF-8");
		final String kittenConfig = writer.toString();
		kittenConfigStream.close();
		
		ps.println("<code class=\"lua\">");
		ps.println(kittenConfig);
		ps.println("</code>");	
	}

}
