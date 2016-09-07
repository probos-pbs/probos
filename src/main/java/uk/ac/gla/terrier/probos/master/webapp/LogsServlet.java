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

import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.yarn.api.records.ContainerId;

import uk.ac.gla.terrier.probos.api.PBSClient;
import uk.ac.gla.terrier.probos.common.BaseServlet;
import uk.ac.gla.terrier.probos.master.ProbosApplicationMasterServiceImpl;

public class LogsServlet extends BaseServlet {

	private static final long serialVersionUID = 1L;

	static final String NAME = "logs";
	
	ProbosApplicationMasterServiceImpl pams;
	
	public LogsServlet(String _uri, List<Entry<String,BaseServlet>> _servletNameSpace, PBSClient _pbsClient,
			ProbosApplicationMasterServiceImpl _pams)
			throws Exception {
		super(NAME, _uri, _servletNameSpace, _pbsClient);
		pams =_pams;
	}
	
	@Override
	protected void getPreformattedContent(HttpServletRequest req,
			HttpServletResponse resp, PrintStream ps) throws ServletException,
			IOException {}

	@Override
	protected void getContent(HttpServletRequest req, HttpServletResponse resp, PrintStream ps)
			throws ServletException, IOException
	{
		try{
			ps.println("<p>");
			int jobId = pams.getJobId();
			if (pams.isArray())
			{
				ps.println("<ul>");
				TIntObjectHashMap<ContainerId> array2cid = pams.getArrayIds();
				for(int aid : array2cid.keys())
				{
					String stdout = new String( super.c.jobLog(jobId, aid, true, 0, true) );
					String stderr = new String( super.c.jobLog(jobId, aid, false, 0, true) );
					
					ps.println("<li>" + 
						String.valueOf(aid) + ": <a href=\""+stdout+"\">stdout</a>" + ", " + "<a href=\""+stderr+"\">stderr</a>"
						+"</li>");					
				}
				ps.println("</ul>");
			} else {
				String stdout = new String( super.c.jobLog(jobId, -1, true, 0, true) );
				String stderr = new String( super.c.jobLog(jobId, -1, false, 0, true) );				
				ps.println("<a href=\""+stdout+"\">stdout</a>" + ", " + "<a href=\""+stderr+"\">stderr</a>");
			}
			ps.println("</p>");
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
