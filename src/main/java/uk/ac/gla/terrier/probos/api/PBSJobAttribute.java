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

package uk.ac.gla.terrier.probos.api;

/** Defines attributes that can be used by qselect */
public enum PBSJobAttribute {
	ATTR_a, 
	ATTR_A,
	//ATTR_c "Checkpoint"
	ATTR_e, //"Error_Path"
	ATTR_g,
	ATTR_h,
	ATTR_j,
	ATTR_k,
	ATTR_l,
	ATTR_m,
	ATTR_M,
	ATTR_N, //"Job_Name"
	ATTR_o,
	ATTR_p,
	ATTR_q,
	ATTR_r,
	//ATTR_session,
	ATTR_S,
	ATTR_u,
	ATTR_v,
	//ATTR_ctime
	//ATTR_depend
	//ATTR_mtime
	//ATTR_qtime
	//ATTR_qtype
	//ATTR_stagein
	//ATTR_stageout
	ATTR_state
	;
    
	public String getLabel()
	{
		String rtr = null;
		switch (this) {
		case ATTR_a:
			rtr = "Execution_Time";
			break;
		case ATTR_A:
			rtr = "Account_Name";
			break;
		default:
			break;
		
		}
		return rtr;
	}
	
	public String getDescription()
	{
		String rtr = null;
		switch (this) {
		case ATTR_a:
			rtr = "Select based upon the job's execution time.";
			break;
		default:
			break;
		
		}
		return rtr;
	}
	
}
