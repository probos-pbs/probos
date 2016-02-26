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
