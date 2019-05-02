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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import uk.ac.gla.terrier.probos.JobUtils;
import uk.ac.gla.terrier.probos.Utils;

/** Defines the representation of a job */
public class PBSJob implements Writable {
	static final byte FORMAT_VERSION = 3;


	private String Job_Name;
	
	String Job_Owner;
	String Error_Path;
	String Output_Path;
	
	String join = null;

	//not used by ProBoS, maintained for PBS compatibility
	String account;
	
	String euser;
	String egroup;
	String shell;
	String[] submit_args;
	
	String mailEvent = "a";
	String mailUser = null;
	
	boolean interactive = false;
	boolean rerunnable = true;
	boolean userHold = false;

	int[][] taskArrayIds;
	int taskSlotLimit;

	String queue = JobUtils.DEFAULT_QUEUE;

	private String command;


	Map<String,String> Resource_List = new HashMap<String,String>();
	Map<String,String> Variable_List = new HashMap<String,String>();

	public PBSJob(){}

	public PBSJob(String job_Owner, String error_Path, String output_Path,
			String euser, String egroup, String[] submit_args, String job_Name,
			String command, String account) {
		super();
		Job_Owner = job_Owner;
		Error_Path = error_Path;
		Output_Path = output_Path;
		this.account = account;
		this.euser = euser;
		this.egroup = egroup;
		this.submit_args = submit_args;
		setJob_Name(job_Name);
		this.setCommand(command);
		Variable_List = new HashMap<String,String>();
	}

	public String getCommand() {
		return command;
	}

	public String getShell() {
		return shell;
	}
	
	public void setMailEvent(String eventDescription) {
		this.mailEvent = eventDescription;
	}
	
	public String getMailEvent() {
		return this.mailEvent;
	}
	
	public void setMailUser(String user) {
		this.mailUser = user;
	}
	
	public String getMailUser() {
		return this.mailUser;
	}
	
	public void setShell(String s) {
		shell = s;
	}
	
	public String getAccount() {
		return account;
	}
	
	public void setAccount(String a) {
		account = a;
	}
	
	public boolean getInteractive() {
		return interactive;
	}
	
	public void setInteractive(boolean i) {
		interactive = i;
	}

	public String getJob_Name() {
		return Job_Name;
	}
	public String getJob_Owner() {
		return Job_Owner;
	}
	public String getError_Path() {
		return Error_Path;
	}
	public String getOutput_Path() {
		return Output_Path;
	}
	
	public String getJoin() {
		return join;
	}
	
	public void setJoin(String j) {
		this.join = j;
	}

	public void setError_Path(String error_Path) {
		Error_Path = error_Path;
	}

	public void setOutput_Path(String output_Path) {
		Output_Path = output_Path;
	}
	
	public void setRerunnable(boolean yes) 
	{
		this.rerunnable = yes;
	}

	public void setUserHold(boolean yes) 
	{
		this.userHold = yes;
	}
	
	public boolean getUserHold() 
	{
		return this.userHold;
	}
	
	public Map<String, String> getResource_List() {
		return Resource_List;
	}
	
	public Map<String, String> getVariable_List() {
		return Variable_List;
	}
	
	public String getEuser() {
		return euser;
	}
	
	public String getEgroup() {
		return egroup;
	}

	public void setArrayTaskIds(int[][] arrayIds, int slotLimit)
	{
		taskArrayIds = arrayIds;
		taskSlotLimit = slotLimit;
	}

	public int[][] getArrayTaskIds()
	{
		return taskArrayIds;
	}

	public int getArraySlotLimit()
	{
		return taskSlotLimit;
	}

	public int getTaskCount()
	{
		return JobUtils.countTaskArrayItems(this);
	}

	public String[] getSubmit_args() {
		return submit_args;
	}
	
	public void setSubmit_args(String[] args) {
		submit_args = args;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {

		if (in.readByte() != FORMAT_VERSION)
		{
			throw new IllegalArgumentException("Job format is unsupported");
		}
		setJob_Name(in.readUTF());
		Job_Owner = in.readUTF();
		Error_Path = in.readUTF();
		Output_Path = in.readUTF();
		join = Utils.readStringOrNull(in);
		
		euser = in.readUTF();
		shell = Utils.readStringOrNull(in);
		queue = Utils.readStringOrNull(in);
		account = Utils.readStringOrNull(in);
		mailEvent = Utils.readStringOrNull(in);
		mailUser = Utils.readStringOrNull(in);
		rerunnable = in.readBoolean();
		userHold = in.readBoolean();
		interactive = in.readBoolean();
		
		int n = in.readInt();
		submit_args = new String[n];
		for(int i=0;i<n;i++)
			submit_args[i] = in.readUTF();
		setCommand(in.readUTF());
		ObjectInputStream ois = new ObjectInputStream((InputStream) in);
		try{
			Variable_List = (Map<String,String>) ois.readObject();
			Resource_List = (Map<String,String>) ois.readObject();
			if (ois.readBoolean())
			{
				taskArrayIds = (int[][]) ois.readObject();
				taskSlotLimit = ois.readInt();
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(FORMAT_VERSION);
		out.writeUTF(getJob_Name());
		out.writeUTF(Job_Owner);
		out.writeUTF(Error_Path);
		out.writeUTF(Output_Path);
		Utils.writeStringOrNull(out, join);
		out.writeUTF(euser);
		Utils.writeStringOrNull(out, shell);
		Utils.writeStringOrNull(out, queue);
		Utils.writeStringOrNull(out, account);
		Utils.writeStringOrNull(out, mailEvent);
		Utils.writeStringOrNull(out, mailUser);
		out.writeBoolean(rerunnable);
		out.writeBoolean(userHold);
		out.writeBoolean(interactive);
		
		out.writeInt(submit_args.length);
		for(String sa : submit_args)
			out.writeUTF(sa);
		//out.writeInt(commands.length);
		//for(String c : commands)
		//	out.writeUTF(c);
		out.writeUTF(getCommand());
		ObjectOutputStream oos = new ObjectOutputStream((OutputStream)out);
		oos.writeObject(Variable_List);
		oos.writeObject(Resource_List);
		if (taskArrayIds != null)
		{
			oos.writeBoolean(true);
			oos.writeObject(taskArrayIds);
			oos.writeInt(taskSlotLimit);
		}
		else
		{
			oos.writeBoolean(false);
		}
		oos.flush();

	}
	
	public void setQueue(String q) {
		this.queue = q;
	}

	public String getQueue() {
		return queue;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public void setJob_Name(String job_Name) {
		Job_Name = job_Name;
	}

}
