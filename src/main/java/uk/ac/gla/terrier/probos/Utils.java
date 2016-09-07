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

package uk.ac.gla.terrier.probos;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;

public class Utils {

	public static String padRight(String s, int n) {
		if (s.length() > n)
			return s.substring(0, n-1);
		return String.format("%1$-" + n + "s", s);
	}
	
	public static String padLeft(String s, int n) {
		if (s.length() > n)
			return s.substring(0, n-1);
		return String.format("%1$" + n + "s", s);
	}
	
	public static String getHostname() throws IOException {
        String OS = System.getProperty("os.name").toLowerCase();
        //System.err.println(OS);
        String hostname = null;
        if (OS.indexOf("win") >= 0) {
        	hostname = System.getenv("COMPUTERNAME");
        	if (hostname == null)
        		hostname = execReadToString("hostname");
        } else {
            if (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.equals("mac os x")) {
            	hostname = System.getenv("HOSTNAME");
            	if (hostname == null)
            		hostname = execReadToString("hostname");
            	if (hostname == null)
            		hostname = execReadToString("cat /etc/hostname");
            }
        }
        
        assert hostname != null;
        
        return hostname;
    }

    public static String execReadToString(String execCommand) throws IOException {
        Process proc = Runtime.getRuntime().exec(execCommand);
        try (InputStream stream = proc.getInputStream()) {
            try (Scanner s = new Scanner(stream).useDelimiter("\\A")) {
                return s.hasNext() ? s.next().trim() : "";
            }
        }
    }
    
    public static byte[] slurpBytes (final File file) throws IOException {
    	BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
    	byte[] rtr = IOUtils.toByteArray(bis);
    	bis.close();
    	return rtr;
	}

	public static String[] slurpString (final File file) throws IOException {
	    List<String> l = new ArrayList<String>();
	    BufferedReader br = null;
	    try {
	        br = new BufferedReader(new FileReader(file));
	        String line = null;
	        while((line = br.readLine()) != null)
	        {
	        	l.add(line);
	        }
	    }
	    finally {
	    	if (br != null)
	    		br.close();
	    }

	    return l.toArray(new String[l.size()]);
	}
	
	/* http://stackoverflow.com/questions/6118922/convert-seconds-value-to-hours-minutes-seconds-android-java */
	public static String makeTime(long seconds) {

	    long hours = seconds / 3600;
	    long minutes = (seconds % 3600) / 60;
	    seconds = seconds % 60;

	    return twoDigitString(hours) + ":" + twoDigitString(minutes) + ":" + twoDigitString(seconds);
	}

	public static String twoDigitString(long number) {
	    if (number == 0) {
	        return "00";
	    }
	    if (number / 10 == 0) {
	        return "0" + number;
	    }
	    return String.valueOf(number);
	}
	
	public static long parseTime(String timeExpression)
	{
		/** format: [[hours:]minutes:]seconds[.milliseconds] */
		String[] parts = timeExpression.split(":");
		long duration = 0;
		if (parts.length == 1)
		{
			duration = Integer.parseInt(parts[0]);
		}
		else if (parts.length == 2)
		{
			duration = Integer.parseInt(parts[1])
				+ 60l * Integer.parseInt(parts[0]);
		}
		else if (parts.length == 3)
		{
			duration = Integer.parseInt(parts[2])
				+ 60l * Integer.parseInt(parts[1])
				+ 3600l * Integer.parseInt(parts[0]);			
		}
		else
		{
			throw new IllegalArgumentException("Invalid time format:" + timeExpression);
		}
		return duration;
	}

	public static String readStringOrNull(DataInput in) throws IOException {
		if (! in.readBoolean())
			return null;
		return in.readUTF();
	}

	public static void writeStringOrNull(DataOutput out, String s) throws IOException
	{
		if (s == null)
		{
			out.writeBoolean(false);
		}
		else
		{
			out.writeBoolean(true);
			out.writeUTF(s);
		}
	}
}
