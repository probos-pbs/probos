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

import org.apache.hadoop.conf.Configuration;

public class PConfiguration extends Configuration {

	public static final String KEY_CONTROLLER_HOSTNAME = Constants.PRODUCT_NAME + ".controller.hostname";
	
	public static final String KEY_CONTROLLER_BIND_ADDRESS = Constants.PRODUCT_NAME + ".controller.bind.address";
	
	public static final String KEY_CONTROLLER_PORT = Constants.PRODUCT_NAME + ".controller.port";
	public static final String KEY_CONTROLLER_HTTP_PORT = Constants.PRODUCT_NAME + ".controller.http.port";
	public static final String KEY_CONTROLLER_KILL_TIMEOUT = Constants.PRODUCT_NAME + ".controller.kill.timeout";
	
	
	public static final String KEY_CONTROLLER_JOBDIR = Constants.PRODUCT_NAME + ".controller.jobdir";
	public static final String KEY_CONTROLLER_PRINCIPAL = Constants.PRODUCT_NAME + ".controller.principal";
	public static final String KEY_CONTROLLER_KEYTAB = Constants.PRODUCT_NAME + ".controller.keytab";
	
	public static final String KEY_KITTEN_HADOOP_ON_PATH = Constants.PRODUCT_NAME + ".kitten.hadoop_on_path";
	
	//job defaults
	public static final String KEY_JOB_DEFAULT_CORES = Constants.PRODUCT_NAME + ".job.default.cores";
	public static final String KEY_JOB_DEFAULT_MEM = Constants.PRODUCT_NAME + ".job.default.mem";
	public static final String KEY_JOB_DEFAULT_DURATION = Constants.PRODUCT_NAME + ".job.default.duration";
	
	//public static final String KEY_JOB_DEFAULT_CORES = Constants.PRODUCT_NAME + ".job.master.default.cores";
	public static final String KEY_JOB_MASTER_DEFAULT_MEM = Constants.PRODUCT_NAME + ".job.master.default.mem";
	public static final String KEY_JOB_MASTER_DEFAULT_LABEL = Constants.PRODUCT_NAME + ".job.master.default.label";
	
	//queue constraints
	public static final String KEY_JOB_MAX_QUEUE = Constants.PRODUCT_NAME + ".job.max.queueable";
	public static final String KEY_JOB_MAX_USER_QUEUE = Constants.PRODUCT_NAME + ".job.max.user.queueable";
	
	//copying files around
	public static final String KEY_RCP =  Constants.PRODUCT_NAME + ".rcp";
	public static final String KEY_RCP_USE =  Constants.PRODUCT_NAME + ".rcp.use";
	
	//email
	public static final String KEY_MAIL_CLIENT = Constants.PRODUCT_NAME + ".mail.client";
	public static final String KEY_MAIL_SENDMAIL = Constants.PRODUCT_NAME + ".mail.sendmail";
	public static final String KEY_MAIL_FROM = Constants.PRODUCT_NAME + ".mail.from";
	public static final String KEY_MAIL_HOST = Constants.PRODUCT_NAME + ".mail.host";
	public static final String KEY_MAIL_HOST_PORT = Constants.PRODUCT_NAME + ".mail.host.port";
	public static final String KEY_MAIL_HOST_LOGIN = Constants.PRODUCT_NAME + ".mail.host.login";
	public static final String KEY_MAIL_HOST_PASSWORD = Constants.PRODUCT_NAME + ".mail.host.password";
	public static final String KEY_MAIL_HOST_TLS = Constants.PRODUCT_NAME + ".mail.host.tls";
	
	
	
	public PConfiguration() {
		super();
		super.addResource("probos-default.xml");
		super.addResource("probos-site.xml");
		this.reloadConfiguration();
	}

	public PConfiguration(boolean loadDefaults) {
		super(loadDefaults);
		super.addResource("probos-default.xml");
		super.addResource("probos-site.xml");
		this.reloadConfiguration();
	}

	public PConfiguration(Configuration other) {
		super(other);
		super.addResource("probos-default.xml");
		super.addResource("probos-site.xml");
		this.reloadConfiguration();
	}
	
	

}
