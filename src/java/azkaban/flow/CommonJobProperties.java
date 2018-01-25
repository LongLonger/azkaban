/*
 * Copyright 2012 LinkedIn Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.flow;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CommonJobProperties {
	/*
	 * The following are Common properties that can be set in a job file
	 */
	
	/**
	 * The type of job that will be executed.
	 * Examples: command, java, etc.
	 */
	public static final String JOB_TYPE = "type";
	
	/**
	 * Force a node to be a root node in a flow, even if there are other jobs dependent on it.
	 */
	public static final String ROOT_NODE = "root.node";
	
	/**
	 * Comma delimited list of job names which are dependencies
	 */
	public static final String DEPENDENCIES = "dependencies";
	
	/**
	 * The number of retries when this job has failed.
	 */
	public static final String RETRIES = "retries";
	
	/**
	 * The time in millisec to back off after every retry
	 */
	public static final String RETRY_BACKOFF = "retry.backoff";
	
	/**
	 * Comma delimited list of email addresses for both failure and success messages
	 */
	public static final String NOTIFY_EMAILS = "notify.emails";
	
	/**
	 * Comma delimited list of email addresses for success messages
	 */
	public static final String SUCCESS_EMAILS = "success.emails";
	
	/**
	 * Comma delimited list of email addresses for failure messages
	 */
	public static final String FAILURE_EMAILS = "failure.emails";

	/*
	 * The following are the common props that will be added to the job by azkaban
	 */
	
	/**
	 * The attempt number of the executing job.
	 */
	public static final String JOB_ATTEMPT = "azkaban.job.attempt";
	
	/**
	 * The attempt number of the executing job.
	 */
	public static final String JOB_METADATA_FILE = "azkaban.job.metadata.file";

	/**
	 * The attempt number of the executing job.
	 */
	public static final String JOB_ATTACHMENT_FILE = "azkaban.job.attachment.file";
	
	/**
	 * The executing flow id
	 */
	public static final String FLOW_ID = "azkaban.flow.flowid";
	
	/**
	 * The nested flow id path
	 */
	public static final String NESTED_FLOW_PATH = "azkaban.flow.nested.path";
	
	/**
	 * The execution id. This should be unique per flow, but may not be due to 
	 * restarts.
	 */
	public static final String EXEC_ID = "azkaban.flow.execid";
	
	/**
	 * The numerical project id identifier.
	 */
	public static final String PROJECT_ID = "azkaban.flow.projectid";
	
	/**
	 * The version of the project the flow is running. This may change if a
	 * forced hotspot occurs.
	 */
	public static final String PROJECT_VERSION = "azkaban.flow.projectversion";
	
	/**
	 * A uuid assigned to every execution
	 */
	public static final String FLOW_UUID = "azkaban.flow.uuid";
	
	/**
	 * Properties for passing the flow start time to the jobs.
	 */
	public static final String FLOW_START_TIMESTAMP = "azkaban.flow.start.timestamp";
	public static final String FLOW_START_YEAR = "azkaban.flow.start.year";
	public static final String FLOW_START_MONTH = "azkaban.flow.start.month";
	public static final String FLOW_START_DAY = "azkaban.flow.start.day";
	public static final String FLOW_START_HOUR = "azkaban.flow.start.hour";
	public static final String FLOW_START_MINUTE = "azkaban.flow.start.minute";
	public static final String FLOW_START_SECOND = "azkaban.flow.start.second";
	public static final String FLOW_START_MILLISSECOND = "azkaban.flow.start.milliseconds";
	public static final String FLOW_START_TIMEZONE = "azkaban.flow.start.timezone";

	/**
	 * zhongshu-comment: added by zhongshu, my email zhongshuhuang215039@sohu-inc.com
	 */
	public static final String CUSTOM_DAY= "custom.day";
	public static final String CUSTOM_LAST_DAY= "custom.last.day";
	public static final String CUSTOM_HOUR = "custom.hour";
	public static final String CUSTOM_LAST_HOUR = "custom.last.hour";
	public static final String CUSTOM_MINUTE = "custom.minute"; //zhongshu-comment: 为了测试重跑flow时使用的时间是否是失败那次flow的时间，所以加上分钟这个属性，不然我就要等下个小时才能测试
	public static final String CUSTOM_TIME = "custom.time"; //zhongshu-comment：对时间做了一层抽象

	public static void main(String[] args) throws IOException {
		Process pcs = Runtime.getRuntime().exec("date +%Y%m%d");
		// 定义shell返回值
		String result = null;

		// 获取shell返回流
		BufferedInputStream in = new BufferedInputStream(pcs.getInputStream());
		// 字符流转换字节流
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		// 这里也可以输出文本日志

		String lineStr;
		while ((lineStr = br.readLine()) != null) {
			result = lineStr;
		}
		// 关闭输入流
		br.close();
		in.close();

		//输出结果示例：20171221
		System.out.println("==============================" + result);
	}
}
