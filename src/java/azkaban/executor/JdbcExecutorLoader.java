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

package azkaban.executor;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import azkaban.flow.CommonJobProperties;
import azkaban.utils.*;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import azkaban.database.AbstractJdbcLoader;
import azkaban.utils.FileIOUtils.LogData;

public class JdbcExecutorLoader extends AbstractJdbcLoader 
		implements ExecutorLoader {
	private static final Logger logger = 
			Logger.getLogger(JdbcExecutorLoader.class);

	private EncodingType defaultEncodingType = EncodingType.GZIP;
	
	public JdbcExecutorLoader(Props props) {
		super(props);
	}

	public EncodingType getDefaultEncodingType() {
		return defaultEncodingType;
	}

	public void setDefaultEncodingType(EncodingType defaultEncodingType) {
		this.defaultEncodingType = defaultEncodingType;
	}

	/**
	 * zhongshu-comment 该方法只在一处被调用了，就是在新建一个execution的时候被调用了
	 * @param flow
	 * @throws ExecutorManagerException
	 */
	@Override
	public synchronized void uploadExecutableFlow(ExecutableFlow flow) 
			throws ExecutorManagerException {
		Connection connection = getConnection();
		try {
			uploadExecutableFlow(connection, flow, defaultEncodingType);
		}
		catch (IOException e) {
			throw new ExecutorManagerException("Error uploading flow", e);
		}
		finally {
			DbUtils.closeQuietly(connection);
		}
	}
	
	private synchronized void uploadExecutableFlow(Connection connection, 
			ExecutableFlow flow, EncodingType encType) 
			throws ExecutorManagerException, IOException {
		final String INSERT_EXECUTABLE_FLOW = 
				"INSERT INTO execution_flows " +  //zhongshu-comment 往表中插入一条数据，一条数据对应一次execution
						"(project_id, flow_id, version, status, submit_time, submit_user, update_time, custom_time_flag) " +
						"values (?,?,?,?,?,?,?,?)";
		QueryRunner runner = new QueryRunner();
		long submitTime = System.currentTimeMillis();//zhongshu-comment 这就是submit_time

		System.out.println("===zhongshu=== customTimeFlag=" + flow.getCustomTimeFlag());

		long id;
		try {
			flow.setStatus(Status.PREPARING);
			runner.update(
					connection, 
					INSERT_EXECUTABLE_FLOW, 
					flow.getProjectId(), 
					flow.getFlowId(), 
					flow.getVersion(), 
					Status.PREPARING.getNumVal(), //zhongshu-comment 刚insert到数据库中的记录是preparing状态
					submitTime, 
					flow.getSubmitUser(), 
					submitTime,
					flow.getCustomTimeFlag());

			connection.commit();
			id = runner.query( //zhongshu-comment： execution id是自增的，insert完再去读取一下
					connection, LastInsertID.LAST_INSERT_ID, new LastInsertID());

			if (id == -1l) {
				throw new ExecutorManagerException("Execution id is not properly created.");
			}
			logger.info("Flow given " + flow.getFlowId() + " given id " + id);
			flow.setExecutionId((int)id);//zhongshu-comment 就是在这里生成execId
			
			updateExecutableFlow(connection, flow, encType);
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error creating execution.", e);
		}
	}
	
	@Override
	public void updateExecutableFlow(ExecutableFlow flow) 
			throws ExecutorManagerException {
		Connection connection = this.getConnection();
		
		try {
			updateExecutableFlow(connection, flow, defaultEncodingType);
		}
		finally {
			DbUtils.closeQuietly(connection);
		}
	}

	/**
	 * zhongshu-comment 目测这个方法主要就是为了将ExecutableFlow对象的内容压缩一下，然后存到flow_data字段中
	 * ==question== flow_data的字段都是来自于其他字段的，为什么要冗余存储一下？？
	 *
	 * 用INSERT.*flow_data和UPDATE.*flow_data这两个真个则搜索
	 * 就只有这个位置是修改了flow_data字段的值
	 */
	private void updateExecutableFlow(
			Connection connection, ExecutableFlow flow, EncodingType encType) 
			throws ExecutorManagerException {
		final String UPDATE_EXECUTABLE_FLOW_DATA = 
				"UPDATE execution_flows " + 
						"SET status=?,update_time=?,start_time=?,end_time=?,enc_type=?,flow_data=? " + 
						"WHERE exec_id=?";
		QueryRunner runner = new QueryRunner();
		
		String json = JSONUtils.toJSON(flow.toObject());
		System.out.println("===zhongshu===updateExecutableFlow_gg " + json);

		System.out.println();
		System.out.println("===zhongshu_debug=== " + json);
		System.out.println();

		byte[] data = null;
		try {
			byte[] stringData = json.getBytes("UTF-8");
			data = stringData;
	
			if (encType == EncodingType.GZIP) {
				data = GZIPUtils.gzipBytes(stringData);
			}
		}
		catch (IOException e) {
			throw new ExecutorManagerException("Error encoding the execution flow.");
		}

		try {
			runner.update(
					connection, 
					UPDATE_EXECUTABLE_FLOW_DATA, 
					flow.getStatus().getNumVal(), 
					flow.getUpdateTime(), 
					flow.getStartTime(), 
					flow.getEndTime(), 
					encType.getNumVal(), 
					data, 
					flow.getExecutionId());
			connection.commit();
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error updating flow.", e);
		}
	}
	
	//zhongshu-comment
	@Override
	public ExecutableFlow fetchExecutableFlow(int id) 
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		FetchExecutableFlows flowHandler = new FetchExecutableFlows();

		try {
			List<ExecutableFlow> properties = runner.query(
					FetchExecutableFlows.FETCH_EXECUTABLE_FLOW, flowHandler, id);
			return properties.get(0);
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error fetching flow id " + id, e);
		}
	}
	
	@Override
	public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows() 
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		FetchActiveExecutableFlows flowHandler = new FetchActiveExecutableFlows();

		try {
			Map<Integer, Pair<ExecutionReference, ExecutableFlow>> properties = 
					runner.query(
							FetchActiveExecutableFlows.FETCH_ACTIVE_EXECUTABLE_FLOW, 
							flowHandler);
			return properties;
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error fetching active flows", e);
		}
	}
	
	@Override
	public int fetchNumExecutableFlows() throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		
		IntHandler intHandler = new IntHandler();
		try {
			int count = runner.query(IntHandler.NUM_EXECUTIONS, intHandler);
			return count;
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error fetching num executions", e);
		}
	}
	
	@Override
	public int fetchNumExecutableFlows(int projectId, String flowId) 
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		
		IntHandler intHandler = new IntHandler();
		try {
			int count = runner.query(
					IntHandler.NUM_FLOW_EXECUTIONS, intHandler, projectId, flowId);
			return count;
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error fetching num executions", e);
		}
	}
	
	@Override
	public int fetchNumExecutableNodes(int projectId, String jobId) 
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		
		IntHandler intHandler = new IntHandler();
		try {
			int count = runner.query(
					IntHandler.NUM_JOB_EXECUTIONS, intHandler, projectId, jobId);
			return count;
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error fetching num executions", e);
		}
	}
	//zhongshu-comment
	@Override
	public List<ExecutableFlow> fetchFlowHistory(int projectId, String flowId, 
			int skip, int num) throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		FetchExecutableFlows flowHandler = new FetchExecutableFlows();

		try {
			List<ExecutableFlow> properties = runner.query(
					FetchExecutableFlows.FETCH_EXECUTABLE_FLOW_HISTORY, 
					flowHandler, 
					projectId, 
					flowId, 
					skip, 
					num);
			return properties;
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error fetching active flows", e);
		}
	}
	
	@Override
	public List<ExecutableFlow> fetchFlowHistory(
			int projectId, String flowId, int skip, int num, Status status) 
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		FetchExecutableFlows flowHandler = new FetchExecutableFlows();

		try {
			List<ExecutableFlow> properties = runner.query(
					FetchExecutableFlows.FETCH_EXECUTABLE_FLOW_BY_STATUS, 
					flowHandler, 
					projectId, 
					flowId, 
					status.getNumVal(), 
					skip, 
					num);
			return properties;
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error fetching active flows", e);
		}
	}
	
	@Override
	public List<ExecutableFlow> fetchFlowHistory(int skip, int num) 
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();

		FetchExecutableFlows flowHandler = new FetchExecutableFlows();
		
		try {
			List<ExecutableFlow> properties = runner.query(
					FetchExecutableFlows.FETCH_ALL_EXECUTABLE_FLOW_HISTORY, 
					flowHandler, 
					skip, 
					num);
			return properties;
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error fetching active flows", e);
		}
	}
	

	@Override
	public List<ExecutableFlow> fetchFlowHistory(
			String projContain, 
			String flowContains, 
			String userNameContains, 
			int status, 
			long startTime, 
			long endTime, 
			int skip, 
			int num) throws ExecutorManagerException {
		String query = FetchExecutableFlows.FETCH_BASE_EXECUTABLE_FLOW_QUERY;
		ArrayList<Object> params = new ArrayList<Object>();
		
		boolean first = true;
		if (projContain != null && !projContain.isEmpty()) {
			query += " ef JOIN projects p ON ef.project_id = p.id WHERE name LIKE ?";
			params.add('%'+projContain+'%');
			first = false;
		}
		
		if (flowContains != null && !flowContains.isEmpty()) {
			if (first) {
				query += " WHERE ";
				first = false;
			}
			else {
				query += " AND ";
			}

			query += " flow_id LIKE ?";
			params.add('%'+flowContains+'%');
		}
		
		if (userNameContains != null && !userNameContains.isEmpty()) {
			if (first) {
				query += " WHERE ";
				first = false;
			}
			else {
				query += " AND ";
			}
			query += " submit_user LIKE ?";
			params.add('%'+userNameContains+'%');
		}
		
		if (status != 0) {
			if (first) {
				query += " WHERE ";
				first = false;
			}
			else {
				query += " AND ";
			}
			query += " status = ?";
			params.add(status);
		}
		
		if (startTime > 0) {
			if (first) {
				query += " WHERE ";
				first = false;
			}
			else {
				query += " AND ";
			}
			query += " start_time > ?";
			params.add(startTime);
		}
		
		if (endTime > 0) {
			if (first) {
				query += " WHERE ";
				first = false;
			}
			else {
				query += " AND "; 
			}
			query += " end_time < ?";
			params.add(endTime);
		}
		
		if (skip > -1 && num > 0) {
			query += "  ORDER BY exec_id DESC LIMIT ?, ?";
			params.add(skip);
			params.add(num);
		}
		
		QueryRunner runner = createQueryRunner();
		FetchExecutableFlows flowHandler = new FetchExecutableFlows();

		try {
			List<ExecutableFlow> properties = runner.query(
					query, flowHandler, params.toArray());
			return properties;
		} catch (SQLException e) {
			throw new ExecutorManagerException("Error fetching active flows", e);
		}
	}
	
	@Override
	public void addActiveExecutableReference(ExecutionReference reference)
			throws ExecutorManagerException {
		final String INSERT = 
				"INSERT INTO active_executing_flows " + 
					"(exec_id, host, port, update_time) values (?,?,?,?)";
		QueryRunner runner = createQueryRunner();
		
		try {
			runner.update(
					INSERT, 
					reference.getExecId(), 
					reference.getHost(), 
					reference.getPort(), 
					reference.getUpdateTime());
		}
		catch (SQLException e) {
			throw new ExecutorManagerException(
					"Error updating active flow reference " + reference.getExecId(), e);
		}
	}
	
	@Override
	public void removeActiveExecutableReference(int execid) 
			throws ExecutorManagerException {
		final String DELETE = "DELETE FROM active_executing_flows WHERE exec_id=?";
		
		QueryRunner runner = createQueryRunner();
		try {
			runner.update(DELETE, execid);
		}
		catch (SQLException e) {
			throw new ExecutorManagerException(
					"Error deleting active flow reference " + execid, e);
		}
	}
	
	@Override
	public boolean updateExecutableReference(int execId, long updateTime) 
			throws ExecutorManagerException {
		final String DELETE = 
				"UPDATE active_executing_flows set update_time=? WHERE exec_id=?";
		
		QueryRunner runner = createQueryRunner();
		int updateNum = 0;
		try {
			updateNum = runner.update(DELETE, updateTime, execId);
		}
		catch (SQLException e) {
			throw new ExecutorManagerException(
					"Error deleting active flow reference " + execId, e);
		}
		
		// Should be 1.
		return updateNum > 0;
	}

	@Override
	public void uploadExecutableNode(ExecutableNode node, Props inputProps) 
			throws ExecutorManagerException {
		final String INSERT_EXECUTION_NODE = 
			"INSERT INTO execution_jobs " + 
					"(exec_id, project_id, version, flow_id, job_id, start_time, " + 
					"end_time, status, input_params, attempt) VALUES (?,?,?,?,?,?,?,?,?,?)";
		
		byte[] inputParam = null;
		if (inputProps != null) {
			try {
				String jsonString =
						JSONUtils.toJSON(PropsUtils.toHierarchicalMap(inputProps));
				inputParam = GZIPUtils.gzipString(jsonString, "UTF-8");
			}
			catch (IOException e) {
				throw new ExecutorManagerException("Error encoding input params");
			}
		}
		
		ExecutableFlow flow = node.getExecutableFlow();
		String flowId = node.getParentFlow().getFlowPath();
		System.out.println("Uploading flowId " + flowId);
		QueryRunner runner = createQueryRunner();
		try {
			runner.update(
					INSERT_EXECUTION_NODE, 
					flow.getExecutionId(), 
					flow.getProjectId(), 
					flow.getVersion(), 
					flowId, 
					node.getId(),
					node.getStartTime(),
					node.getEndTime(), 
					node.getStatus().getNumVal(),
					inputParam,
					node.getAttempt());
		} catch (SQLException e) {
			throw new ExecutorManagerException(
					"Error writing job " + node.getId(), e);
		}
	}
	
	@Override
	public void updateExecutableNode(ExecutableNode node)
			throws ExecutorManagerException {
		final String UPSERT_EXECUTION_NODE = 
				"UPDATE execution_jobs " +
						"SET start_time=?, end_time=?, status=?, output_params=? " + 
						"WHERE exec_id=? AND flow_id=? AND job_id=? AND attempt=?";
		
		byte[] outputParam = null;
		Props outputProps = node.getOutputProps();
		if (outputProps != null) {
			try {
				String jsonString =
						JSONUtils.toJSON(PropsUtils.toHierarchicalMap(outputProps));
				outputParam = GZIPUtils.gzipString(jsonString, "UTF-8");
			}
			catch (IOException e) {
				throw new ExecutorManagerException("Error encoding input params");
			}
		}
		
		QueryRunner runner = createQueryRunner();
		try {
			runner.update(
					UPSERT_EXECUTION_NODE, 
					node.getStartTime(), 
					node.getEndTime(), 
					node.getStatus().getNumVal(), 
					outputParam,
					node.getExecutableFlow().getExecutionId(),
					node.getParentFlow().getFlowPath(),
					node.getId(),
					node.getAttempt());
		} catch (SQLException e) {
			throw new ExecutorManagerException(
					"Error updating job " + node.getId(), e);
		}
	}
	
	@Override
	public List<ExecutableJobInfo> fetchJobInfoAttempts(int execId, String jobId)
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		
		try {
			List<ExecutableJobInfo> info = runner.query(
					FetchExecutableJobHandler.FETCH_EXECUTABLE_NODE_ATTEMPTS, 
					new FetchExecutableJobHandler(), 
					execId,
					jobId);
			if (info == null || info.isEmpty()) {
				return null;
			}
			return info;
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error querying job info " + jobId, e);
		}
	}
	
	@Override
	public ExecutableJobInfo fetchJobInfo(int execId, String jobId, int attempts)
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		
		try {
			List<ExecutableJobInfo> info = runner.query(
					FetchExecutableJobHandler.FETCH_EXECUTABLE_NODE, 
					new FetchExecutableJobHandler(), 
					execId, 
					jobId,
					attempts);
			if (info == null || info.isEmpty()) {
				return null;
			}
			return info.get(0);
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error querying job info " + jobId, e);
		}
	}
	
	@Override
	public Props fetchExecutionJobInputProps(int execId, String jobId)
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		try {
			Pair<Props, Props> props = runner.query(
					FetchExecutableJobPropsHandler.FETCH_INPUT_PARAM_EXECUTABLE_NODE, 
					new FetchExecutableJobPropsHandler(), 
					execId, 
					jobId);
			return props.getFirst();
		}
		catch (SQLException e) {
			throw new ExecutorManagerException(
					"Error querying job params " + execId + " " + jobId, e);
		}
	}
	
	@Override
	public Props fetchExecutionJobOutputProps(int execId, String jobId) 
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		try {
			Pair<Props, Props> props = runner.query(
					FetchExecutableJobPropsHandler.FETCH_OUTPUT_PARAM_EXECUTABLE_NODE,
					new FetchExecutableJobPropsHandler(),
					execId,
					jobId);
			return props.getFirst();
		}
		catch (SQLException e) {
			throw new ExecutorManagerException(
					"Error querying job params " + execId + " " + jobId, e);
		}
	}
	
	@Override
	public Pair<Props, Props> fetchExecutionJobProps(int execId, String jobId)
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		try {
			Pair<Props, Props> props = runner.query(
					FetchExecutableJobPropsHandler.FETCH_INPUT_OUTPUT_PARAM_EXECUTABLE_NODE, 
					new FetchExecutableJobPropsHandler(), 
					execId, 
					jobId);
			return props;
		}
		catch (SQLException e) {
			throw new ExecutorManagerException(
					"Error querying job params " + execId + " " + jobId, e);
		}
	}
	
	@Override
	public List<ExecutableJobInfo> fetchJobHistory(
			int projectId, String jobId, int skip, int size) 
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();
		
		try {
			List<ExecutableJobInfo> info = runner.query(
					FetchExecutableJobHandler.FETCH_PROJECT_EXECUTABLE_NODE,
					new FetchExecutableJobHandler(), 
					projectId, 
					jobId, 
					skip, 
					size);
			if (info == null || info.isEmpty()) {
				return null;
			}
			return info;
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error querying job info " + jobId, e);
		}
	}

	//zhongshu-comment
	@Override
	public LogData fetchLogs(
			int execId, String name, int attempt, int startByte, int length)
			throws ExecutorManagerException {

		try {

			QueryRunner runner = createQueryRunner();

			//zhongshu-comment added by zhongshu
			FetchJobMaxRerunTimeHandler fetchJobMaxRerunTimeHandler = new FetchJobMaxRerunTimeHandler();
			int maxRerunTime = runner.query(FetchJobMaxRerunTimeHandler.FETCH_JOB_MAX_RERUN_TIME, fetchJobMaxRerunTimeHandler, execId, name, attempt);

			FetchLogsHandler handler = new FetchLogsHandler(startByte, length + startByte);

			LogData result = runner.query(
					FetchLogsHandler.FETCH_LOGS, 
					handler,
					execId, 
					name, 
					attempt,
					maxRerunTime,
					startByte, 
					startByte + length);
			return result;
		}
		catch (SQLException e) {
			throw new ExecutorManagerException(
					"Error fetching logs " + execId + " : " + name, e);
		}
	}

	@Override
	public List<Object> fetchAttachments(int execId, String jobId, int attempt)
			throws ExecutorManagerException {
		QueryRunner runner = createQueryRunner();

		try {
			String attachments = runner.query(
					FetchExecutableJobAttachmentsHandler.FETCH_ATTACHMENTS_EXECUTABLE_NODE,
					new FetchExecutableJobAttachmentsHandler(),
					execId,
					jobId);
			if (attachments == null) {
				return null;
			}
			
			@SuppressWarnings("unchecked")
      List<Object> attachmentList = (List<Object>) JSONUtils.parseJSONFromString(attachments);
			
			return attachmentList;
		}
		catch (IOException e) {
			throw new ExecutorManagerException(
					"Error converting job attachments to JSON " + jobId, e);
		}
		catch (SQLException e) {
			throw new ExecutorManagerException(
					"Error query job attachments " + jobId, e);
		}
	}

	/**
	 * zhongshu-comment 上传日志
	 * execution和job的日志都上传到execution_logs这张表，应该是 to be confirmed
	 * 该方法在两个地方被调用：
	 * 		1、FlowRunner，应该是上传每次execution的日志
	 * 		2、JobRunner，应该是上传job的日志，每个execution对应多个job
 	 * @param execId
	 * @param name zhongshu-comment 假如是上传execution的日志，那name参数就为空字符串""
	 *             					假如上传job的日志，那name参数就为ExecutableNode.getNestedId()，即job的名字
	 * @param attempt
	 * @param files
	 * @throws ExecutorManagerException
	 */
	@Override
	public void uploadLogFile(
			int execId, String name, int attempt, int currentRerunTime, File ... files)
			throws ExecutorManagerException {
		Connection connection = getConnection();
		try {
			uploadLogFile(
					connection, execId, name, attempt, files, defaultEncodingType, currentRerunTime);
			connection.commit();
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error committing log", e);
		}
		catch (IOException e) {
			throw new ExecutorManagerException("Error committing log", e);
		}
		finally {
			DbUtils.closeQuietly(connection);
		}
	}

	//zhongshu-comment
	private void uploadLogFile(
			Connection connection, 
			int execId, 
			String name, 
			int attempt, 
			File[] files, 
			EncodingType encType, int currentRerunTime) throws ExecutorManagerException, IOException {
		// 50K buffer... if logs are greater than this, we chunk.
		// However, we better prevent large log files from being uploaded somehow
		byte[] buffer = new byte[50*1024];
		int pos = 0;
		int length = buffer.length;
		int startByte = 0;
		BufferedInputStream bufferedStream = null;
		try {
			for (int i = 0; i < files.length; ++i) {
				File file = files[i];
				
				bufferedStream = new BufferedInputStream(new FileInputStream(file));
				int size = bufferedStream.read(buffer, pos, length);
				while (size >= 0) {//zhongshu-comment 循环上传日志，每次上传一部分
					if (pos + size == buffer.length) {
						// Flush here.
						uploadLogPart(
								connection, 
								execId, 
								name, 
								attempt, 
								startByte, 
								startByte + buffer.length, 
								encType, 
								buffer, 
								buffer.length, currentRerunTime);
						
						pos = 0;
						length = buffer.length;
						startByte += buffer.length;
					}
					else {
						// Usually end of file.
						pos += size;
						length = buffer.length - pos;
					}
					size = bufferedStream.read(buffer, pos, length);
				}
			}
			
			// Final commit of buffer. //zhongshu-comment 将最后的末尾部分也上传了
			if (pos > 0) {
				uploadLogPart(
						connection, 
						execId, 
						name, 
						attempt, 
						startByte, 
						startByte + pos, 
						encType, 
						buffer, 
						pos,
						currentRerunTime);
			}
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error writing log part.", e);
		}
		catch (IOException e) {
			throw new ExecutorManagerException("Error chunking", e);
		}
		finally {
			IOUtils.closeQuietly(bufferedStream);
		}
	}
	
	//zhongshu-comment 上传日志到MySQL
	private void uploadLogPart(
			Connection connection, 
			int execId, 
			String name, 
			int attempt, 
			int startByte, 
			int endByte, 
			EncodingType encType, 
			byte[] buffer, 
			int length,
			int currentRerunTime) throws SQLException, IOException {
		final String INSERT_EXECUTION_LOGS = 
				"INSERT INTO execution_logs " + 
						"(exec_id, name, attempt, enc_type, start_byte, end_byte, " + 
						"log, upload_time, rerun_time) VALUES (?,?,?,?,?,?,?,?,?)";
		
		QueryRunner runner = new QueryRunner();
		byte[] buf = buffer;
		if (encType == EncodingType.GZIP) {
			buf = GZIPUtils.gzipBytes(buf, 0, length);
		}
		else if (length < buf.length) {
			buf = Arrays.copyOf(buffer, length);
		}
		
		runner.update(
				connection, 
				INSERT_EXECUTION_LOGS, 
				execId, 
				name, 
				attempt, 
				encType.getNumVal(), 
				startByte, 
				startByte + length, 
				buf, 
				DateTime.now().getMillis(),
				currentRerunTime);
	}
	
	@Override
	public void uploadAttachmentFile(ExecutableNode node, File file)
			throws ExecutorManagerException {
		Connection connection = getConnection();
		try {
			uploadAttachmentFile(connection, node, file, defaultEncodingType);
			connection.commit();
		}
		catch (SQLException e) {
			throw new ExecutorManagerException("Error committing attachments ", e);
		}
		catch (IOException e) {
			throw new ExecutorManagerException("Error uploading attachments ", e);
		}
		finally {
			DbUtils.closeQuietly(connection);
		}
	}

	private void uploadAttachmentFile(
			Connection connection,
			ExecutableNode node,
			File file,
			EncodingType encType) throws SQLException, IOException {

		String jsonString = FileUtils.readFileToString(file);
		byte[] attachments = GZIPUtils.gzipString(jsonString, "UTF-8");

		final String UPDATE_EXECUTION_NODE_ATTACHMENTS = 
				"UPDATE execution_jobs " +
						"SET attachments=? " + 
						"WHERE exec_id=? AND flow_id=? AND job_id=? AND attempt=?";

		QueryRunner runner = new QueryRunner();
		runner.update(
				connection,
				UPDATE_EXECUTION_NODE_ATTACHMENTS,
				attachments,
				node.getExecutableFlow().getExecutionId(),
				node.getParentFlow().getNestedId(),
				node.getId(),
				node.getAttempt());
	}
	
	private Connection getConnection() throws ExecutorManagerException {
		Connection connection = null;
		try {
			connection = super.getDBConnection(false);
		}
		catch (Exception e) {
			DbUtils.closeQuietly(connection);
			throw new ExecutorManagerException("Error getting DB connection.", e);
		}
		return connection;
	}
	
	private static class LastInsertID implements ResultSetHandler<Long> {
		private static String LAST_INSERT_ID = "SELECT LAST_INSERT_ID()";
		@Override
		public Long handle(ResultSet rs) throws SQLException {
			if (!rs.next()) {
				return -1l;
			}
			long id = rs.getLong(1);
			return id;
		}
	}

	private static class FetchExecutionMaxRerunTimeHandler implements ResultSetHandler<Integer> {

		private static String FETCH_EXECUTION_MAX_RERUN_TIME = "SELECT max(rerun_time) FROM execution_logs WHERE exec_id=?";

		@Override
		public Integer handle(ResultSet resultSet) throws SQLException {
			if (resultSet.next()) {
				return resultSet.getInt(1);
			}
			return 0;//zhongshu-comment 第一次跑execution时因为之前没有插入过记录，所以直接返回0即可，即rerun_time=0，第一次跑execution
		}
	}


	//zhongshu-comment added by zhongshu
	@Override
	public int fetchExecutionMaxRerunTime(
			int execId)
			throws ExecutorManagerException {

		try {
			FetchExecutionMaxRerunTimeHandler fetchExecutionMaxRerunTimeHandler = new FetchExecutionMaxRerunTimeHandler();
			QueryRunner runner = createQueryRunner();

			return runner.query(FetchExecutionMaxRerunTimeHandler.FETCH_EXECUTION_MAX_RERUN_TIME, fetchExecutionMaxRerunTimeHandler, execId);
		}
		catch (SQLException e) {
			throw new ExecutorManagerException(
					"Error fetching logs : fetch execution max rerun_time failed " + execId, e);
		}
	}

	/*
	added by zhongshu
	zhongshu-comment 因为可能有些job在第1次执行就成功了，那这个jobA那条记录字段rerun_time=0，
					有些jobB在第2次执行才成功，那这个job那条记录字段rerun_time=1，
					虽然jobB在第1次执行时失败了，但是这次日志还是会上传的，字段rerun_time=0。

					job跑成功的那一次对应那条记录的rerun_time字段的值肯定是最大的，因为跑成功之后就不能再重跑了！！
	 */
	private static class FetchJobMaxRerunTimeHandler implements ResultSetHandler<Integer> {

		private static String FETCH_JOB_MAX_RERUN_TIME = "SELECT max(rerun_time) " +
				"FROM execution_logs " +
				"WHERE exec_id=? AND name=? AND attempt=?";

		@Override
		public Integer handle(ResultSet resultSet) throws SQLException {
			if (resultSet.next()) {
				return resultSet.getInt(1);
			}
			throw new SQLException("cannot query the max rerun_time!");
		}
	}
	
	private static class FetchLogsHandler implements ResultSetHandler<LogData> {
		//zhongshu-comment
		private static String FETCH_LOGS =
				"SELECT exec_id, name, attempt, enc_type, start_byte, end_byte, log " +
				"FROM execution_logs " +
				"WHERE exec_id=? AND name=? AND attempt=? AND rerun_time=? " + //zhongshu-comment   end_byte > startByte
				"AND end_byte > ? AND start_byte <= ? ORDER BY start_byte"; //zhongshu-comment   start_byte <= startByte + length
		/*
		req 300 400
		record  350    500

		500 > 300
		350 < 400
		 */

		private int startByte;
		private int endByte;
		
		public FetchLogsHandler(int startByte, int endByte) {
			this.startByte = startByte;
			this.endByte = endByte;
		}
		
		@Override
		public LogData handle(ResultSet rs) throws SQLException {
			if (!rs.next()) {
				return null;
			}
			
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

			do {
				//int execId = rs.getInt(1);
				//String name = rs.getString(2);
				@SuppressWarnings("unused")
				int attempt = rs.getInt(3);
				EncodingType encType = EncodingType.fromInteger(rs.getInt(4));
				int startByte = rs.getInt(5);
				int endByte = rs.getInt(6);

				byte[] data = rs.getBytes(7);

				int offset = this.startByte > startByte 
						? this.startByte - startByte 
						: 0;
				int length = this.endByte < endByte 
						? this.endByte - startByte - offset
						: endByte - startByte - offset;
				try {
					byte[] buffer = data;
					if (encType == EncodingType.GZIP) {
						buffer = GZIPUtils.unGzipBytes(data);
					}

					byteStream.write(buffer, offset, length);
				}
				catch (IOException e) {
					throw new SQLException(e);
				}
			} while (rs.next());

			byte[] buffer = byteStream.toByteArray();
			Pair<Integer,Integer> result = FileIOUtils.getUtf8Range(
					buffer, 0, buffer.length);
		
			return new LogData(
					startByte + result.getFirst(), 
					result.getSecond(), 
					new String(buffer, result.getFirst(), result.getSecond()));
		}
	}
	
	private static class FetchExecutableJobHandler 
			implements ResultSetHandler<List<ExecutableJobInfo>> {
		private static String FETCH_EXECUTABLE_NODE = 
				"SELECT exec_id, project_id, version, flow_id, job_id, " + 
						"start_time, end_time, status, attempt " + 
						"FROM execution_jobs WHERE exec_id=? " + 
						"AND job_id=? AND attempt_id=?";
		private static String FETCH_EXECUTABLE_NODE_ATTEMPTS = 
				"SELECT exec_id, project_id, version, flow_id, job_id, " + 
						"start_time, end_time, status, attempt FROM execution_jobs " + 
						"WHERE exec_id=? AND job_id=?";
		private static String FETCH_PROJECT_EXECUTABLE_NODE =
				"SELECT exec_id, project_id, version, flow_id, job_id, " + 
						"start_time, end_time, status, attempt FROM execution_jobs " +
						"WHERE project_id=? AND job_id=? " + 
						"ORDER BY exec_id DESC LIMIT ?, ? ";

		@Override
		public List<ExecutableJobInfo> handle(ResultSet rs) throws SQLException {
			if (!rs.next()) {
				return Collections.<ExecutableJobInfo>emptyList();
			}
			
			List<ExecutableJobInfo> execNodes = new ArrayList<ExecutableJobInfo>();
			do {
				int execId = rs.getInt(1);
				int projectId = rs.getInt(2);
				int version = rs.getInt(3);
				String flowId = rs.getString(4);
				String jobId = rs.getString(5);
				long startTime = rs.getLong(6);
				long endTime = rs.getLong(7);
				Status status = Status.fromInteger(rs.getInt(8));
				int attempt = rs.getInt(9);
				
				ExecutableJobInfo info = new ExecutableJobInfo(
						execId, 
						projectId, 
						version, 
						flowId, 
						jobId, 
						startTime, 
						endTime, 
						status, 
						attempt);
				execNodes.add(info);
			} while (rs.next());

			return execNodes;
		}
	}

	private static class FetchExecutableJobAttachmentsHandler
			implements ResultSetHandler<String> {
		private static String FETCH_ATTACHMENTS_EXECUTABLE_NODE = 
				"SELECT attachments FROM execution_jobs WHERE exec_id=? AND job_id=?";

		@Override
		public String handle(ResultSet rs) throws SQLException {
			String attachmentsJson = null;
			if (rs.next()) {
				try {
					byte[] attachments = rs.getBytes(1);
					if (attachments != null) {
						attachmentsJson = GZIPUtils.unGzipString(attachments, "UTF-8");
					}
				}
				catch (IOException e) {
					throw new SQLException("Error decoding job attachments", e);
				}
			}
			return attachmentsJson;
		}
	}

	//zhongshu-comment added by zhongshu
	private static class FetchSubmitTimeHandler
			implements ResultSetHandler<Long> {
		private static String SQL =
				"SELECT submit_time FROM execution_flows WHERE exec_id=?";

		@Override
		public Long handle(ResultSet rs) throws SQLException {
			long submitTime = -12345;
			if (rs.next()) {
				try {
					submitTime = rs.getLong("submit_time");
				}
				catch (Exception e) {
					throw new SQLException("query submit_time fail! ", e);
				}
			}
			return submitTime;
		}
	}

	//zhongshu-comment added by zhongshu
	public void querySubmitTimeByRerunId(String rerunExecid, Props commonFlowProps) throws Exception {
		if (rerunExecid != null && !rerunExecid.trim().equals("")) {
			//查询数据库的execution_flows表的submit_time字段
			QueryRunner runner = createQueryRunner();
			long submitTime = runner.query(FetchSubmitTimeHandler.SQL, new JdbcExecutorLoader.FetchSubmitTimeHandler(), rerunExecid);
			if (submitTime == -12345) {
				throw new Exception("query submit time fail!");
			} else {

				CustomDateUtil.customDate(commonFlowProps, submitTime);
			}
		}
	}

	private static class FetchExecutableJobPropsHandler 
			implements ResultSetHandler<Pair<Props, Props>> {
		private static String FETCH_OUTPUT_PARAM_EXECUTABLE_NODE = 
				"SELECT output_params FROM execution_jobs WHERE exec_id=? AND job_id=?";
		private static String FETCH_INPUT_PARAM_EXECUTABLE_NODE = 
				"SELECT input_params FROM execution_jobs WHERE exec_id=? AND job_id=?";
		private static String FETCH_INPUT_OUTPUT_PARAM_EXECUTABLE_NODE = 
				"SELECT input_params, output_params " +
						"FROM execution_jobs WHERE exec_id=? AND job_id=?";
		
		@SuppressWarnings("unchecked")
		@Override
		public Pair<Props, Props> handle(ResultSet rs) throws SQLException {
			if (!rs.next()) {
				return new Pair<Props, Props>(null, null);
			}
			
			if (rs.getMetaData().getColumnCount() > 1) {
				byte[] input = rs.getBytes(1);
				byte[] output = rs.getBytes(2);
				
				Props inputProps = null;
				Props outputProps = null;
				try {
					if (input != null) {
						String jsonInputString = GZIPUtils.unGzipString(input, "UTF-8");
						inputProps = PropsUtils.fromHierarchicalMap(
								(Map<String, Object>)JSONUtils.parseJSONFromString(jsonInputString));
						
					}
					if (output != null) {
						String jsonOutputString = GZIPUtils.unGzipString(output, "UTF-8");
						outputProps = PropsUtils.fromHierarchicalMap(
								(Map<String, Object>) JSONUtils.parseJSONFromString(jsonOutputString));
					}
				}
				catch (IOException e) {
					throw new SQLException("Error decoding param data", e);
				}
				
				return new Pair<Props, Props>(inputProps, outputProps);
			}
			else {
				byte[] params = rs.getBytes(1);
				Props props = null;
				try {
					if (params != null) {
						String jsonProps = GZIPUtils.unGzipString(params, "UTF-8");

						props = PropsUtils.fromHierarchicalMap(
								(Map<String, Object>)JSONUtils.parseJSONFromString(jsonProps));
					}
				}
				catch (IOException e) {
					throw new SQLException("Error decoding param data", e);
				}
				
				return new Pair<Props,Props>(props, null);
			}
		}
	}

	//zhongshu-comment 查询flow_data
	private static class FetchActiveExecutableFlows 
			implements ResultSetHandler<Map<Integer, Pair<ExecutionReference,ExecutableFlow>>> {
		private static String FETCH_ACTIVE_EXECUTABLE_FLOW = 
				"SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data " + 
						"flow_data, ax.host host, ax.port port, ax.update_time " + 
						"axUpdateTime " + 
						"FROM execution_flows ex " + 
						"INNER JOIN active_executing_flows ax ON ex.exec_id = ax.exec_id";
		
		@Override
		public Map<Integer, Pair<ExecutionReference,ExecutableFlow>> handle(ResultSet rs) 
				throws SQLException {
			if (!rs.next()) {
				return Collections.<Integer, Pair<ExecutionReference,ExecutableFlow>>emptyMap();
			}

			Map<Integer, Pair<ExecutionReference,ExecutableFlow>> execFlows = 
					new HashMap<Integer, Pair<ExecutionReference,ExecutableFlow>>();
			do {
				int id = rs.getInt(1);
				int encodingType = rs.getInt(2);
				byte[] data = rs.getBytes(3);
				String host = rs.getString(4);
				int port = rs.getInt(5);
				long updateTime = rs.getLong(6);
				
				if (data == null) {
					execFlows.put(id, null);
				}
				else {
					EncodingType encType = EncodingType.fromInteger(encodingType);
					Object flowObj;
					try {
						// Convoluted way to inflate strings. Should find common package or
						// helper function.
						if (encType == EncodingType.GZIP) {
							// Decompress the sucker.
							String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
							flowObj = JSONUtils.parseJSONFromString(jsonString);
						}
						else {
							String jsonString = new String(data, "UTF-8");
							flowObj = JSONUtils.parseJSONFromString(jsonString);
						}
						
						ExecutableFlow exFlow = 
								ExecutableFlow.createExecutableFlowFromObject(flowObj);
						ExecutionReference ref = new ExecutionReference(id, host, port);
						ref.setUpdateTime(updateTime);
						
						execFlows.put(
								id, new Pair<ExecutionReference, ExecutableFlow>(ref, exFlow));
					}
					catch (IOException e) {
						throw new SQLException("Error retrieving flow data " + id, e);
					}
				}
			} while (rs.next());

			return execFlows;
		}
	}

	//zhongshu-comment 查询execution_flows表的flow_data字段
	private static class FetchExecutableFlows 
			implements ResultSetHandler<List<ExecutableFlow>> {
		private static String FETCH_BASE_EXECUTABLE_FLOW_QUERY = 
				"SELECT exec_id, enc_type, flow_data FROM execution_flows ";

		//zhongshu-comment
		private static String FETCH_EXECUTABLE_FLOW =
				"SELECT exec_id, enc_type, flow_data FROM execution_flows " +
						"WHERE exec_id=?";

		//private static String FETCH_ACTIVE_EXECUTABLE_FLOW =
		//	"SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data " +
		//			"FROM execution_flows ex " +
		//			"INNER JOIN active_executing_flows ax ON ex.exec_id = ax.exec_id";
		private static String FETCH_ALL_EXECUTABLE_FLOW_HISTORY = 
				"SELECT exec_id, enc_type, flow_data FROM execution_flows " +
						"ORDER BY exec_id DESC LIMIT ?, ?";
		private static String FETCH_EXECUTABLE_FLOW_HISTORY = 
				"SELECT exec_id, enc_type, flow_data FROM execution_flows " +
						"WHERE project_id=? AND flow_id=? " +
						"ORDER BY exec_id DESC LIMIT ?, ?";
		private static String FETCH_EXECUTABLE_FLOW_BY_STATUS = 
				"SELECT exec_id, enc_type, flow_data FROM execution_flows " +
						"WHERE project_id=? AND flow_id=? AND status=? " +
						"ORDER BY exec_id DESC LIMIT ?, ?";
		
		@Override
		public List<ExecutableFlow> handle(ResultSet rs) throws SQLException {
			if (!rs.next()) {
				return Collections.<ExecutableFlow>emptyList();
			}
			
			List<ExecutableFlow> execFlows = new ArrayList<ExecutableFlow>();
			do {
				int id = rs.getInt(1);
				int encodingType = rs.getInt(2);
				byte[] data = rs.getBytes(3);
				
				if (data != null) {
					EncodingType encType = EncodingType.fromInteger(encodingType);
					Object flowObj;
					try {
						// Convoluted way to inflate strings. Should find common package 
						// or helper function.
						if (encType == EncodingType.GZIP) {
							// Decompress the sucker.
							String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
							System.out.println("===zhongshu===_FetchExecutableFlows 1" + jsonString);
							flowObj = JSONUtils.parseJSONFromString(jsonString);
						}
						else {
							String jsonString = new String(data, "UTF-8");
							System.out.println("===zhongshu===_FetchExecutableFlows 2" + jsonString);
							flowObj = JSONUtils.parseJSONFromString(jsonString);
						}

						ExecutableFlow exFlow =
								ExecutableFlow.createExecutableFlowFromObject(flowObj);
						execFlows.add(exFlow);
					}
					catch (IOException e) {
						throw new SQLException("Error retrieving flow data " + id, e);
					}
				}
			} while (rs.next());

			System.out.println("===zhongshu===_execFlows_size = " + execFlows.size());
			return execFlows;
		}
	}
	
	private static class IntHandler implements ResultSetHandler<Integer> {
		private static String NUM_EXECUTIONS = 
				"SELECT COUNT(1) FROM execution_flows";
		private static String NUM_FLOW_EXECUTIONS = 
				"SELECT COUNT(1) FROM execution_flows WHERE project_id=? AND flow_id=?";
		private static String NUM_JOB_EXECUTIONS = 
				"SELECT COUNT(1) FROM execution_jobs WHERE project_id=? AND job_id=?";
		
		@Override
		public Integer handle(ResultSet rs) throws SQLException {
			if (!rs.next()) {
				return 0;
			}
			return rs.getInt(1);
		}
	}

	@Override
	public int removeExecutionLogsByTime(long millis) 
			throws ExecutorManagerException {
		final String DELETE_BY_TIME = 
				"DELETE FROM execution_logs WHERE upload_time < ?";
		
		QueryRunner runner = createQueryRunner();
		int updateNum = 0;
		try {
			updateNum = runner.update(DELETE_BY_TIME, millis);
		}
		catch (SQLException e) {
			e.printStackTrace();
			throw new ExecutorManagerException(
					"Error deleting old execution_logs before " + millis, e);			
		}
		
		return updateNum;
	}

	private static class FetchJobStartEndTimeHandler
			implements ResultSetHandler<Pair<Long, Long>> {

		private static String FETCH_JOB_START_END_TIME = "select start_time,end_time from execution_jobs " +
				"where exec_id=? and job_id=? and attempt=? and flow_id=?";

		@Override
		public Pair<Long, Long> handle(ResultSet resultSet) throws SQLException {

			Pair<Long, Long> pair = null;
			if (resultSet.next()) {
				long startTime = resultSet.getLong("start_time");
				long endTime = resultSet.getLong("end_time");

				pair = new Pair<Long, Long>(startTime, endTime);
			}
			return pair;
		}
	}

	//zhongshu-comment added by zhongshu
	public Pair<Long, Long> fetchJobStartEndTime(ExecutableNode node) throws SQLException {
		try {

			QueryRunner runner = createQueryRunner();
			FetchJobStartEndTimeHandler fetchJobStartEndTimeHandler = new FetchJobStartEndTimeHandler();

			ExecutableFlow flow = node.getExecutableFlow();
			String flowId = node.getParentFlow().getFlowPath();

			System.out.println("===zhongshu===_fetchJobStartEndTime_id " + flow.getExecutionId());//executionID
			System.out.println("===zhongshu===_fetchJobStartEndTime_nestedId " + node.getNestedId());
			System.out.println("===zhongshu===_fetchJobStartEndTime_attempt " + node.getAttempt());
			System.out.println("===zhongshu===_fetchJobStartEndTime_attempt " + flowId);

			return runner.query(FetchJobStartEndTimeHandler.FETCH_JOB_START_END_TIME,
                    fetchJobStartEndTimeHandler,
					flow.getExecutionId(),
                    node.getNestedId(),
                    node.getAttempt(),
					flowId);
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}
}
