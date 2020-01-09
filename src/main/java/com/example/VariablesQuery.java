package com.example;

import static org.kie.server.remote.rest.common.util.RestUtils.createResponse;
import static org.kie.server.remote.rest.common.util.RestUtils.getContentType;
import static org.kie.server.remote.rest.common.util.RestUtils.getVariant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Variant;

import org.kie.server.api.marshalling.MarshallingFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("server/queries/variables")
public class VariablesQuery {

	private static final Logger logger = LoggerFactory.getLogger(VariablesQuery.class);

	private EntityManagerFactory emf;
	private static final String SELECT_PROCESSID_TASKID_VAR = "SELECT taskid,processinstanceid FROM ( SELECT T.taskId,t.processinstanceid ";

	private static final String FROM_PROCESSVARLOG = " FROM VARIABLEINSTANCELOG V "
			+ "	LEFT JOIN VARIABLEINSTANCELOG  V2 ON ( V.VARIABLEINSTANCEID = V2.VARIABLEINSTANCEID  AND V.PROCESSINSTANCEID=V2.PROCESSINSTANCEID AND V.ID < V2.ID )"
			+ "	INNER JOIN AUDITTASKIMPL  T ON T.PROCESSINSTANCEID = V.PROCESSINSTANCEID"
			+ "	WHERE V2.ID IS NULL GROUP BY T.TASKID,t.processinstanceid ) resultAlias ";

	private static final String FROM_TASKVARLOG = "  FROM TASKVARIABLEIMPL V  "
			+ " LEFT JOIN TASKVARIABLEIMPL  V2 ON ( V.NAME = V2.NAME AND V.TASKID=V2.TASKID AND V.ID < V2.ID )	"
			+ " INNER JOIN AUDITTASKIMPL  T ON T.PROCESSINSTANCEID = V.PROCESSINSTANCEID	 "
			+ " WHERE V2.ID IS NULL GROUP BY T.TASKID,t.processinstanceid ) resultAlias ";

	private static final String PROCESS_VAR_MAX = ", MAX ( CASE V.VARIABLEINSTANCEID WHEN '%s' THEN V.VALUE END )  VAR_%s";
	private static final String TASK_VAR_MAX = ", MAX ( CASE V.name WHEN '%s' THEN V.VALUE END )  VAR_%s";

	private static final String WHERE = "WHERE ";
	private static final String AND = " AND ";
	private static final String VAR_PREFIX = "VAR_";

	private static final String TASK_VAR_PREFIX = "t_";
	private static final String PROCESS_VAR_PREFIX = "p_";
	private static final String COMMA = ",";
	private static final String TASK_TYPE = "task";
	private static final String PROCESS_TYPE = "process";

	private static final String SELECT_PROCESS_VARS = "select " + " v.processinstanceid," + " v.value,"
			+ " v.variableid from variableinstancelog v " + " inner join ( " + " select  max(v.id) myId "
			+ "from variableinstancelog v where v.processinstanceid in ( ";
	private static final String END_PROCESS_VARS = ") group by v.processinstanceid,v.variableid ) resultAlias on v.id = resultAlias.myId";

	private static final String SELECT_TASK_VARS = " select t.taskid,t.value,t.name from taskvariableimpl t "
			+ "	inner join ( " + "			select max(tv.id) myId from taskvariableimpl tv  "
			+ "			where tv.taskid in ( ";
	private static final String END_TASK_VARS = " ) " + "	group by tv.taskid,tv.name "
			+ "	) resultAlias on t.id = resultAlias.myId ";

	private Map<String, Object> searchTaskVars = new HashMap<String, Object>();
	private Map<String, Object> searchProcessVars = new HashMap<String, Object>();
	private Map<Long, List<Variable>> taskVariables = new HashMap<Long, List<Variable>>();
	private Map<Long, List<Variable>> processVariables = new HashMap<Long, List<Variable>>();

	public VariablesQuery(EntityManagerFactory emf) {
		this.emf = emf;
	}

	@POST
	@Path("/")
	@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public Response tasksVariables(@Context HttpHeaders headers, SearchPayload payload) {

		searchTaskVars.clear();
		searchProcessVars.clear();
		taskVariables.clear();
		processVariables.clear();

		logger.info("VariablesQuery.tasksVariables()");
		Variant v = getVariant(headers);
		String contentType = getContentType(headers);

		MarshallingFormat format = MarshallingFormat.fromType(contentType);
		if (format == null) {
			format = MarshallingFormat.valueOf(contentType);
		}
		try {

			logger.info("Received following search criterias");
			payload.getSearchCriteria().keySet().forEach(k -> {

				logger.info("{} = {}", k, payload.getSearchCriteria().get(k));
			});
			logger.info("appendProcessVars {}", payload.getAppendProcessVars());

			searchTaskVars = filterByPrefix(payload, TASK_VAR_PREFIX);

			searchTaskVars.keySet().forEach(k -> {

				logger.info("task var name {}, task var value {}", k, searchTaskVars.get(k));
			});

			searchProcessVars = filterByPrefix(payload, PROCESS_VAR_PREFIX);

			searchProcessVars.keySet().forEach(k -> {

				logger.info("process var name {}, task var value {}", k, searchProcessVars.get(k));
			});

			List<IDWrapper> tasksByProcessVars = filterByProcessVars(searchProcessVars);
			tasksByProcessVars.forEach(p -> logger.info("filtered by pVar {} ", p));
			List<IDWrapper> tasksByTasksVar = filterByTasksVars(searchTaskVars);
			tasksByProcessVars.forEach(t -> logger.info("filtered by tVar {} ", t));

			Set<IDWrapper> intersect = new HashSet<IDWrapper>();
			intersect.addAll(tasksByProcessVars);
			intersect.addAll(tasksByTasksVar);
			intersect.forEach(t -> logger.info("intersect {} ", t));

			taskVariables = fetchTaskVariables(intersect);

			if (payload.getAppendProcessVars() != null && payload.getAppendProcessVars()) {

				processVariables = fetchProcessVariables(intersect);
				processVariables.keySet().forEach(pid -> {

					logger.info("pVars for pid {} : {}", pid, processVariables.get(pid));
				});
			}

			List<BPMTask> result = generateResult(intersect);

			return createResponse(result, v, Response.Status.OK);
		} catch (Exception e) {
			// in case marshalling failed return the call container response to
			// keep backward compatibility
			e.printStackTrace();
			String response = "Execution failed with error : " + e.getMessage();
			logger.error("Returning Failure response with content '{}'", response);
			return createResponse(response, v, Response.Status.INTERNAL_SERVER_ERROR);
		}
	}

	private Map<Long, List<Variable>> fetchProcessVariables(Set<IDWrapper> intersect) {
		Set<Long> pids = new HashSet<Long>();
		intersect.forEach(id -> pids.add(id.getProcessinstanceid()));
		List<Variable> variables = executeProcessVariablesSQL(pids);
		return variables.stream().collect(Collectors.groupingBy(Variable::getParentId));
	}

	private Map<Long, List<Variable>> fetchTaskVariables(Set<IDWrapper> intersect) {
		Set<Long> tids = new HashSet<Long>();
		intersect.forEach(id -> tids.add(id.getProcessinstanceid()));
		List<Variable> variables = executeTaskVariablesSQL(tids);
		return variables.stream().collect(Collectors.groupingBy(Variable::getParentId));

	}

	private List<Variable> executeTaskVariablesSQL(Set<Long> tids) {
		String sql = buildGetTaskVarsQuery(tids);
		logger.info("final sql \n {}", sql);
		EntityManager em = emf.createEntityManager();
		Query query = em.createNativeQuery(sql);
		List<Object[]> sqlResult = query.getResultList();
		List<Variable> pojoResult = transformToVariable(sqlResult, TASK_TYPE);
		em.close();

		return pojoResult;
	}

	private String buildGetTaskVarsQuery(Set<Long> tids) {
		String sql = "";
		sql += SELECT_TASK_VARS;

		String idList = "";
		for (Long id : tids) {
			idList += id + " , ";
		}

		idList = removeLastOccurence(idList, COMMA);
		sql += idList;
		sql += END_TASK_VARS;

		return sql;
	}

	private List<BPMTask> generateResult(Set<IDWrapper> intersect) {

		List<BPMTask> result = new ArrayList<BPMTask>();

		intersect.forEach(id -> {

			result.add(generateBPMTask(id));
		});

		return result;
	}

	private BPMTask generateBPMTask(IDWrapper id) {
		BPMTask task = new BPMTask();
		task.setProcessInstanceId(id.getProcessinstanceid());
		task.setTaskId(id.getTaskid());

		if (taskVariables.containsKey(id.getTaskid())) {
			task.addTaskVariables(taskVariables.get(id.getTaskid()));
		}
		if (processVariables.containsKey(id.getProcessinstanceid())) {
			task.addProcessVariables(processVariables.get(id.getProcessinstanceid()));
		}

		return task;
	}

	private List<Variable> executeProcessVariablesSQL(Set<Long> pids) {
		String sql = buildGetProcessVarsQuery(pids);
		logger.info("final sql \n {}", sql);
		EntityManager em = emf.createEntityManager();
		Query query = em.createNativeQuery(sql);
		List<Object[]> sqlResult = query.getResultList();
		List<Variable> pojoResult = transformToVariable(sqlResult, PROCESS_TYPE);
		em.close();

		return pojoResult;

	}

	private List<Variable> transformToVariable(List<Object[]> sqlResult, String varType) {

		List<Variable> result = new ArrayList<Variable>();

		sqlResult.forEach(s -> {

			result.add(new Variable(s, varType));
		});
		return result;
	}

	private String buildGetProcessVarsQuery(Set<Long> pids) {
		String sql = "";
		sql += SELECT_PROCESS_VARS;

		String idList = "";
		for (Long id : pids) {
			idList += id + " , ";
		}

		idList = removeLastOccurence(idList, COMMA);
		sql += idList;
		sql += END_PROCESS_VARS;

		return sql;
	}

	private List<IDWrapper> filterByTasksVars(Map<String, Object> taskVars) {
		// TODO Auto-generated method stub

		String sql = buildSearchByTaskVarQuery(taskVars);
		logger.info("final sql \n {}", sql);

		EntityManager em = emf.createEntityManager();
		Query query = em.createNativeQuery(sql);
		List<Object[]> sqlResult = query.getResultList();
		List<IDWrapper> pojoResult = transform(sqlResult);

		em.close();

		return pojoResult;

	}

	private List<IDWrapper> filterByProcessVars(Map<String, Object> processVars) {

		String sql = buildSearchByProcessVarQuery(processVars);
		logger.info("final sql \n {}", sql);

		EntityManager em = emf.createEntityManager();
		Query query = em.createNativeQuery(sql);
		List<Object[]> sqlResult = query.getResultList();
		List<IDWrapper> pojoResult = transform(sqlResult);

		em.close();

		return pojoResult;
	}

	private String buildSearchByProcessVarQuery(Map<String, Object> processVars) {
		String variableColumns = "";
		String whereClause = "";
		for (String var : processVars.keySet()) {
			variableColumns += String.format(PROCESS_VAR_MAX, var.substring(PROCESS_VAR_PREFIX.length()),
					var.substring(PROCESS_VAR_PREFIX.length()));
			whereClause += VAR_PREFIX + var.substring(PROCESS_VAR_PREFIX.length()) + " = " + "'"
					+ processVars.get(var).toString() + "' " + AND;
		}

		logger.info("where clause before remove {} ", whereClause);

		String sql = "";
		sql += SELECT_PROCESSID_TASKID_VAR + variableColumns + FROM_PROCESSVARLOG;
		whereClause = removeLastOccurence(whereClause, AND);
		logger.info("where clause after remove {} ", whereClause);

		sql += WHERE + " " + whereClause;

		return sql;
	}

	private String buildSearchByTaskVarQuery(Map<String, Object> taskVars) {
		String variableColumns = "";
		String whereClause = "";
		for (String var : taskVars.keySet()) {
			variableColumns += String.format(TASK_VAR_MAX, var.substring(TASK_VAR_PREFIX.length()),
					var.substring(TASK_VAR_PREFIX.length()));
			whereClause+= VAR_PREFIX + var.substring(TASK_VAR_PREFIX.length()) + " = " + "'"
					+ taskVars.get(var).toString() + "' " + AND;
		}

		String sql = "";
		sql += SELECT_PROCESSID_TASKID_VAR + variableColumns + FROM_TASKVARLOG;
		whereClause = removeLastOccurence(whereClause, AND);
		sql += WHERE + " " + whereClause;

		return sql;
	}

	private String removeLastOccurence(String source, String toRemove) {
		StringBuilder builder = new StringBuilder();
		int start = source.lastIndexOf(toRemove);
		builder.append(source.substring(0, start));
		return builder.toString();

	}

	private Map<String, Object> filterByPrefix(SearchPayload payload, String prefix) {
		return payload.getSearchCriteria().entrySet().stream().filter(k -> k.getKey().toLowerCase().startsWith(prefix))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	private List<IDWrapper> transform(List<Object[]> sqlResult) {
		List<IDWrapper> result = new ArrayList<IDWrapper>();

		sqlResult.forEach(s -> {
			result.add(new IDWrapper(s));
		});

		return result;
	}
}
