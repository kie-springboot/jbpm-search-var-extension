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
import java.util.concurrent.atomic.AtomicReference;
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

	private Map<String, Object> searchTaskVars = new HashMap<String, Object>();
	private Map<String, Object> searchProcessVars = new HashMap<String, Object>();
	private Map<Long, List<Variable>> taskVariables = new HashMap<Long, List<Variable>>();
	private Map<Long, List<Variable>> processVariables = new HashMap<Long, List<Variable>>();
	private Map<Attribute, Object> attributes = new HashMap<Attribute, Object>();

	public VariablesQuery(EntityManagerFactory emf) {
		this.emf = emf;
	}

	@POST
	@Path("/")
	@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public Response tasksVariables(@Context HttpHeaders headers, SearchPayload payload) {

		clear();

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

			searchTaskVars = filterByPrefix(payload, SQLConstants.TASK_VAR_PREFIX);

			searchTaskVars.keySet().forEach(k -> {
				logger.info("task var name {}, task var value {}", k, searchTaskVars.get(k));
			});

			searchProcessVars = filterByPrefix(payload, SQLConstants.PROCESS_VAR_PREFIX);

			searchProcessVars.keySet().forEach(k -> {
				logger.info("process var name {}, task var value {}", k, searchProcessVars.get(k));
			});

			attributes = filterByAttribute(payload);
			attributes.keySet().forEach(t -> {
				logger.info("attribute  name {}, attribute value {}", t, attributes.get(t));
			});

			List<IDWrapper> tasksByProcessVars = filterByProcessVars(searchProcessVars);
			tasksByProcessVars.forEach(p -> logger.info("filtered by pVar {} ", p));
			List<IDWrapper> tasksByTasksVar = filterByTasksVars(searchTaskVars);
			tasksByProcessVars.forEach(t -> logger.info("filtered by tVar {} ", t));

			Set<IDWrapper> intersect = intersect(tasksByProcessVars, tasksByTasksVar);

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

	private Set<IDWrapper> intersect(List<IDWrapper> list, List<IDWrapper> otherlist) {
		return list.stream().distinct().filter(otherlist::contains).collect(Collectors.toSet());
	}

	private Map<Attribute, Object> filterByAttribute(SearchPayload payload) {

		return payload.getSearchCriteria().keySet().stream()
				.filter(k -> !k.toLowerCase().startsWith(SQLConstants.TASK_VAR_PREFIX)
						&& !k.toLowerCase().startsWith(SQLConstants.PROCESS_VAR_PREFIX))
				.collect(Collectors.toMap(k -> Attribute.valueOf(k.toUpperCase()),
						k -> payload.getSearchCriteria().get(k)));

	}

	private void clear() {
		searchTaskVars.clear();
		searchProcessVars.clear();
		taskVariables.clear();
		processVariables.clear();
		attributes.clear();
	}

	private Map<Long, List<Variable>> fetchProcessVariables(Set<IDWrapper> intersect) {
		Set<Long> pids = new HashSet<Long>();
		intersect.forEach(id -> pids.add(id.getProcessinstanceid()));

		List<Variable> variables = new ArrayList<Variable>();
		if (!pids.isEmpty()) {
			variables = executeProcessVariablesSQL(pids);
		}
		return variables.stream().collect(Collectors.groupingBy(Variable::getParentId));
	}

	private Map<Long, List<Variable>> fetchTaskVariables(Set<IDWrapper> intersect) {
		Set<Long> tids = new HashSet<Long>();
		intersect.forEach(id -> tids.add(id.getProcessinstanceid()));
		List<Variable> variables = new ArrayList<Variable>();
		if (!tids.isEmpty()) {
			variables = executeTaskVariablesSQL(tids);
		}
		return variables.stream().collect(Collectors.groupingBy(Variable::getParentId));

	}

	@SuppressWarnings("unchecked")
	private List<Variable> executeTaskVariablesSQL(Set<Long> tids) {
		String sql = buildGetTaskVarsQuery(tids);
		EntityManager em = emf.createEntityManager();
		Query query = em.createNativeQuery(sql);
		List<Object[]> sqlResult = query.getResultList();
		List<Variable> pojoResult = transformToVariable(sqlResult, SQLConstants.TASK_TYPE);
		em.close();

		return pojoResult;
	}

	private String buildGetTaskVarsQuery(Set<Long> tids) {
		String sql = "";
		sql += SQLConstants.SELECT_TASK_VARS;

		String idList = "";
		for (Long id : tids) {
			idList += id + " , ";
		}

		idList = removeLastOccurence(idList, SQLConstants.COMMA);
		sql += idList;
		sql += SQLConstants.END_TASK_VARS;

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

	@SuppressWarnings("unchecked")
	private List<Variable> executeProcessVariablesSQL(Set<Long> pids) {
		String sql = buildGetProcessVarsQuery(pids);
		logger.info("final sql \n {}", sql);
		EntityManager em = emf.createEntityManager();
		Query query = em.createNativeQuery(sql);
		List<Object[]> sqlResult = query.getResultList();
		List<Variable> pojoResult = transformToVariable(sqlResult, SQLConstants.PROCESS_TYPE);
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
		sql += SQLConstants.SELECT_PROCESS_VARS;

		String idList = "";
		for (Long id : pids) {
			idList += id + " , ";
		}

		idList = removeLastOccurence(idList, SQLConstants.COMMA);
		sql += idList;
		sql += SQLConstants.END_PROCESS_VARS;

		return sql;
	}

	@SuppressWarnings("unchecked")
	private List<IDWrapper> filterByTasksVars(Map<String, Object> taskVars) {

		String sql = buildSearchByTaskVarQuery(taskVars);
		logger.info("final sql \n {}", sql);

		EntityManager em = emf.createEntityManager();
		Query query = em.createNativeQuery(sql);
		List<Object[]> sqlResult = query.getResultList();
		List<IDWrapper> pojoResult = transform(sqlResult);

		em.close();

		return pojoResult;

	}

	@SuppressWarnings("unchecked")
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
			variableColumns += String.format(SQLConstants.PROCESS_VAR_MAX,
					var.substring(SQLConstants.PROCESS_VAR_PREFIX.length()),
					var.substring(SQLConstants.PROCESS_VAR_PREFIX.length()));
			whereClause += SQLConstants.VAR_PREFIX + var.substring(SQLConstants.PROCESS_VAR_PREFIX.length()) + " = "
					+ "'" + processVars.get(var).toString() + "' " + SQLConstants.AND;
		}
		
		whereClause += applyProcessAttributes(attributes);

		String sql = "";
		sql += SQLConstants.SELECT_PROCESSID_TASKID_CORRELATIONKEY_VAR + variableColumns
				+ SQLConstants.FROM_PROCESSVARLOG;
		whereClause = removeLastOccurence(whereClause, SQLConstants.AND);

		sql += SQLConstants.WHERE + " " + whereClause;

		return sql;
	}

	private String buildSearchByTaskVarQuery(Map<String, Object> taskVars) {
		String variableColumns = "";
		String whereClause = "";
		for (String var : taskVars.keySet()) {
			variableColumns += String.format(SQLConstants.TASK_VAR_MAX,
					var.substring(SQLConstants.TASK_VAR_PREFIX.length()),
					var.substring(SQLConstants.TASK_VAR_PREFIX.length()));
			whereClause += SQLConstants.VAR_PREFIX + var.substring(SQLConstants.TASK_VAR_PREFIX.length()) + " = " + "'"
					+ taskVars.get(var).toString() + "' " + SQLConstants.AND;
		}


		String sql = "";
		sql += SQLConstants.SELECT_PROCESSID_TASKID_VAR + variableColumns + SQLConstants.FROM_TASKVARLOG;
		whereClause = removeLastOccurence(whereClause, SQLConstants.AND);
		sql += SQLConstants.WHERE + " " + whereClause;

		return sql;
	}

	private String applyProcessAttributes(Map<Attribute, Object> attributes) {
		AtomicReference<String> sql = new AtomicReference<String>();
		//@formatter:off

		attributes.keySet().forEach(a -> {

			switch (a) {

			case BUSINESS_KEY: {

				String local = sql.get() != null ? sql.get() : "";
				String tmp = SQLConstants.CORRELATION_KEY_NAME 
						+ SQLConstants.EQUAL_TO
						+ SQLConstants.SINGLE_QUOTE
						+ attributes.get(Attribute.BUSINESS_KEY)
						+ SQLConstants.SINGLE_QUOTE
						+ SQLConstants.AND;
				sql.set(local + "\n" + tmp);
				
				break;
			}
			
			case PROCESS_INSTANCE_ID: {
				String local = sql.get() != null ? sql.get() : "";
				String tmp = SQLConstants.PROCESS_INSTANCE_ID 
						+ SQLConstants.EQUAL_TO
						+ Long.valueOf(attributes.get(Attribute.PROCESS_INSTANCE_ID).toString())
						+ SQLConstants.AND;
				sql.set(local + "\n" + tmp);
				break;
				
			}
			
			default: break;

			}

		});
		
		//@formatter:on

		return sql.get();

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
