package com.example;

import static org.kie.server.remote.rest.common.util.RestUtils.createResponse;
import static org.kie.server.remote.rest.common.util.RestUtils.getContentType;
import static org.kie.server.remote.rest.common.util.RestUtils.getVariant;

import java.util.ArrayList;
import java.util.Arrays;
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

import org.jbpm.services.api.RuntimeDataService;
import org.kie.api.task.model.TaskSummary;
import org.kie.internal.query.QueryFilter;
import org.kie.server.api.marshalling.Marshaller;
import org.kie.server.api.marshalling.MarshallerFactory;
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
	private Map<Attribute, Object> attributesCriterias = new HashMap<Attribute, Object>();
	private Map<Long, TaskAttributes> taskAttributes = new HashMap<Long, TaskAttributes>();
	private static final String EMPTY_RESULT = "[ ]";
	private Boolean HAVE_RESULTS = true;
	private static Boolean HAVE_TASK_VAR = false;
	private static Boolean HAVE_PROCESS_VAR = false;
	private static Boolean HAVE_POTENTIAL_OWNER = false;

	private Marshaller marshaller;

	public VariablesQuery(EntityManagerFactory emf) {
		this.emf = emf;
		Set<Class<?>> classes = new HashSet<Class<?>>();
		classes.add(BPMTask.class);
		this.marshaller = MarshallerFactory.getMarshaller(classes, MarshallingFormat.JSON,
				VariablesQuery.class.getClassLoader());
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
			if (!searchTaskVars.isEmpty()) {
				HAVE_TASK_VAR = true;
			}

			searchTaskVars.keySet().forEach(k -> {
				logger.info("task var name {}, task var value {}", k, searchTaskVars.get(k));
			});

			searchProcessVars = filterByPrefix(payload, SQLConstants.PROCESS_VAR_PREFIX);
			if (!searchProcessVars.isEmpty()) {
				HAVE_PROCESS_VAR = true;
			}

			searchProcessVars.keySet().forEach(k -> {
				logger.info("process var name {}, task var value {}", k, searchProcessVars.get(k));
			});

			attributesCriterias = filterByAttribute(payload);
			checkAttributes(attributesCriterias);

			attributesCriterias.keySet().forEach(t -> {
				logger.info("attribute  name {}, attribute value {}", t, attributesCriterias.get(t));
			});

			List<IDWrapper> tasksByPotentialOwner = new ArrayList<IDWrapper>();
			List<IDWrapper> tasksByProcessVars = new ArrayList<IDWrapper>();
			List<IDWrapper> tasksByTasksVar = new ArrayList<IDWrapper>();

			tasksByPotentialOwner = filterByPotentialOwner(attributesCriterias);
			tasksByPotentialOwner.forEach(p -> logger.info("filtered by pot owner {} ", p));

			if (attributesCriterias.containsKey(Attribute.POTENTIAL_OWNER) && tasksByPotentialOwner.isEmpty()) {

				HAVE_RESULTS = false;
				logger.info("tasksByPotentialOwner empty");
			}

			if (HAVE_RESULTS && HAVE_PROCESS_VAR) {

				tasksByProcessVars = filterByProcessVars(searchProcessVars);
				if (tasksByProcessVars.isEmpty()) {
					HAVE_RESULTS = false;
					logger.info("tasksByProcessVars empty");
				}
			}
			tasksByProcessVars.forEach(p -> logger.info("filtered by pVar {} ", p));

			if (HAVE_RESULTS && HAVE_TASK_VAR) {
				tasksByTasksVar = filterByTasksVars(searchTaskVars);
				if (tasksByTasksVar.isEmpty()) {
					logger.info("tasksByTasksVar empty");
					HAVE_RESULTS = false;
				}
			}
			tasksByTasksVar.forEach(t -> logger.info("filtered by tVar {} ", t));

			logger.info("HAVE_RESULTS ? {} ", HAVE_RESULTS);

			if (HAVE_RESULTS) {

				List<Set<IDWrapper>> tmpResult = new ArrayList<>();
				if (HAVE_PROCESS_VAR)
					tmpResult.add(new HashSet<>(tasksByProcessVars));
				if (HAVE_TASK_VAR)
					tmpResult.add(new HashSet<>(tasksByTasksVar));
				if (HAVE_POTENTIAL_OWNER)
					tmpResult.add(new HashSet<>(tasksByPotentialOwner));

				Set<IDWrapper> intersect = intersect(tmpResult);

				intersect.forEach(t -> logger.info("intersect {} ", t));

				taskVariables = fetchTaskVariables(intersect);

				if (payload.getAppendProcessVars() != null && payload.getAppendProcessVars()) {

					processVariables = fetchProcessVariables(intersect);
					processVariables.keySet().forEach(pid -> {

						logger.info("pVars for pid {} : {}", pid, processVariables.get(pid));
					});
				}

				List<BPMTask> result = generateResult(intersect);
				String marshall = this.marshaller.marshall(result);

				return createResponse(marshall, v, Response.Status.OK);
			} else {
				return createResponse(EMPTY_RESULT, v, Response.Status.OK);

			}
		} catch (Exception e) {
			// in case marshalling failed return the call container response to
			// keep backward compatibility
			e.printStackTrace();
			String response = "Execution failed with error : " + e.getMessage();
			logger.error("Returning Failure response with content '{}'", response);
			return createResponse(response, v, Response.Status.INTERNAL_SERVER_ERROR);
		}
	}

	private Set<IDWrapper> intersect(List<Set<IDWrapper>> tmpResult) {
		Set<IDWrapper> src = tmpResult.get(0);
		Set<IDWrapper> result = new HashSet<IDWrapper>();
		for (int i = 1; i < tmpResult.size(); i++) {
			src.retainAll(tmpResult.get(i));
		}
		for (IDWrapper address : src) {
			result.add(address);
		}
		return result;
	}

	private void checkAttributes(Map<Attribute, Object> attributesCriterias) {
		// TODO Auto-generated method stub

		attributesCriterias.keySet().forEach(a -> {

			switch (a) {

			case ACTUAL_OWNER:
			case TASK_NAME: {
				HAVE_TASK_VAR = true;

				break;
			}

			case PROCESS_ID:
			case PROCESS_INSTANCE_ID:
			case BUSINESS_KEY:

			{
				HAVE_PROCESS_VAR = true;
				break;
			}

			default:
				break;
			}

		});

	}

	private List<IDWrapper> filterByPotentialOwner(Map<Attribute, Object> attributesCriterias) {

		if (attributesCriterias.containsKey(Attribute.POTENTIAL_OWNER)) {
			HAVE_POTENTIAL_OWNER = true;

			String sql = buildGetTasksByPotentialOwnerSQL(
					attributesCriterias.get(Attribute.POTENTIAL_OWNER).toString());
			logger.info("filterByPotentialOwner sql {}", sql);
			EntityManager em = emf.createEntityManager();
			Query query = em.createNativeQuery(sql);
			List<Object[]> sqlResult = query.getResultList();
			List<IDWrapper> pojoResult = transform(sqlResult);
			em.close();
			return pojoResult;

		}
		return new ArrayList<IDWrapper>();
	}

	private String buildGetTasksByPotentialOwnerSQL(String groups) {
		String sql = "";

		sql += SQLConstants.SELECT_TASKS_BY_POTENTIAL_OWNERS_START;
		List<String> listGroups = new ArrayList<>(Arrays.asList(groups.split(",")));
		String strGroups = "";
		for (String group : listGroups) {

			strGroups += SQLConstants.SINGLE_QUOTE;
			strGroups += group;
			strGroups += SQLConstants.SINGLE_QUOTE;
			strGroups += SQLConstants.COMMA;

		}

		strGroups = removeLastOccurence(strGroups, SQLConstants.COMMA);

		sql += strGroups;
		sql += SQLConstants.SELECT_TASKS_BY_POTENTIAL_OWNERS_END;
		return sql;
	}

//	private Set<IDWrapper> intersect(List<IDWrapper> list, List<IDWrapper> otherlist,
//			List<IDWrapper> tasksByPotentialOwner) {
//		return list.stream().distinct().filter(otherlist::contains).collect(Collectors.toSet());
//	}

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
		attributesCriterias.clear();
		taskAttributes.clear();
		HAVE_RESULTS = true;
		HAVE_TASK_VAR = false;
		HAVE_PROCESS_VAR = false;
		HAVE_POTENTIAL_OWNER = false;
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
		logger.info("executeTaskVariablesSQL sql {}", sql);
		EntityManager em = emf.createEntityManager();
		Query query = em.createNativeQuery(sql);
		List<Object[]> sqlResult = query.getResultList();
		extractTaskAttributes(sqlResult);
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
		task.setActualOwner(taskAttributes.get(id.getTaskid()).getActualOwner());
		task.setName(taskAttributes.get(id.getTaskid()).getName());
		task.setProcessId(taskAttributes.get(id.getTaskid()).getProcessId());
		task.setCorrelationKeyName(taskAttributes.get(id.getTaskid()).getCorrelationKeyName());

		return task;
	}

	@SuppressWarnings("unchecked")
	private List<Variable> executeProcessVariablesSQL(Set<Long> pids) {
		String sql = buildGetProcessVarsQuery(pids);
		logger.info("executeProcessVariablesSQL sql \n {}", sql);
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
		logger.info("filterByTasksVars sql \n {}", sql);

		EntityManager em = emf.createEntityManager();
		Query query = em.createNativeQuery(sql);
		List<Object[]> sqlResult = query.getResultList();
		List<IDWrapper> pojoResult = transform(sqlResult);

		em.close();

		return pojoResult;

	}

	private void extractTaskAttributes(List<Object[]> sqlResult) {

		sqlResult.forEach(sql -> {

			TaskAttributes attribute = new TaskAttributes(sql);
			taskAttributes.put(attribute.getTaskId(), attribute);
		});

	}

	@SuppressWarnings("unchecked")
	private List<IDWrapper> filterByProcessVars(Map<String, Object> processVars) {

		String sql = buildSearchByProcessVarQuery(processVars);
		logger.info("filterByProcessVars sql \n {}", sql);

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

		whereClause += applyProcessAttributes(attributesCriterias);

		String sql = "";
		sql += SQLConstants.SELECT_PROCESS + variableColumns + SQLConstants.FROM_PROCESSVARLOG;

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

		whereClause += applyTaskAttributes(attributesCriterias);

		String sql = "";
		sql += SQLConstants.SELECT_TASK + variableColumns + SQLConstants.FROM_TASKVARLOG;
		whereClause = removeLastOccurence(whereClause, SQLConstants.AND);
		sql += SQLConstants.WHERE + " " + whereClause;

		return sql;
	}

	private String applyTaskAttributes(Map<Attribute, Object> attributes) {
		AtomicReference<String> sql = new AtomicReference<String>();
		//@formatter:off

		attributes.keySet().forEach(a -> {

			switch (a) {

			case ACTUAL_OWNER: {

				String local = sql.get() != null ? sql.get() : "";
				String tmp = SQLConstants.ACTUALOWNER_ID
						+ SQLConstants.EQUAL_TO
						+ SQLConstants.SINGLE_QUOTE
						+ attributes.get(Attribute.ACTUAL_OWNER)
						+ SQLConstants.SINGLE_QUOTE
						+ SQLConstants.AND;
				sql.set(local + "\n" + tmp);

				break;
			}

			default:
				break;
			}
		});
		
		//@formatter:on

		return sql.get();
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
			
			case PROCESS_ID: {
				String local = sql.get() != null ? sql.get() : "";
				String tmp = SQLConstants.PROCESS_ID 
						+ SQLConstants.EQUAL_TO
						+ SQLConstants.SINGLE_QUOTE
						+ attributes.get(Attribute.PROCESS_ID)
						+ SQLConstants.SINGLE_QUOTE
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
		if (!source.isEmpty() && source.contains(toRemove)) {
			StringBuilder builder = new StringBuilder();
			int start = source.lastIndexOf(toRemove);
			builder.append(source.substring(0, start));
			return builder.toString();
		} else
			return source;

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
