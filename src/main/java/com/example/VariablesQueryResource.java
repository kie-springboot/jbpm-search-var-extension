package com.example;

import static org.kie.server.remote.rest.common.util.RestUtils.createResponse;
import static org.kie.server.remote.rest.common.util.RestUtils.getContentType;
import static org.kie.server.remote.rest.common.util.RestUtils.getVariant;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManagerFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Variant;

import org.kie.server.api.marshalling.Marshaller;
import org.kie.server.api.marshalling.MarshallerFactory;
import org.kie.server.api.marshalling.MarshallingFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("server/queries/search")
public class VariablesQueryResource {

	private static final Logger logger = LoggerFactory.getLogger(VariablesQueryResource.class);
	private static Boolean PRINT_VERBOSE = true;
	private EntityManagerFactory emf;
	private static final String EMPTY_RESULT = "[ ]";
	private VariablesQueryRequest searchRequest;
	private VariablesQueryService searchService;
	private Marshaller marshaller;

	public VariablesQueryResource(EntityManagerFactory emf) {
		this.emf = emf;
		Set<Class<?>> classes = new HashSet<Class<?>>();
		classes.add(Task.class);
		this.marshaller = MarshallerFactory.getMarshaller(classes, MarshallingFormat.JSON,
				VariablesQueryResource.class.getClassLoader());
	}

	@POST
	@Path("/cases")
	@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public Response queryCases(@Context HttpHeaders headers, SearchPayload payload) {

		throw new UnsupportedOperationException("Not implemented yet!");

	}

	@POST
	@Path("/processes")
	@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public Response queryProcesses(@Context HttpHeaders headers, SearchPayload payload) {
		throw new UnsupportedOperationException("Not implemented yet!");

	}

	@POST
	@Path("/tasks")
	@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public Response queryTasks(@Context HttpHeaders headers, SearchPayload payload) {

		searchRequest = new VariablesQueryRequest(payload, PRINT_VERBOSE);
		searchService = new VariablesQueryService(emf, PRINT_VERBOSE);

		logger.info("VariablesQuery.tasksVariables()");
		Variant v = getVariant(headers);
		String contentType = getContentType(headers);

		MarshallingFormat format = MarshallingFormat.fromType(contentType);
		if (format == null) {
			format = MarshallingFormat.valueOf(contentType);
		}

		try {

			searchRequest.printSearchCriteria();
			searchRequest.filterTaskVariables(SQLConstants.TASK_VAR_PREFIX);
			searchRequest.filterProcessVariables(SQLConstants.PROCESS_VAR_PREFIX);
			searchRequest.filterAttributes();
			searchService.setRequest(searchRequest);

			searchService.filterByPotentialOwner();
			searchService.filterByProcessVars();
			searchService.filterByTasksVars();

			if (searchService.getHaveResults()) {

				Set<IDWrapper> intersect = searchService.intersectResults();
				searchService.fetchTaskVariables(intersect);
				searchService.fetchTaskGroups(intersect);
				searchService.fetchProcessVariables(intersect);

				List<Task> result = searchService.generateResult(intersect);

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

}
