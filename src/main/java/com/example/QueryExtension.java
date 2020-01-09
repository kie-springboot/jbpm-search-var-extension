package com.example;

import org.kie.server.services.jbpm.JbpmKieServerExtension;

public class QueryExtension extends JbpmKieServerExtension {

	public String getImplementedCapability() {

		return "query-capability";
	}

	public String getExtensionName() {
		return "query-extension";
	}

}
