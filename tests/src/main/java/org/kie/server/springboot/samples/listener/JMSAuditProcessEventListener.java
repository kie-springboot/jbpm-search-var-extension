package org.kie.server.springboot.samples.listener;

import org.jbpm.workflow.instance.impl.WorkflowProcessInstanceImpl;
import org.kie.api.event.process.DefaultProcessEventListener;
import org.kie.api.event.process.ProcessNodeLeftEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

@Component
public class JMSAuditProcessEventListener extends DefaultProcessEventListener {

	Logger logger = LoggerFactory.getLogger(JMSAuditProcessEventListener.class);
	@Autowired
	private JmsTemplate jmsTemplate;

	private String varPayload;

	@Override
	public void afterNodeLeft(ProcessNodeLeftEvent event) {
		varPayload = "";

		logger.info("Sending jms message");
		jmsTemplate.setDefaultDestinationName("bpmqueue");

		WorkflowProcessInstanceImpl wpi = (WorkflowProcessInstanceImpl) event.getProcessInstance();
		wpi.getVariables().keySet().forEach(k -> {

			String tmp = k + "=" + wpi.getVariables().get(k) + ",";

			varPayload += tmp;
		});

		jmsTemplate.convertAndSend("id:" + event.getProcessInstance().getProcessId() + " ,instance:"
				+ event.getProcessInstance().getId() + ", variables:\n" + varPayload);
	}

}
