package org.kie.server.springboot.samples.listener;

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

	@Override
	public void afterNodeLeft(ProcessNodeLeftEvent event) {

		logger.info("listener triggered");
		if (jmsTemplate != null) {
			logger.info("Sending jms message");
			jmsTemplate.setDefaultDestinationName("bpmqueue");
		
			jmsTemplate.convertAndSend("id:" + event.getProcessInstance().getProcessId() + " ,instance:"
					+ event.getProcessInstance().getId());
		} else {

			logger.info("Injection failed");
		}

	}

}
