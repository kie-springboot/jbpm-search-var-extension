package com.example;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BPMTask implements Serializable {

	private static final long serialVersionUID = 2820588057425543709L;
	private Long taskId;
	private Long processInstanceId;
	private String owner;
	private String name;
	private Map<String, Object> taskVariables;
	private Map<String, Object> processVariables;

	public Long getTaskId() {
		return taskId;
	}

	public void setTaskId(Long taskId) {
		this.taskId = taskId;
	}

	public Long getProcessInstanceId() {
		return processInstanceId;
	}

	public void setProcessInstanceId(Long processInstanceId) {
		this.processInstanceId = processInstanceId;
	}

	public Map<String, Object> getTaskVariables() {
		return taskVariables;
	}

	public void setTaskVariables(Map<String, Object> taskVariables) {
		this.taskVariables = taskVariables;
	}

	public Map<String, Object> getProcessVariables() {
		return processVariables;
	}

	public void setProcessVariables(Map<String, Object> processVariables) {
		this.processVariables = processVariables;
	}

	public void addTaskVariables(List<Variable> vars) {

		if (taskVariables == null)
			taskVariables = new HashMap<String, Object>();
		vars.forEach(v -> {

			taskVariables.put(v.getName(), v.getValue());

		});
	}

	public void addProcessVariables(List<Variable> vars) {
		if (processVariables == null)
			processVariables = new HashMap<String, Object>();
		vars.forEach(v -> {

			processVariables.put(v.getName(), v.getValue());

		});
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
