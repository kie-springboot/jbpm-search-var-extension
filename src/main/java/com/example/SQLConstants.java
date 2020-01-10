package com.example;

public class SQLConstants {

	public static final String SELECT_PROCESSID_TASKID_VAR = "SELECT taskid,processinstanceid FROM ( SELECT T.taskId,t.processinstanceid ";

	public static final String SELECT_PROCESSID_TASKID_CORRELATIONKEY_VAR = "SELECT taskid,processinstanceid,correlationKeyName FROM ( SELECT T.taskId,t.processinstanceid,ck.NAME correlationKeyName ";
	public static final String FROM_PROCESSVARLOG = " FROM VARIABLEINSTANCELOG V "
			+ "	LEFT JOIN VARIABLEINSTANCELOG  V2 ON ( V.VARIABLEINSTANCEID = V2.VARIABLEINSTANCEID  AND V.PROCESSINSTANCEID=V2.PROCESSINSTANCEID AND V.ID < V2.ID )"
			+ "	INNER JOIN AUDITTASKIMPL  T ON T.PROCESSINSTANCEID = V.PROCESSINSTANCEID"
			+ " JOIN CORRELATIONKEYINFO ck ON ck.PROCESSINSTANCEID = T.PROCESSINSTANCEID "
			+ "	JOIN PROCESSINSTANCEINFO pi ON pi.instanceid = t.PROCESSINSTANCEID "
			+ "	WHERE V2.ID IS NULL GROUP BY T.TASKID,t.processinstanceid,ck.name ) resultAlias ";

	public static final String FROM_TASKVARLOG = "  FROM TASKVARIABLEIMPL V  "
			+ " LEFT JOIN TASKVARIABLEIMPL  V2 ON ( V.NAME = V2.NAME AND V.TASKID=V2.TASKID AND V.ID < V2.ID )	"
			+ " INNER JOIN AUDITTASKIMPL  T ON T.PROCESSINSTANCEID = V.PROCESSINSTANCEID	 "
			+ " JOIN PROCESSINSTANCEINFO pi ON pi.instanceid = t.PROCESSINSTANCEID "
			+ " WHERE V2.ID IS NULL GROUP BY T.TASKID,t.processinstanceid ) resultAlias ";

	public static final String PROCESS_VAR_MAX = ", MAX ( CASE V.VARIABLEINSTANCEID WHEN '%s' THEN V.VALUE END )  VAR_%s";
	public static final String TASK_VAR_MAX = ", MAX ( CASE V.name WHEN '%s' THEN V.VALUE END )  VAR_%s";

	public static final String WHERE = "WHERE ";
	public static final String AND = " AND ";
	public static final String VAR_PREFIX = "VAR_";
	public static final String CORRELATION_KEY_NAME = "correlationKeyName";
	public static final String PROCESS_INSTANCE_ID = "processinstanceid";
	public static final String EQUAL_TO = " = ";
	public static final String SINGLE_QUOTE = "'";
	


	public static final String TASK_VAR_PREFIX = "t_";
	public static final String PROCESS_VAR_PREFIX = "p_";
	public static final String COMMA = ",";
	public static final String TASK_TYPE = "task";
	public static final String PROCESS_TYPE = "process";

	public static final String SELECT_PROCESS_VARS = "select " + " v.processinstanceid," + " v.value,"
			+ " v.variableid from variableinstancelog v " + " inner join ( " + " select  max(v.id) myId "
			+ "from variableinstancelog v where v.processinstanceid in ( ";
	public static final String END_PROCESS_VARS = ") group by v.processinstanceid,v.variableid ) resultAlias on v.id = resultAlias.myId";

	public static final String SELECT_TASK_VARS = " select t.taskid,t.value,t.name from taskvariableimpl t "
			+ "	inner join ( " + "			select max(tv.id) myId from taskvariableimpl tv  "
			+ "			where tv.taskid in ( ";
	public static final String END_TASK_VARS = " ) " + "	group by tv.taskid,tv.name "
			+ "	) resultAlias on t.id = resultAlias.myId ";

}
