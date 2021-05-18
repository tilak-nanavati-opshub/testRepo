/**
 * Copyright C 2012 OpsHub, Inc. All rights reserved
 */
package com.opshub.eai.fogbugz.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.opshub.eai.CommentSettings;
import com.opshub.eai.EAIAttachment;
import com.opshub.eai.EAIEntityRefrences;
import com.opshub.eai.EAIKeyValue;
import com.opshub.eai.EAILinkEntityItem;
import com.opshub.eai.EaiUtility;
import com.opshub.eai.OIMEntityObject;
import com.opshub.eai.core.adapters.OIMConnector;
import com.opshub.eai.core.carriers.EntityPrimaryDetailsCarrier;
import com.opshub.eai.core.carriers.MaxEntityCarrier;
import com.opshub.eai.core.exceptions.OIMAdapterException;
import com.opshub.eai.core.interfaces.HasReverseLinkSupport;
import com.opshub.eai.core.interfaces.HasStringDate;
import com.opshub.eai.core.interfaces.HasUserLookup;
import com.opshub.eai.core.interfaces.IAttachmentProcessor;
import com.opshub.eai.core.interfaces.ILinkSupport;
import com.opshub.eai.core.interfaces.RulesVerifyInterface;
import com.opshub.eai.fogbugz.common.HTTPAttachementPost;
import com.opshub.eai.fogbugz.common.FogBugzConnector;
import com.opshub.eai.fogbugz.common.FogBugzConstants;
import com.opshub.eai.fogbugz.common.FogBugzHTTP;
import com.opshub.eai.fogbugz.common.FogBugzHTTPGet;
import com.opshub.eai.fogbugz.common.FogBugzUtility;
import com.opshub.eai.fogbugz.exceptions.FogBugzAdapterException;
import com.opshub.eai.metadata.DataType;
import com.opshub.eai.metadata.FieldsMeta;
import com.opshub.eai.metadata.LinkTypeMeta;
import com.opshub.eai.metadata.UserMeta;
import com.opshub.exceptions.RequestorException;
import com.opshub.logging.OpsHubLoggingUtil;

public class FogBugzAdapter extends OIMConnector implements HasUserLookup, RulesVerifyInterface, IAttachmentProcessor, ILinkSupport, HasReverseLinkSupport,HasStringDate{
	
	FogBugzConnector fogBugzConnector;
	private static final Logger LOGGER = Logger.getLogger(FogBugzAdapter.class);
	private String userDisplayName;
	private String entityType;
	private String xmlRpcURL;
	private String password;
	private String integrationUser;
	private String projectName;
	private List<HashMap> userList;
	private String token;
	private List<LinkTypeMeta> fogbugzLinkTypeMeta=null;
	private List<String> customProperties;
	public static final HashMap<String, String> systemProperties = new HashMap<String, String>();
	
	static{
		systemProperties.put("Title", "sTitle");
		systemProperties.put("Project", "sProject");
		systemProperties.put("Area", "sArea");
		systemProperties.put("Milestone", "sFixFor");
		systemProperties.put("Status", "sStatus");
		systemProperties.put("Priority", "sPriority");
		systemProperties.put("Category", "sCategory");
		systemProperties.put("Promoted in backlog", FogBugzConstants.BACKLOG);
		systemProperties.put("Demoted in backlog", FogBugzConstants.BACKLOG);
		systemProperties.put("Non-timesheet elapsed time", "hrsElapsedExtra"); 
		systemProperties.put("Estimate", "hrsCurrEst");
		systemProperties.put("Version", "sVersion");
		systemProperties.put("Computer", "sComputer");
		systemProperties.put("Date due", "dtDue");
	}

	/**
	 * @param integrationId
	 * @param integrationUser
	 * @param password
	 * @param displayName
	 * @param entityType
	 * @param xmlRpcURL
	 * @param systemId
	 * @param projectName
	 * @throws OIMAdapterException
	 */
	public FogBugzAdapter(Integer integrationId, String integrationUser, String password, String displayName,
			String entityType, String xmlRpcURL, Integer systemId, String projectName)
			throws OIMAdapterException {
		super(integrationId, displayName, entityType, false, systemId);
		this.xmlRpcURL = xmlRpcURL;
		this.userDisplayName = displayName;
		this.entityType = entityType;
		this.password=password;
		this.integrationUser=integrationUser;
		this.projectName = projectName;
		fogBugzConnector = new FogBugzConnector(xmlRpcURL, integrationUser, password);
		this.token = fogBugzConnector.login(integrationUser,password);
		OpsHubLoggingUtil.debug(LOGGER, "FogBugz Adapter successfully initialized", null);
	}
	
	public FogBugzConnector getFogBugzConnector() {
		return fogBugzConnector;
	}
	
	public String getToken() {
		return token;
	}

	@Override
	public OIMEntityObject addComment(String internalId, String commentTitle,
 String commentBody, boolean isretry, boolean isRecovery,
			final CommentSettings commentSettings)
			throws OIMAdapterException {
		Map<String, Object> systemProperties=new HashMap<String, Object>();
		systemProperties.put("sEvent", commentBody);
		return updateEntity(internalId, null, systemProperties, null, null, isretry, null, null);
	}

	@Override
	public Object getPropertyValue(String internalId, String propertyName,
			boolean isSystemProperty) throws OIMAdapterException {
		OpsHubLoggingUtil.debug(LOGGER, "In getPropertyValue", null);
		if(internalId==null||internalId.isEmpty() || propertyName==null || propertyName.isEmpty())
			return new FogBugzAdapterException("0005", null, null);
		
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(FogBugzConstants.QUERY, internalId);
		properties.put(FogBugzConstants.COLUMNS, propertyName);
		properties.put(FogBugzConstants.CMD, FogBugzConstants.SEARCH);
		properties.put(FogBugzConstants.TOKEN, token);
		String xml = fogBugzConnector.callFogBugzPostAPI(properties);
		List<HashMap> xmlListMap;
		
		xmlListMap = callParseXmlForList(xml, FogBugzUtility.FogbugParserCommands.GET_CASES);
		
		if(xmlListMap==null){
			OpsHubLoggingUtil.error(LOGGER, "unable to find property in fogbug : " + propertyName, null);
			throw new FogBugzAdapterException("0007", new String[]{propertyName}, null);
		}
		
		OpsHubLoggingUtil.debug(LOGGER, propertyName + " : " + xmlListMap.get(FogBugzConstants.ZERO).get(propertyName), null);
		return xmlListMap.get(FogBugzConstants.ZERO).get(propertyName);
	}

	@Override
	public String getPropertyValueAsString(String internalId,
			String propertyName, boolean isSystemProperty)
			throws OIMAdapterException {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(FogBugzConstants.QUERY, internalId);
		properties.put(FogBugzConstants.COLUMNS, propertyName);
		properties.put(FogBugzConstants.CMD, FogBugzConstants.SEARCH);
		properties.put(FogBugzConstants.TOKEN, token);
		String xml = fogBugzConnector.callFogBugzPostAPI(properties);
		
		List<HashMap> xmlListMap;
		xmlListMap = callParseXmlForList(xml, FogBugzUtility.FogbugParserCommands.GET_CASES);
		
		if(xmlListMap==null){
			OpsHubLoggingUtil.error(LOGGER, "unable to find property in fogbug : " + propertyName, null);
			throw new FogBugzAdapterException("0007", new String[]{propertyName}, null);
		}
		
		return xmlListMap.get(FogBugzConstants.ZERO).get(propertyName).toString();
	}

	@Override
	public Object getEntityObject(String internalId) throws OIMAdapterException {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(FogBugzConstants.QUERY , internalId);
		properties.put(FogBugzConstants.COLUMNS , EaiUtility.getCommaSeperatedListValues(FogBugzConstants.FIELDS_LIST, false));
		properties.put(FogBugzConstants.CMD, FogBugzConstants.SEARCH);
		properties.put(FogBugzConstants.TOKEN, token);
		
		String xml = fogBugzConnector.callFogBugzPostAPI(properties);
		
		List<HashMap> xmlListMap;
		
		xmlListMap = callParseXmlForList(xml, FogBugzUtility.FogbugParserCommands.GET_CASES);
		
		if(xmlListMap==null){
			OpsHubLoggingUtil.info(LOGGER, "unable to find case in fogbug : " + internalId, null);
			return null;
		}
		
		return xmlListMap.get(FogBugzConstants.ZERO);
	}

	@Override
	public List getHistorySince(Calendar afterTime, Calendar maxTime,
			String lastProcessedId, String lastProcessedRevisionId,
			String entityName, String notUpdatedByUser, Integer pageSize,
			List<String> fields, boolean isCaseSensititve)
			throws OIMAdapterException {
		// checking the after time as if it is null we can not get the data.
		if(afterTime==null){
			OpsHubLoggingUtil.error(LOGGER, "after time can not be null while getting history : ", null);
			throw new FogBugzAdapterException("0009", null, null);
		}

		SimpleDateFormat format = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
		// Convert the time to system understandable time
		String afterQueryTime = (afterTime!=null)?convertToDateAndTimeForQuery(afterTime):null;
		String maxQueryTime = (maxTime!=null)?format.format(maxTime.getTime()):null;
		
		// Creation of query
		String dateString = "\"" + afterQueryTime + ".." + ((maxQueryTime!=null)?maxQueryTime+" " :" ") + "\"";
		String query = "edited:" + dateString + " OR created:" + dateString + " -editedby=\"" + userDisplayName + "\"-opendby=\"" + userDisplayName + "\"";
		
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(FogBugzConstants.QUERY, query);
		properties.put(FogBugzConstants.COLUMNS, FogBugzConstants.IXBUG);
		properties.put(FogBugzConstants.CMD, FogBugzConstants.SEARCH);
		properties.put(FogBugzConstants.TOKEN, token);
		
		String xml = fogBugzConnector.callFogBugzPostAPI(properties);
		
		List<HashMap> xmlListMap;
		
		xmlListMap = callParseXmlForList(xml, FogBugzUtility.FogbugParserCommands.GET_CASES);
				
		if(xmlListMap==null){
			OpsHubLoggingUtil.debug(LOGGER, "No History found since " + afterQueryTime, null);
			return new ArrayList();
		}
		
		return FogBugzUtility.convertListOfHashMapToListOfString(xmlListMap, FogBugzConstants.IXBUG);
	}

	@Override
	public List executeQuery(String query) throws OIMAdapterException {
		if(query==null)
			throw new FogBugzAdapterException("0012", null, null);
		
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(FogBugzConstants.QUERY, query);
		properties.put(FogBugzConstants.COLUMNS, FogBugzConstants.IXBUG);
		properties.put(FogBugzConstants.CMD, FogBugzConstants.SEARCH);
		properties.put(FogBugzConstants.TOKEN, token);
		
		String xml = fogBugzConnector.callFogBugzPostAPI(properties);
		List<HashMap> xmlListMap;
		
		xmlListMap = callParseXmlForList(xml, FogBugzUtility.FogbugParserCommands.GET_CASES);
		
		if(xmlListMap==null)
			return new ArrayList();
		
		return FogBugzUtility.convertListOfHashMapToListOfString(xmlListMap, FogBugzConstants.IXBUG);
	}

	@Override
	public String getScopeId(String internalId, Object entityObject,
			Map<String, Object> systemProperties) throws OIMAdapterException {
		
		return "";
	}

	@Override
	public String getActualEntityType(String entityId, Object entityObject,
			Map<String, Object> systemProperties) throws OIMAdapterException {
		
		return entityType;
	}

	@Override
	public MaxEntityCarrier getMaxUpdateTime(Calendar afterTime, 
			String afterRevisionId) throws OIMAdapterException {
		OpsHubLoggingUtil.debug(LOGGER, "Getting maxumum update time for fogbugz", null);
		
		// checking the after time as if it is null we can not get the data.
		if(afterTime==null){
			OpsHubLoggingUtil.error(LOGGER, "value of after time can not be null to get maxUpdateTime", null);
			throw new FogBugzAdapterException("0009", null, null);
		}
		
		// Creation of query
		String query = "orderby:LastEdited";
			
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(FogBugzConstants.QUERY, query);
		properties.put(FogBugzConstants.COLUMNS, "ixBug,dtLastUpdated");
		properties.put(FogBugzConstants.MAX, "1");
		properties.put("cmd", "search");
		properties.put("token", token);
		String xml = fogBugzConnector.callFogBugzPostAPI(properties);
				
		List<HashMap> xmlListMap;
				
		xmlListMap = callParseXmlForList(xml, FogBugzUtility.FogbugParserCommands.GET_CASES);
		
		if(xmlListMap==null){
			OpsHubLoggingUtil.debug(LOGGER, "No data to get max update time", null);
			return new MaxEntityCarrier(null, null, null);
		}
			
		String dateString = xmlListMap.get(FogBugzConstants.ZERO).get("dtLastUpdated").toString().replace("T", " ").replace("Z", "");
		SimpleDateFormat sdf = new SimpleDateFormat(FogBugzConstants.DATE_FORMATE);
		
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(sdf.parse(dateString));
			
			cal.add(Calendar.SECOND, -90);
		} catch (ParseException e) {
			OpsHubLoggingUtil.error(LOGGER, "error while parsing Date : " + dateString, e);
			throw new FogBugzAdapterException("0006", new String[]{dateString}, e);
		}
		
		MaxEntityCarrier maxEntityCarrier = new MaxEntityCarrier(xmlListMap.get(FogBugzConstants.ZERO).get(FogBugzConstants.IXBUG).toString(), cal, null);
		
		return maxEntityCarrier;
	}

	@Override
	public Map<String, Object> getFieldMap(String fieldName, String value,
			boolean isSystem) {
		Map<String, Object> field = new HashMap<String, Object>();
		field.put(fieldName, value);
		return field;
	}

	@Override
	public boolean isHistoryAvailable() {
		
		return true;
	}

	@Override
	public Calendar getLastChangeTimeByIntegration(Calendar afterTime,
			String internalId, String integrationUserName)
			throws OIMAdapterException {
		OpsHubLoggingUtil.debug(LOGGER, "Getting last change time by integration user", null);
		
		if(afterTime==null){
			OpsHubLoggingUtil.error(LOGGER, "after time can not be null while getting last change time by integration user", null);
			return null;
		}
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(FogBugzConstants.QUERY, "lasteditedby:\"" + userDisplayName + "\" orderby:LastEdited");
		properties.put(FogBugzConstants.COLUMNS, "ixBug,dtLastUpdated");
		properties.put("max", "\"1\"");
		properties.put(FogBugzConstants.CMD, FogBugzConstants.SEARCH);
		properties.put(FogBugzConstants.TOKEN, token);
		
		String xml = fogBugzConnector.callFogBugzPostAPI(properties);
		
		List<HashMap> xmlListMap;
		
		xmlListMap = callParseXmlForList(xml, FogBugzUtility.FogbugParserCommands.GET_CASES);
		
		if(xmlListMap==null || xmlListMap.get(FogBugzConstants.ZERO).isEmpty()){
			OpsHubLoggingUtil.debug(LOGGER, "No data to get last change time by integration user ", null);
			return null;
		}
		
		String dateString = xmlListMap.get(FogBugzConstants.ZERO).get("dtLastUpdated").toString().replace("T", " ").replace("Z", "");

		SimpleDateFormat sdf = new SimpleDateFormat(FogBugzConstants.DATE_FORMATE);
		
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(sdf.parse(dateString));
		} catch (ParseException e) {
			OpsHubLoggingUtil.error(LOGGER, "Error while parsing Date : " + dateString, e);
			throw new FogBugzAdapterException("0006", new String[]{dateString}, e);
		}
		
		return cal;
	}

	@Override
	public List<EntityPrimaryDetailsCarrier> getEntitiesCreatedByIntegrationAfter(
			final Calendar afterTime, final String afterRevisionId,
			final String integrationUserName, final Map<String, Object> ohProperties) throws OIMAdapterException {
		OpsHubLoggingUtil.debug(LOGGER, "Getting entities created by integration after", null);
		
		if(afterTime==null){
			OpsHubLoggingUtil.error(LOGGER, "after time can not be null in getEntitiesCreatedByIntegrationAfter", null);
			throw new FogBugzAdapterException("0009", null, null);
		}
		
		String afterQueryTime = convertToDateAndTimeForQuery(afterTime);
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(FogBugzConstants.QUERY, "created:\"" + afterQueryTime + "..\" createdby:\"" + userDisplayName + "\" OrderBy:\"ixBug\"");
		properties.put(FogBugzConstants.COLUMNS, FogBugzConstants.IXBUG);
		properties.put(FogBugzConstants.CMD, FogBugzConstants.SEARCH);
		properties.put(FogBugzConstants.TOKEN, token);
		
		String xml = fogBugzConnector.callFogBugzPostAPI(properties);
		List<HashMap> xmlListMap = callParseXmlForList(xml, FogBugzUtility.FogbugParserCommands.GET_CASES);
		
		if(xmlListMap==null){
			OpsHubLoggingUtil.debug(LOGGER, "No Data to get the information", null);
			return null;
		}
		
		return FogBugzUtility.convertListOfHashMapToListOfEntityPrimaryDetailsCarrier(xmlListMap, getScopeId(null, null, null), entityType);
	}

	@Override
	public String getRemoteEntityLink(String id) throws OIMAdapterException {
		/* http://sanket.fogbugz.com/default.asp?13 */
		String urlForRemoteLink = xmlRpcURL.substring(FogBugzConstants.ZERO, xmlRpcURL.lastIndexOf("/"));
		return urlForRemoteLink + "/default.asp?" + id;
	}

	@Override
	public Map<String, Object> getSystemProperties(String internalId,
			Object entityObject) throws OIMAdapterException {
		
		return (Map<String, Object>)entityObject;
	}

	@Override
	public Map<String, Object> getCustomProperties(String internalId,
			Object entityObject) throws OIMAdapterException {
		
		return (Map<String, Object>)entityObject;
	}

	@Override
	public String getOHLastUpdateFieldName() throws FogBugzAdapterException {
		return null;
	}

	@Override
	public OIMEntityObject getOIMEntityObject(final String internalId,
			final String operationId,final String stepNumber) throws OIMAdapterException {
		OIMEntityObject oimEntityObject = new OIMEntityObject(internalId, super.getEntityName() , getScopeId(null, null, null), getCurrentState(internalId), null, false);
		return oimEntityObject;
	}
	
	public List<EAIKeyValue> getCurrentState(String internalId) throws OIMAdapterException
	{
		Object entityObject = getEntityObject(internalId);
		return EaiUtility.convertMapToEAIKeyValue((Map)entityObject);
	}
	@Override
	public List<FieldsMeta> getFieldsMetadata() throws OIMAdapterException {
		List<FieldsMeta> fieldsMetaList = new ArrayList<FieldsMeta>();
		
		fieldsMetaList.add(new FieldsMeta("sTitle", "Title", DataType.TEXT, true));
		fieldsMetaList.add(new FieldsMeta("sProject", "Project", DataType.LOOKUP, true));
		fieldsMetaList.add(new FieldsMeta("sArea", "Area", DataType.LOOKUP, true));
		fieldsMetaList.add(new FieldsMeta("sFixFor", "Milestone", DataType.LOOKUP, true));
		fieldsMetaList.add(new FieldsMeta("sStatus", "Status", DataType.LOOKUP, true));
		fieldsMetaList.add(new FieldsMeta("sPriority", "Priority", DataType.LOOKUP, true));
		fieldsMetaList.add(new FieldsMeta("ixPersonAssignedTo", "Assigned to", DataType.USER, true));
		fieldsMetaList.add(new FieldsMeta("ixPerson", "Person Edited By", DataType.USER, true));
		fieldsMetaList.add(new FieldsMeta("sCategory", "Category", DataType.LOOKUP, true));
		fieldsMetaList.add(new FieldsMeta("tags", "tags", DataType.TEXT, true));
		fieldsMetaList.add(new FieldsMeta("sReleaseNotes", "Release Notes", DataType.TEXT, true));
		fieldsMetaList.add(new FieldsMeta("ixBug", "Case Id", DataType.TEXT, true));
		fieldsMetaList.add(new FieldsMeta(FogBugzConstants.BACKLOG, FogBugzConstants.BACKLOG, DataType.TEXT, true));
		fieldsMetaList.add(new FieldsMeta("hrsCurrEst", "Estimate", DataType.TEXT, true));
		fieldsMetaList.add(new FieldsMeta("hrsElapsedExtra", "Hours Elapsed", DataType.TEXT, true));
		fieldsMetaList.add(new FieldsMeta("sVersion", "Version", DataType.TEXT, true));
		fieldsMetaList.add(new FieldsMeta("sComputer", "Computer", DataType.TEXT, true));
		fieldsMetaList.add(new FieldsMeta("dtDue", "Date due", DataType.DATE_STRING, true));
		
		
		List<String> customFieldList = getListOfCustomProperties();
		
		for (String customField : customFieldList)
		{
			if(!customField.equals("plugin_customfields"))
				fieldsMetaList.add(new FieldsMeta(customField, customField, DataType.TEXT, false));
		}
		
		return fieldsMetaList;
	}

	@Override
	public List<String> getEnumerationsForField(String fieldName)
			throws OIMAdapterException {
		if(fieldName==null || fieldName.isEmpty())
			throw new FogBugzAdapterException("0004", null, null);
		
		String listFieldName = null;
		String tag = null;
		String key = null;
		Map<String, String> queryProperties = new HashMap<String, String>();
		
		if(fieldName.equals("ixProject") || fieldName.equals("sProject") || fieldName.equals("fbzProjectId")){
			listFieldName = "listProjects";
			tag = FogBugzUtility.FogbugParserCommands.GET_PROJECTS;
			key = "sProject";
		}
		else if(fieldName.equals("ixArea") || fieldName.equals("sArea")){
			listFieldName = "listAreas";
			tag = FogBugzUtility.FogbugParserCommands.GET_AREA;
			key = "sArea";
		}
		else if(fieldName.equals("ixCategory") || fieldName.equals("sCategory")){
			listFieldName = "listCategories";
			tag = FogBugzUtility.FogbugParserCommands.GET_CATEGORIES;
			key = "sCategory";
		}
		else if(fieldName.equals("ixPriority") || fieldName.equals("sPriority")){
			listFieldName = "listPriorities";
			tag = FogBugzUtility.FogbugParserCommands.GET_PRIORITY;
			key = "sPriority";
		}
		else if(fieldName.equals("ixStatus") || fieldName.equals("sStatus")){
			listFieldName = "listStatuses";
			tag = FogBugzUtility.FogbugParserCommands.GET_STATUSES;
			key = "sStatus";
		}
		else if(fieldName.equals("Milestone") || fieldName.equals("sFixFor")){
			listFieldName="listFixFors";
			tag = FogBugzUtility.FogbugParserCommands.GET_FIXFOR;
			key = "sFixFor";
			queryProperties.put("fIncludeDeleted", "1");
			queryProperties.put("fIncludeReallyDeleted", "1");
		}
		
		if(listFieldName==null)
			throw new FogBugzAdapterException("0004", null, null);
		queryProperties.put(FogBugzConstants.CMD, listFieldName);
		queryProperties.put(FogBugzConstants.TOKEN, token);
		
		String xml = fogBugzConnector.callFogBugzPostAPI(queryProperties);
		
		List<HashMap> list = callParseXmlForList(xml, tag);
		if(key !=null && key.equals("sStatus"))
		{
			Set<String> statusList = null;
			HashMap<String, Object> properties = new HashMap<String, Object>();
			properties.put("fResolved", "1");
			properties.put(FogBugzConstants.CMD, listFieldName);
			properties.put(FogBugzConstants.TOKEN, token);
			String closedStatusXML = fogBugzConnector.callFogBugzPostAPI(properties);
			List<HashMap> mapStatusList = callParseXmlForList(closedStatusXML, tag);
			Set<String> closedStatusList = FogBugzUtility.convertListOfHashMapToSet(mapStatusList, key);
			if(list != null)
				statusList = FogBugzUtility.convertListOfHashMapToSet(list, key);
			else
				statusList = new HashSet<String>();
			
			if(closedStatusList==null)
			{
				closedStatusList = new HashSet<String>();
			}
			
			statusList.add("Closed");
			for (String statusName : closedStatusList) {
				String subStr =null;
				if(statusName.contains("(")){
					subStr = statusName.substring(statusName.indexOf("("), statusName.lastIndexOf(")")+1);
					subStr = "Closed "+ subStr;
				}else{
					subStr = "Closed (" + statusName + ")";
				}
				if(subStr != null)
					statusList.add(subStr);
			}
			return new ArrayList<String>(statusList);
		}else{
			return new ArrayList<String>(FogBugzUtility.convertListOfHashMapToSet(list, key));
		}
	}

	@Override
	public List<UserMeta> getUserList() throws OIMAdapterException {
		List<HashMap> peopleMap = getListOfUsers();
		List<UserMeta> returnList = null;
		
		for(HashMap people : peopleMap){
			if(returnList==null)
				returnList = new ArrayList<UserMeta>();
			
			UserMeta userMeta = new UserMeta();
			
			userMeta.setUserDisplayName(people.get("sFullName").toString());
			userMeta.setUserEmail(people.get("sEmail").toString());
			userMeta.setUserId(people.get("ixPerson").toString());
			userMeta.setUserName(people.get("sEmail").toString());
			
			returnList.add(userMeta);
		}
		
		return returnList;
	}

	@Override
	public boolean isAttachmentSupported(boolean isSourceSystem)
			throws OIMAdapterException {
		
		return true;
	}

	@Override
	public boolean isRelationShipSupported(boolean isSourceSystem)
			throws OIMAdapterException {
		
		return true;
	}

	@Override
	public boolean isCommentsOrNotesSupported(boolean isSourceSystem)
			throws OIMAdapterException {
		return true;
	}

	@Override
	@Deprecated
	public boolean isCommentOrNotesHTML() throws OIMAdapterException {
		return false;
	}

	@Override
	public boolean isCommentOrNotesHTML(boolean isSource)
			throws OIMAdapterException {
		// As source it will return HTML so if it's true for source we need to return true.
		return isSource;
	}
	
	@Override
	public String getHtmlReplacementForNewLine() {
		
		return null;
	}
	
	@Override
	public List<String> getEmailFromUserName(String userName,
			boolean caseSensitive) throws OIMAdapterException {
		List<String> returnList = new ArrayList<String>();
		
		// In fogbugz email id is used as user name. Thus returning userName itself
		if(userName != null && !userName.isEmpty())
			returnList.add(userName);
		
		return returnList;
	}

	@Override
	public List<String> getUserNameFromEmail(String email, boolean caseSensitive)
			throws OIMAdapterException {
		List<String> returnList = new ArrayList<String>();
		
		// In fogbugz email id is used as user name. Thus returning email id itself
		if(email != null && !email.isEmpty())
			returnList.add(getUserId(email,caseSensitive));
		
		return returnList;
	}

	@Override
	public List<String> getUserFullNameFromEmail(String email,
			boolean caseSensitive) throws OIMAdapterException {
		Map<String, Object> userMap = getUserMapWithKey(FogBugzConstants.EMAIL);
		List<String> user = null;
		if(userMap.get(email)!=null && !userMap.get(email).toString().isEmpty()){
			user = new ArrayList<String>();
			user.add(((HashMap)userMap.get(email)).get(FogBugzConstants.DISPLAYNAME).toString());
		}
		return user;
	}

	@Override
	protected OIMEntityObject createEntity(String externalId,
			Map<String, Object> systemProperties,
			Map<String, Object> customProperties, boolean isRetry)
			throws OIMAdapterException {
		Map<String, Object> properties = EaiUtility.addMapValues(systemProperties, customProperties);
		
		checkAndUpdatePropertyInternalNames(properties,true);
		
		// send request to create new entity
		properties.put(FogBugzConstants.CMD, FogBugzConstants.NEW);
		properties.put(FogBugzConstants.TOKEN, token);
		String responseXML = fogBugzConnector.callFogBugzPostAPI(properties);
		if(responseXML==null){
			throw new FogBugzAdapterException("0021",new String[]{"Response from server can not be null."},null);
		}
		String internalId;
		
		try{
			internalId = FogBugzUtility.ParseInternalId(responseXML);
		}catch(IOException e){
			throw new FogBugzAdapterException("0022", new String[]{e.getLocalizedMessage()}, e);
		}catch(SAXException e){
			throw new FogBugzAdapterException("0023", new String[]{e.getLocalizedMessage()}, e);
		}
		
		if(internalId == null)
		{
			throw new FogBugzAdapterException("0021",new String[]{"Unable to find Internal id for Case."}, null);
		}
		return new OIMEntityObject(internalId, FogBugzConstants.CASE, getScopeId(null, null, null), getCurrentState(internalId), null, true);
	}

	@Override
	protected OIMEntityObject updateEntity(String internalId,
			Object entityObject, Map<String, Object> systemProperties,
			Map<String, Object> customProperties, String externalIdValue,
			boolean isRetry, String operationId, Integer subStep)
			throws OIMAdapterException {
		Map<String, Object> properties = EaiUtility.addMapValues(systemProperties, customProperties);
		
		checkAndUpdatePropertyInternalNames(properties, false);
		
		String command = null;
		// putting the bug id to update
		properties.put(FogBugzConstants.IXBUG, internalId);
		if(systemProperties.get("sStatus") != null)
		{
			String status = systemProperties.get("sStatus").toString();
			
			if(status.toLowerCase().contains("close"))
				command = FogBugzConstants.CMD_CLOSE;
			
			else if(status.toLowerCase().contains("reopen"))
				command = FogBugzConstants.CMD_REOPEN;
			
			else
				command = "edit";
		}
		else
			command = "edit";
		
		properties.put("cmd", command);
		properties.put("token", token);
		fogBugzConnector.callFogBugzPostAPI(properties);
		
		return new OIMEntityObject(internalId, FogBugzConstants.CASE, getScopeId(null, null, null), getCurrentState(internalId), null, true);
	}
	
	/**
	 * This method is used to get the user unique id from FogBugz using email id   
	 * @param caseSensitive
	 * @throws FogBugzAdapterException
	 */
	private String getUserId(String email, boolean caseSensitive) throws FogBugzAdapterException{
		String userId = null;
		
		if(email != null && !email.isEmpty()){
			Map<String, Object> userList = getUserMapWithKey(FogBugzConstants.EMAIL, !caseSensitive);
			
			if(!caseSensitive)
				email = email.toLowerCase();
				
			if(userList.get(email)==null)
				throw new FogBugzAdapterException("0020", new String[]{email}, null);
			
			userId = ((HashMap)userList.get(email)).get(FogBugzConstants.PERSON_ID).toString();
			
		}
		
		return userId;
	}
	
	/**
	 * this function is used to convert the calendar to date that FogBugz can understand.
	 * mm-dd-yyyy hh:mm:ss
	 * @param cal
	 * @return
	 */
	public String convertToDateAndTimeForQuery(Calendar cal){
		return convertToDateAndTimeForQuery(cal, true);
	}
	
	public String convertToDateAndTimeForQuery(Calendar cal, boolean addSec){
		Calendar tempCal =  Calendar.getInstance();
		tempCal.setTimeInMillis(cal.getTimeInMillis());
		if(addSec)
			tempCal.add(Calendar.SECOND, 31);
		Date calDate = tempCal.getTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String formattedDate = format.format(calDate);
		return formattedDate;
	}
	
	/**
	 * This function gets the list of users and converts it in to hashmap with key specified in argument
	 * @param keyName
	 * @return
	 * @throws FogBugzAdapterException
	 */
	public Map<String, Object> getUserMapWithKey(String keyName) throws FogBugzAdapterException{
		return getUserMapWithKey(keyName, false);
	}
	
	/**
	 * This function gets the list of users and converts it in to hashmap with key specified in argument
	 * @param keyName
	 * @return
	 * @throws FogBugzAdapterException
	 */
	public Map<String, Object> getUserMapWithKey(String keyName, boolean caseSensitive) throws FogBugzAdapterException{
		Boolean needToRefresh = true;
		
		if(userList!=null && !userList.isEmpty())
			needToRefresh = false;
		
		if(needToRefresh){
			userList = getListOfUsers();
		}
		
		Map<String, Object> userMap = new HashMap<String, Object>();
		for(HashMap user : userList){
			if(caseSensitive)
				userMap.put(user.get(keyName).toString().toLowerCase(), user);
			else
				userMap.put(user.get(keyName).toString(), user);
		}
		
		return userMap;
	}

	@Override
	public Map<String, String> getMappedPropertyName() throws OIMAdapterException
	{
		Map<String, String> properties = new HashMap<String, String>();
		properties.put(RulesVerifyInterface.OH_STATUS, FogBugzConstants.STATUS);
		return properties;
	}

	@Override
	public boolean userExists(String userName, boolean isCaseSensitive) throws OIMAdapterException
	{
		Iterator<UserMeta> userItr = getUserList().iterator();
		while(userItr.hasNext()){
			String user = userItr.next().getUserName();
			if((isCaseSensitive && user.equals(userName)) || (!isCaseSensitive && user.equalsIgnoreCase(userName))){
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean projectExists(String projectName, boolean isCaseSensitiv) throws OIMAdapterException
	{
		return getEnumerationsForField("sProject").contains(projectName);
	}

	@Override
	public InputStream getAttachmentInputStream(String attachmentURI) throws OIMAdapterException
	{
		URL url=null;
		URLConnection conn=null;
		InputStream inputStream=null;
		String appendToken="&token=";
		try
		{
			String token=getTokenForAttachment();
			if(token==null)
			{
				OpsHubLoggingUtil.debug(LOGGER, "Error in getting token for downloading attachment from server.", null);
				throw new FogBugzAdapterException("0017", new String []{"Error in getting token for downloading attachment from server."}, null);
			}
			attachmentURI=validateUrl(attachmentURI);
			url = new URL(attachmentURI+appendToken+token);
			conn=url.openConnection();
			inputStream=conn.getInputStream();
			if(inputStream==null)
			{
				OpsHubLoggingUtil.debug(LOGGER, "Error in getting attachment file from server", null);
				throw new FogBugzAdapterException("0017", new String []{"Error in getting attachment input stream from server."}, null);
			}
		}
		catch (MalformedURLException e)
		{
			OpsHubLoggingUtil.debug(LOGGER, "Error in URL for getting attachment from server.", null);
			throw new FogBugzAdapterException("0017", new String []{"Error in URL for getting attachment from server."}, e);
		}
		catch (IOException e)
		{
			OpsHubLoggingUtil.debug(LOGGER, "Error in getting attachment file from server", null);
			throw new FogBugzAdapterException("0017", new String []{"Error in getting attachment file from server."}, e);
		}
		return inputStream;
	}
	
	private String validateUrl(String Url)
	{
		if(!Url.startsWith("https"))
		{
			Url=Url.replace("http", "https");
		}
		return Url;
	}
	
	private Map getAttachmentAsMap(String entityId, boolean excludeIntegrationUser) throws FogBugzAdapterException
	{
		Map<String, Object> properties=new HashMap<String, Object>();
		
		properties.put(FogBugzConstants.COLUMNS, "events");
		properties.put(FogBugzConstants.QUERY, entityId);
		properties.put(FogBugzConstants.CMD, FogBugzConstants.SEARCH);
		properties.put(FogBugzConstants.TOKEN, token);
		String xml=fogBugzConnector.callFogBugzPostAPI(properties);
		
		Map returnMap;
		
		try{
			returnMap =FogBugzUtility.getAttachmentCurrentState(xml,userDisplayName, excludeIntegrationUser); 
		}catch(IOException e){
			throw new FogBugzAdapterException("0022", new String[]{e.getLocalizedMessage()}, e);
		}catch(SAXException e){
			throw new FogBugzAdapterException("0023", new String[]{e.getLocalizedMessage()}, e);
		}
		
		return returnMap;
	}
	
	/**This method is used for getting token for downloading the attachment file from FogBugz server.
	 * @return token
	 * @throws IOException
	 * @throws FogBugzAdapterException 
	 */
	private String getTokenForAttachment() throws IOException, FogBugzAdapterException
	{
		 	HashMap<String, String> map = new HashMap<String, String>(); 
		 	String xmlUrl=xmlRpcURL;
		    map.put("password", password);
		    map.put("email", integrationUser);
		    map.put("cmd","logon");
		    if(!xmlUrl.startsWith("https"))
		    {
		    	xmlUrl=xmlUrl.replace("http", "https");
		    }
		    String token = null;
				URL url = new  URL(xmlUrl+"?");
				FogBugzHTTP bugzHTTP = new FogBugzHTTP("POST", url, map);
				InputStream inputStream= bugzHTTP.execute();
				StringBuffer buffer = new StringBuffer();
				byte[] b = new byte[4096];
				for (int n; (n = inputStream.read(b)) != -1;) {
				  buffer.append(new String(b, FogBugzConstants.ZERO, n));
				}
				token = buffer.toString();
		    List<HashMap> tokenMap;
		    
		    try{
		    	tokenMap=FogBugzUtility.ParseToken(token);
		    }catch(IOException e){
				throw e;
			}catch(SAXException e){
				throw new FogBugzAdapterException("0023", null, e);
			}
		    
		    if(tokenMap.get(FogBugzConstants.ZERO)==null || tokenMap.get(FogBugzConstants.ZERO).isEmpty())
		    {
		    	return null;
		    }
		    return FogBugzUtility.convertListOfHashMapToListOfString(tokenMap, "token").get(FogBugzConstants.ZERO);
	}

	@Override
	public EAIEntityRefrences getCurrentLinks(String internalId,
			Object entityObject, String linkType, Map<String, String> properties)
			throws OIMAdapterException {
		EAIEntityRefrences references = null;
		String columns = null;
		
		// Decision for parent or child if non is given then return null as we can not get information without it.
		if(linkType != null && !linkType.isEmpty()){
			if(linkType.equalsIgnoreCase(FogBugzConstants.PARENT)){
				columns = FogBugzConstants.FogBugz_PARENT;
			}
			else if (linkType.equalsIgnoreCase(FogBugzConstants.CHILD)) {
				columns = FogBugzConstants.FogBugz_CHILD;
			}
		}
		else{
			return null;
		}
		
		// Execute query
		Map<String, Object> queryProperties = new HashMap<String, Object>();
		queryProperties.put(FogBugzConstants.QUERY, internalId);
		queryProperties.put(FogBugzConstants.COLUMNS, columns);
		queryProperties.put(FogBugzConstants.CMD, FogBugzConstants.SEARCH);
		queryProperties.put(FogBugzConstants.TOKEN, token);
		
		String xml = fogBugzConnector.callFogBugzPostAPI(queryProperties);
		
		List<HashMap> xmlToList = callParseXmlForList(xml, FogBugzUtility.FogbugParserCommands.GET_CASES);
		
		// Process through the result
		String linkInfo[] = (xmlToList.get(FogBugzConstants.ZERO).get(columns)!=null)?xmlToList.get(FogBugzConstants.ZERO).get(columns).toString().split(","):null;
		
		if(linkInfo!=null && linkInfo.length>FogBugzConstants.ZERO){
			for(String caseId:linkInfo){
				
				if(!caseId.isEmpty() && Integer.parseInt(caseId)!=FogBugzConstants.ZERO){
					if(references == null)
						references =  new EAIEntityRefrences(linkType);
					references.addLinks(FogBugzConstants.CASE, getScopeId(null, null, null), "", caseId);
				}
			}
		}
				
		return references;
	}

	/**
	 * this method gets the current state of case1 and adds case2 to case1
	 * @param linkField
	 * @param case1
	 * @param case2
	 * @throws FogBugzAdapterException
	 */
	private void associateLink(String linkField, String case1, String case2) throws FogBugzAdapterException{
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(FogBugzConstants.TOKEN, token);
		List<String> finalLinks = new ArrayList<String>();
		String linkInfo[]=null;
		// if it is child we need to get the current state of links, otherwise old will be replaced with argumented cases
		if(linkField==null)
			return;
		if(linkField.equals(FogBugzConstants.FogBugz_CHILD)){
			properties.put(FogBugzConstants.QUERY, case1);
			properties.put(FogBugzConstants.COLUMNS, linkField);
			properties.put(FogBugzConstants.CMD, FogBugzConstants.SEARCH);
			
			String xml = fogBugzConnector.callFogBugzPostAPI(properties); 
					//fogBugzConnector.formURLAndGetResultXML(properties, FogBugzConstants.CMD_SEARCH, token);
			List<HashMap> xmlToList = callParseXmlForList(xml, FogBugzUtility.FogbugParserCommands.GET_CASES);
			linkInfo = (xmlToList.get(FogBugzConstants.ZERO).get(linkField)!=null)?xmlToList.get(FogBugzConstants.ZERO).get(linkField).toString().split(","):null;
			if(linkInfo==null)
				return;
			for(int i = FogBugzConstants.ZERO; i<linkInfo.length;i++){
				if(!linkInfo[i].equals("") && !linkInfo[i].equals("0"))
					finalLinks.add(linkInfo[i]);
			}
		}
		// for parent and for child same process  will be there
		finalLinks.add(case2);
		properties.put(FogBugzConstants.IXBUG, case1);
		properties.put(linkField, EaiUtility.getCommaSeperatedListValues(finalLinks, false));
		properties.put("cmd", "edit");
		fogBugzConnector.callFogBugzPostAPI(properties);
	}

	@Override
	public List<String> getEntitySupportingLinks(boolean isSourceSystem) {
		List<String> returnList = new ArrayList<String>();
		returnList.add(FogBugzConstants.CASE);
		return returnList;
	}

	@Override
	public List<String> getSupportedLinkTypes(String entityName,
			boolean isSourceSystem) {
		List<String> returnList = new ArrayList<String>();
		returnList.add(FogBugzConstants.PARENT);
		returnList.add(FogBugzConstants.CHILD);
		return returnList;
	}
	
	@Override
	public void cleanup(){
		try {
			fogBugzConnector.logout(token);
		} catch (FogBugzAdapterException e) {
			OpsHubLoggingUtil.error(LOGGER, "Unable to logout form FogBugz", e);
		} catch (RequestorException e) {
			OpsHubLoggingUtil.error(LOGGER, "Unable to logout form FogBugz", e);
		}
	}
	
	@Override
	public boolean isAttachmentUpdateSupported()
	{
		return false;
	}

	@Override
	public boolean isAttachmentDeleteSupported()
	{
		return true;
	}

	@Override
	public boolean isAttachmentHistorySupported()
	{
		return false;
	}

	@Override
	public void deleteAttachment(String internalId, EAIAttachment attachment) throws OIMAdapterException
	{
		if(attachment == null)
		{
			throw new FogBugzAdapterException("0017", new String []{"Attachment data should not be null."}, null);
		}
		
		String attachmentId = null, condition = null;
		
		if(attachment.getAttachmentURI()!=null && !attachment.getAttachmentURI().isEmpty()){
			Map<String, String> attachmentProperties =  FogBugzUtility.getQueryMap(attachment.getAttachmentURI().replace("&amp;", "&"));
			
			if(attachmentProperties!=null && !attachmentProperties.isEmpty() && attachmentProperties.get("ixAttachment")!=null && !attachmentProperties.get("ixAttachment").isEmpty()){
				attachmentId = attachmentProperties.get("ixAttachment");
			}
		}
		
		if(attachmentId!=null)
			condition = "ixAttachment=" + attachmentId;
		else
			condition = "sFileName=" + attachment.getFileName().replace(" ", "%20");
		
		OpsHubLoggingUtil.debug(LOGGER, "delete attachment condition : " + condition, null);
		OpsHubLoggingUtil.debug(LOGGER, "deleteing attachment : " + attachment.getFileName() + " for CaseID : " + internalId, null);
		
		String token;
		HashMap attachmentFile=null;
		try
		{
			attachmentFile=(HashMap) getAttachmentAsMap(internalId, false);
			
			Iterator<Entry> itr = attachmentFile.entrySet().iterator();
			while(itr.hasNext()){
				Entry entry = itr.next();
				if(entry.getKey().toString().contains(condition)){
					String urlStr=entry.getKey().toString();
					urlStr=urlStr.replace("&amp;", "&");
					
					Map<String, String> queryValue=FogBugzUtility.getQueryMap(urlStr);
					token = getTokenForAttachment();
					HashMap map1 = new HashMap();
					map1.put("token", token);
					map1.put("ixAttachment", queryValue.get("ixAttachment").toString());
					map1.put("ixBug", internalId);
					map1.put("command","edit");
					map1.put("ixBugEvent",queryValue.get("ixBugEvent").toString());
					map1.put("pg","pgDownload");
					map1.put("pre","preDeleteFile");
	
					String removeUrl=xmlRpcURL.replace("api.asp", "default.asp?");
					if(!removeUrl.startsWith("https"))
					{
						removeUrl=removeUrl.replace("http", "https");
					}
					URL url = new  URL(removeUrl);
	
					FogBugzHTTPGet bugzHTTPGet = new FogBugzHTTPGet("GET", url, map1);
	
					InputStream inputStream= bugzHTTPGet.execute();
				}
			}
		}
		catch (IOException e)
		{
			OpsHubLoggingUtil.debug(LOGGER, "Error in deleting file from case.", null);
			throw new FogBugzAdapterException("0017", new String []{"Error in deleting file from case."}, e);
		}
	}

	@Override
	public List<EAIAttachment> getCurrentAttachments(String internalId, Object entityObject) throws OIMAdapterException
	{
		Map attachments = getAttachmentAsMap(internalId, false);
		List<EAIAttachment> eaiAttachmentList = new ArrayList<EAIAttachment>();
		EAIAttachment eaiAttachment = null;
		if(attachments == null)
		{
			throw new FogBugzAdapterException("0017", new String []{"Error in getting attachment data."}, null);
		}
		
		for(Iterator<Entry> itr = attachments.entrySet().iterator();itr.hasNext();)
		{
			Entry entry = itr.next();
			if(entry.getKey() != null)
			{
				eaiAttachment = new EAIAttachment();
				eaiAttachment.setFileName(entry.getValue().toString());
				eaiAttachment.setAttachmentURI(FogBugzUtility.getAttachmentURL(xmlRpcURL,entry.getKey().toString()));
				eaiAttachment.setUpdateTimeStamp(0);
				eaiAttachmentList.add(eaiAttachment);
			}
		}
		
		return eaiAttachmentList;
	}

	@Override
	public EAIAttachment addAttachment(String internalId, InputStream byteInputStream, String label, String fileName, String contentType, String addedByUser, Map<String, Object> additionalParameters) throws OIMAdapterException
	{
		EAIAttachment eaiAttachment = new EAIAttachment();
		List<EAIAttachment> listAttachment = new ArrayList<EAIAttachment>();
		HashMap<String ,String> map = new HashMap<String, String>();
		String xmlUrl=null;
		URL url=null;
		try
		{
			String token=getTokenForAttachment();
				xmlUrl=validateUrl(xmlRpcURL);	
				url = new  URL(xmlUrl+"?");
			if(token==null)
			{
				OpsHubLoggingUtil.debug(LOGGER, "Error in getting token for Adding attachment to Case {addAttachment}.", null);
				throw new FogBugzAdapterException("0017", new String []{"Error in getting token for Adding attachment to Case {addAttachment}."}, null);
			}
			map.put("cmd","edit");
			map.put("token",token);
			map.put("ixBug",internalId);
			String mimeType= URLConnection.guessContentTypeFromName(fileName);
			HTTPAttachementPost attachement = new HTTPAttachementPost("POST", url, map,fileName,mimeType,byteInputStream);
			attachement.execute();
		}
		catch (MalformedURLException e)
		{
			OpsHubLoggingUtil.debug(LOGGER, "Error in URL for adding attachment to the case.", null);
			throw new FogBugzAdapterException("0017", new String []{"Error in URL for adding attachment to the case."}, e);
		}
		catch (IOException e)
		{
			OpsHubLoggingUtil.debug(LOGGER, "Error in adding attachment to the case", null);
			throw new FogBugzAdapterException("0017", new String []{"Error in adding attachemt to the case."}, e);
		}
		
		listAttachment = getCurrentAttachments(internalId, null);
		for(EAIAttachment attachment : listAttachment){
			if(attachment.getFileName().equalsIgnoreCase(fileName)){
				eaiAttachment.setAttachmentURI(attachment.getAttachmentURI());
				break;
			}
		}
		
		eaiAttachment.setAddedByUser(addedByUser);
		eaiAttachment.setFileName(fileName);
		eaiAttachment.setLabel(label);
		eaiAttachment.setUpdateTimeStamp(0);
		eaiAttachment.setContentType(contentType);
		return eaiAttachment;
	}

	@Override
	public List<LinkTypeMeta> getSupportedLinkTypesMeta(String entityName, boolean isSourceSystem) throws OIMAdapterException
	{
		if(fogbugzLinkTypeMeta==null){
			fogbugzLinkTypeMeta = new ArrayList<LinkTypeMeta>();
			
			fogbugzLinkTypeMeta.add(new LinkTypeMeta(FogBugzConstants.PARENT, FogBugzConstants.CHILD));
			fogbugzLinkTypeMeta.add(new LinkTypeMeta(FogBugzConstants.CHILD, FogBugzConstants.PARENT));
		}
		
		return fogbugzLinkTypeMeta;
	}

	@Override
	public boolean isLinkHistorySupported()
	{
		return true;
	}

	@Override
	public boolean isLinkRemoveSupported()
	{
		return true;
	}

	@Override
	public String getLinkedEntityScopeId(String linkEntityType, Map<String, String> properties) throws OIMAdapterException
	{
		return getScopeId(null, null, null);
	}

	@Override
	public void removeLink(String internalId, String linkType, String linkEntityInternalId, String linkEntityType, Map<String, String> properties) throws OIMAdapterException
	{
		// Getting current state of link
				EAIEntityRefrences references = getCurrentLinks(internalId, null, linkType, null);
				if(references==null){
					return;
				}
				List<EAILinkEntityItem> links = references.getLinks();
				if(links==null || links.isEmpty())
					return;
				List<String> linkIds = new ArrayList<String>();
				
				// Adding all links to a list
				for(EAILinkEntityItem link:links){
					if(!link.getEntityInternalId().equals(linkEntityInternalId)){
						linkIds.add(link.getEntityInternalId());
					}
				}
				
				Map<String, Object> queryProperties = new HashMap<String, Object>();
				if(!linkIds.isEmpty()){
					queryProperties.put((linkType.equals(FogBugzConstants.PARENT)?FogBugzConstants.FogBugz_PARENT:FogBugzConstants.FogBugz_CHILD), EaiUtility.getCommaSeperatedListValues(linkIds,false));
				}
				else{
					queryProperties.put((linkType.equals(FogBugzConstants.PARENT)?FogBugzConstants.FogBugz_PARENT:FogBugzConstants.FogBugz_CHILD), FogBugzConstants.ZERO.toString());
				}
				
				// Adding all the links as FogBugz replaces the value of parent/child
				queryProperties.put(FogBugzConstants.IXBUG, internalId);
				queryProperties.put(FogBugzConstants.CMD, "edit");
				queryProperties.put(FogBugzConstants.TOKEN, token);
				fogBugzConnector.callFogBugzPostAPI(queryProperties);
	}

	@Override
	public void addLink(String internalId, String linkType, String linkEntityInternalId, String linkEntityType, Map<String, String> properties) throws OIMAdapterException
	{
		if(linkEntityInternalId.isEmpty()){
			throw new FogBugzAdapterException("0018", new String[]{linkEntityInternalId}, null);
		}
		
		// Checking parent or child
		if(linkType !=null && !linkType.isEmpty() && linkType.equalsIgnoreCase(FogBugzConstants.CHILD)){
			associateLink(FogBugzConstants.FogBugz_CHILD, internalId, linkEntityInternalId);
		}
		else{
			associateLink(FogBugzConstants.FogBugz_PARENT, internalId, linkEntityInternalId);
		}
	}

	@Override
	public EAIAttachment updateAttachment(String internalId, EAIAttachment attachment, InputStream byteInputStream) throws OIMAdapterException
	{
		OpsHubLoggingUtil.debug(LOGGER, "Update Attachment operation not supported", null);
		throw new FogBugzAdapterException("0017", new String []{"Update Attachment operation not supported"}, null);
	}
	
	private List getListOfUsers() throws FogBugzAdapterException{
		Map<String, String> properties = new HashMap<String, String>();
		properties.put(FogBugzConstants.CMD, "listPeople");
		properties.put(FogBugzConstants.TOKEN, token);
		properties.put("fIncludeVirtual", "1");
		properties.put("fIncludeNormal", "1");
		properties.put("fIncludeCommunity", "1");
		properties.put("fIncludeDeleted", "1");
		properties.put("fIncludeActive", "1");
		String xml = fogBugzConnector.callFogBugzPostAPI(properties);
		return callParseXmlForList(xml, FogBugzUtility.FogbugParserCommands.GET_PEOPLES);
	}
	
	public List<String> getListOfCustomProperties() throws FogBugzAdapterException{
		if(customProperties!=null)
			return customProperties;
		
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(FogBugzConstants.COLUMNS, "plugin_customfields");
		properties.put(FogBugzConstants.MAX, "\"1\"");
		properties.put(FogBugzConstants.CMD,FogBugzConstants.SEARCH);
		properties.put(FogBugzConstants.TOKEN, token);
		String xml = fogBugzConnector.callFogBugzPostAPI(properties);
		try{
			customProperties = FogBugzUtility.ParseCustomeFields(xml, FogBugzUtility.FogbugParserCommands.GET_CASES);
		}catch(IOException e){
			throw new FogBugzAdapterException("0022", new String[]{e.getLocalizedMessage()}, e);
		}catch(SAXException e){
			throw new FogBugzAdapterException("0023", new String[]{e.getLocalizedMessage()}, e);
		}
		
		return customProperties;
	}

	@Override
	public String getDateFormat(String fieldName, boolean isSource) {
		return FogBugzConstants.DATE_FORMATE;
	}
	
	private List<HashMap> callParseXmlForList(String xml, String parseCommand) throws FogBugzAdapterException{
		List<HashMap> xmlListMap;
		
		try{
			xmlListMap = FogBugzUtility.ParseXmlForList(xml, parseCommand);
		}catch(IOException e){
			throw new FogBugzAdapterException("0022", new String[]{e.getLocalizedMessage()}, e);
		}catch(SAXException e){
			throw new FogBugzAdapterException("0023", new String[]{e.getLocalizedMessage()}, e);
		}
		
		return xmlListMap;
	}
	
	/**
	 * This method changes the field name with internal name.
	 * It is necessary for the fields which are different at the time of polling and writing.
	 * for example at the time of polling changing person comes in "ixPerson" but at the time of writing we need to send "ixPersonEditedBy".
	 * @param properties
	 * @param isCreate - at the time of create we need to put the project in map if it doesn't exist.
	 */
	private void checkAndUpdatePropertyInternalNames(Map properties, boolean isCreate) {
		if(properties.containsKey(FogBugzConstants.PERSON_ID) && !properties.get(FogBugzConstants.PERSON_ID).toString().isEmpty()){
			properties.put("ixPersonEditedBy", properties.remove(FogBugzConstants.PERSON_ID));
		}
		if(properties.containsKey(FogBugzConstants.TAGS)){
			properties.put(FogBugzConstants.STAGS, properties.remove(FogBugzConstants.TAGS));
		}
		if(properties.containsKey(FogBugzConstants.BACKLOG)){
			properties.put("plugin_projectbacklog_at_fogcreek_com_ibacklog", properties.remove(FogBugzConstants.BACKLOG));
		}
		if(isCreate && (properties.get(FogBugzConstants.PROJECT_NAME)==null || properties.get(FogBugzConstants.PROJECT_NAME).toString().isEmpty())){
			properties.put(FogBugzConstants.PROJECT_NAME, projectName);
		}
	}

	@Override
	public List<String> getUserNameFromFullName(String fullName, boolean caseSensitive)
			throws OIMAdapterException {
				throw new UnsupportedOperationException("Operation get user name from full name is not supported.");
			}
}
