//Copyright C 2011 OpsHub, Inc. All rights reserved
package com.opshub.eai.jira.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.xml.sax.SAXException;

import com.atlassian.jira.rpc.soap.beans.RemoteComment;
import com.atlassian.jira.rpc.soap.beans.RemoteCustomFieldValue;
import com.atlassian.jira.rpc.soap.beans.RemoteField;
import com.atlassian.jira.rpc.soap.beans.RemoteFieldValue;
import com.atlassian.jira.rpc.soap.beans.RemoteIssue;
import com.atlassian.jira.rpc.soap.beans.RemotePriority;
import com.atlassian.jira.rpc.soap.beans.RemoteProject;
import com.atlassian.jira.rpc.soap.beans.RemoteResolution;
import com.atlassian.jira.rpc.soap.beans.RemoteServerInfo;
import com.atlassian.jira.rpc.soap.beans.RemoteStatus;
import com.atlassian.jira.rpc.soap.beans.RemoteUser;
import com.opshub.dao.core.ActionResult;
import com.opshub.dao.core.JiraAction;
import com.opshub.dao.jira.JiraCustomFields;
import com.opshub.eai.CommentSettings;
import com.opshub.eai.Constants;
import com.opshub.eai.EAIAttachment;
import com.opshub.eai.EAIComment;
import com.opshub.eai.EAIEntityRefrences;
import com.opshub.eai.EAIKeyValue;
import com.opshub.eai.EAIRecoveryUtility;
import com.opshub.eai.EaiUtility;
import com.opshub.eai.OIMEntityObject;
import com.opshub.eai.core.adapters.OIMConnector;
import com.opshub.eai.core.carriers.EntityPrimaryDetailsCarrier;
import com.opshub.eai.core.carriers.MaxEntityCarrier;
import com.opshub.eai.core.carriers.SubStepsDetailsCarrier;
import com.opshub.eai.core.exceptions.OIMAdapterException;
import com.opshub.eai.core.interfaces.HasLinkSupport;
import com.opshub.eai.core.interfaces.HasReverseLinkSupport;
import com.opshub.eai.core.interfaces.HasStringDate;
import com.opshub.eai.core.interfaces.HasUserLookup;
import com.opshub.eai.core.interfaces.IAttachmentProcessor;
import com.opshub.eai.core.interfaces.ILinkSupport;
import com.opshub.eai.core.interfaces.OHFieldMappingInterface;
import com.opshub.eai.core.interfaces.RulesVerifyInterface;
import com.opshub.eai.core.utility.OIMCoreUtility;
import com.opshub.eai.jira.common.JiraConstants;
import com.opshub.eai.jira.common.JiraConstants.JQL;
import com.opshub.eai.jira.common.JiraDbAccessClient;
import com.opshub.eai.jira.common.JiraThreadLocal;
import com.opshub.eai.jira.common.JiraUtility;
import com.opshub.eai.jira.common.JiraWebServiceClient;
import com.opshub.eai.jira.common.JiraWorkFlowParser;
import com.opshub.eai.jira.common.rest.JiraRestRequestor;
import com.opshub.eai.jira.exceptions.OIMJiraAdapterException;
import com.opshub.eai.jira.exceptions.OIMJiraApiException;
import com.opshub.eai.jira.exceptions.OIMJiraDBAccessException;
import com.opshub.eai.jira.exceptions.OIMJiraPermissionException;
import com.opshub.eai.jira.rest.JIRARESTConstants;
import com.opshub.eai.metadata.DataType;
import com.opshub.eai.metadata.FieldsMeta;
import com.opshub.eai.metadata.LinkTypeMeta;
import com.opshub.eai.metadata.UserMeta;
import com.opshub.eai.metadata.interfaces.HasFieldsMeta;
import com.opshub.exceptions.ORMException;
import com.opshub.exceptions.eai.EAIJiraAdapterException;
import com.opshub.logging.OpsHubLoggingUtil;
import com.opshub.proxy.OpsHubAuthenticator;
import com.opshub.utils.hibernate.InstanceSessionFactory;


/**
 * This class gives implementation for all required adapter methods of OIMBaseAdapter interface.
 * This class is called through base implementation , when any write operation needs to be done in
 * Jira System through OpsHub Sync.
 */
public class JiraAdapter extends OIMConnector implements HasUserLookup,HasFieldsMeta,RulesVerifyInterface,ILinkSupport,IAttachmentProcessor,HasLinkSupport,HasStringDate,HasReverseLinkSupport {

	private static final String CHANGE_STATUS = "changeStatus";
	private static final String _0030 = "0030";
	private static final String _0050 = "0050";
	private static final String _0035 = "0035";

	private final static Logger LOGGER = Logger.getLogger (JiraAdapter.class);

	private final static Integer STEP_ID_FOR_CREATE_STEP1 = 1;
	private final static Integer STEP_ID_FOR_STATUS = 2;
	private final static Integer STEP_ID_FOR_UPDATE_STEP1 = 3;
	private final static Integer STEP_ID_FOR_TIMEESTIMATE = 4;
	private final static Integer STEP_ID_FOR_LABELS = 5;
	private final Map<String, String> ISSUE_TYPE_ID_NAME_MAPPING = new HashMap<String, String>();

	private String jiraUserName = null;
	private String jiraUserPassword = null;
	private String jiraVersion = null;
	private String workflow = null;

	private JiraWebServiceClient wsClient = null;
	private InstanceSessionFactory jiraSessionFactory = null;
	private JiraRestRequestor jiraRestRequestor = null;
	
	private int loginCount = 0;
	private boolean hasDbAccess;

	private String jiraWebServiceURL;
	private String jiraProjectIds;
	
	private String commentInterval;

	public JiraAdapter(final String jiraUserName, final String jiraPassword, final String jiraWebServiceURL,
			final String workflow, final InstanceSessionFactory jiraSessionFactory, final Integer jiraSystem,
			final Integer integrationId, final String jiraProjectID, final String commentInterval)
			throws OIMAdapterException {
		super(integrationId, jiraUserName, null, true, jiraSystem);
		try {
			this.jiraUserName = jiraUserName;
			this.jiraUserPassword = jiraPassword;
			this.wsClient = new JiraWebServiceClient(jiraWebServiceURL,this.jiraUserName,jiraPassword);
			this.jiraSessionFactory = jiraSessionFactory;
			this.workflow = workflow;
			this.jiraProjectIds = jiraProjectID;
			
			if(jiraSessionFactory==null){
				// If database access is not available
				this.hasDbAccess = false;
			}
			else{
				//If database access is available
				this.hasDbAccess = true;
			}
			
			this.jiraWebServiceURL = jiraWebServiceURL;

			this.jiraWebServiceURL = jiraWebServiceURL;

			this.jiraWebServiceURL = jiraWebServiceURL;

			this.jiraWebServiceURL = jiraWebServiceURL;

			this.commentInterval = commentInterval;
			JiraThreadLocal.initThreadLocal();

			this.commentInterval = commentInterval;
			JiraThreadLocal.initThreadLocal();


			this.commentInterval = commentInterval;
			JiraThreadLocal.initThreadLocal();

			this.commentInterval = commentInterval;
			JiraThreadLocal.initThreadLocal();

			this.commentInterval = commentInterval;
			JiraThreadLocal.initThreadLocal();
			
		}catch(OIMJiraApiException e) {
			OpsHubLoggingUtil.error(LOGGER,"Failed to initialize Jira Web Service Client because " + e.getMessage(),e);
			throw e;
		}
	}

	private JiraRestRequestor getJiraRestRequestor() throws OIMJiraApiException{
		if(jiraRestRequestor ==null){
			this.jiraRestRequestor =  new JiraRestRequestor(getJiraVersion(), wsClient.getServerInfo().getBaseUrl(), jiraUserName, jiraUserPassword);
		}
		return this.jiraRestRequestor;			
	}
	
	
	public String getJiraVersion() throws OIMJiraApiException {
		if(jiraVersion==null){
			try{
				login();
				RemoteServerInfo serverInfo = wsClient.getServerInfo();
				jiraVersion = serverInfo.getVersion();
			}finally{
				logout();
			}
		}
		return jiraVersion;
	}

	/**
	 * Jira login service returns a token which is required to call any other jira service.
	 * login method is implicitly called when we create object of JiraAdapter and token is set.
	 * Now if we call logout ,then token is invalidated and need to create jiraAdpter object again if required in further scope
	 * of program. So instead of creating jiraAdapter again we can just call this login method. It will reset the token.  
	 * @throws OIMJiraApiException 
	 */
	public void login() throws OIMJiraApiException 
	{
		if(loginCount<=0){
			wsClient.login();
		}
		loginCount++;
	}

	/**
	 * User needs to call this method every time its done using instance of this class.
	 * @throws OIMJiraApiException 
	 * @throws EAIJiraAdapterException
	 */
	public void logout()
	{		
		if(loginCount==1){
			try {
				wsClient.logout();
			} catch (OIMJiraApiException e) {
				OpsHubLoggingUtil.error(LOGGER, "Error while calling logout", e);
			}
		}
		loginCount--;
	}

	@Override
	protected OIMEntityObject createEntity(final String externalId, Map<String, Object> systemProperties,
			Map<String, Object> customProperties, final boolean isRetry)	throws OIMAdapterException {

		try{
			login();
			RemoteIssue issue = new RemoteIssue();
			String issueKey = null;

			if(systemProperties==null)
				systemProperties = new HashMap<String, Object>();
			if(customProperties==null)
				customProperties = new HashMap<String, Object>();

			OIMEntityObject oimEntityObject;
			String notAvailableSystemFields = JiraUtility.isSystemFieldsAvailable(systemProperties);
			//Check Whether All Custom Fields which are sent for update is available or not.
			//Throw exception in case of wrong field name.
			if (notAvailableSystemFields != null) {
				OpsHubLoggingUtil.error(LOGGER, "JIRA does not have field(s) : " + notAvailableSystemFields, null);
				throw new OIMJiraApiException(_0035, new String[] { notAvailableSystemFields }, null);
			}
			String projectName = (String) (systemProperties.containsKey(JiraConstants.SystemField.PROJECTNAME) ? systemProperties.get(JiraConstants.SystemField.PROJECTNAME) : null);
			String issueType = (String) (systemProperties.containsKey(JiraConstants.SystemField.ISSUETYPE) ? systemProperties.get(JiraConstants.SystemField.ISSUETYPE) : null);
			String projectKey  = JiraUtility.getProjectKeyByName(wsClient,projectName);
			if(projectKey==null || projectKey.isEmpty()){
				throw new OIMJiraAdapterException("0047",new String[]{"Project Name",projectName},null);
			}
			if(issueType== null || issueType.isEmpty()){
				throw new OIMJiraAdapterException("0047",new String[]{"Issue Type",issueType},null);
			}
			
			//as the created by is not there in jira
			if (this.isHistoryBasedRecovery()) {
				String constructOhLastUpdateValue =  this.getAdapterIntegration().getIntegrationId()+"=";
				customProperties.put(this.getOHLastUpdateFieldName(), constructOhLastUpdateValue);
			}
			
			// For JIRA above 5.0, we will fetch custom fields metadata from REST else we will fetch it from web service client.
			HashMap<String, String> customFieldsIdName = getJiraRestRequestor().getCustomFieldsIDNamePairForProject(projectKey, issueType);
			if(customFieldsIdName == null)
			{
				customFieldsIdName = JiraUtility.getCustomFieldIdsByNames(wsClient, customProperties.keySet());
			}
			//initialize remote issue instance with system fields
			issue = JiraUtility.setSystemFields(issue,systemProperties,wsClient);
			validateMandatoryIssueFields(issue);

			issue.setCustomFieldValues(getCustomFields(customProperties,customFieldsIdName));

			RemoteIssue createdIssue = wsClient.createIssue(issue);
			issueKey = createdIssue.getKey();
			OpsHubLoggingUtil.debug(LOGGER,"Issue successfully created : " + createdIssue.getKey(),null);
			oimEntityObject = getOIMEntityObjectForRemoteIssue(createdIssue, true,true);
			OpsHubLoggingUtil.debug(LOGGER,"Issue successfully created : " + issueKey,null);

			return oimEntityObject;
		} finally{
			logout();
		}
	}
	
	@Override
	protected SubStepsDetailsCarrier getSubStepForCreate(
			final Map<String, Object> systemProperties,
			final Map<String, Object> customProperties,
			final Map<String, Object> oldSystemProperties,
			final Map<String, Object> oldCustomProperties) {
		String[] removeProperties = new String[]{JiraConstants.SystemField.ORIGINAL_ESTIMATE,JiraConstants.SystemField.REMAINING_ESTIMATE};
		Map<String,Object> createSystemProperties = EaiUtility.getNewMapWithoutProperties(systemProperties, removeProperties);
		Map<String,Object> createCustomProperties = EaiUtility.getNewMapWithoutProperties(customProperties, removeProperties);
		Map<String,Object> createOldSystemProperties = EaiUtility.getNewMapWithoutProperties(oldSystemProperties, removeProperties);
		Map<String,Object> createOldCustomProperties = EaiUtility.getNewMapWithoutProperties(oldCustomProperties, removeProperties);
		SubStepsDetailsCarrier createCarrier = new SubStepsDetailsCarrier(Integer.valueOf(OIMConnector.CREATE_STEP),STEP_ID_FOR_CREATE_STEP1, createSystemProperties, createCustomProperties, createOldSystemProperties, createOldCustomProperties, false);
		return createCarrier;
	}
	
	@Override
	protected List<SubStepsDetailsCarrier> getSubStepsForUpdate(
			final String operationId, final Map<String, Object> systemProperties,
			final Map<String, Object> customProperties,
			final Map<String, Object> oldSystemProperties,
			final Map<String, Object> oldCustomProperties) {
		Integer opIdInt = Integer.valueOf(operationId);
		List<SubStepsDetailsCarrier> list;
		//this needs to be done in particular sequence
		Map<String, Object> sysProp;
		Map<String, Object> custProp;
		Map<String, Object> oldSysProp;
		Map<String, Object> oldCustProp;
		
		if(OIMConnector.CREATE_STEP.equals(operationId)){
			//this is for update steps after create operation
			list =  new ArrayList<SubStepsDetailsCarrier>();

			//time is 3

			sysProp =  EaiUtility.getNewMapForProperties(systemProperties, STATUS_PROPERTIES);
			oldSysProp = EaiUtility.getNewMapForProperties(oldSystemProperties, STATUS_PROPERTIES);
			if(sysProp!=null && !sysProp.isEmpty()){				
				SubStepsDetailsCarrier statusCarrier = new SubStepsDetailsCarrier(opIdInt, STEP_ID_FOR_STATUS, sysProp, null, oldSysProp, null,false);
				list.add(statusCarrier);
			}	
			sysProp =  EaiUtility.getNewMapForProperties(systemProperties, TIME_PROPERTIES);
			oldSysProp = EaiUtility.getNewMapForProperties(oldSystemProperties, TIME_PROPERTIES);
			if(sysProp!=null && !sysProp.isEmpty()){
				SubStepsDetailsCarrier timeCarrier = new SubStepsDetailsCarrier(opIdInt, STEP_ID_FOR_TIMEESTIMATE, sysProp, null, oldSysProp, null,false);
				list.add(timeCarrier);
			}
			sysProp =  EaiUtility.getNewMapForProperties(systemProperties, LABELS_PROPERTIES);
			oldSysProp = EaiUtility.getNewMapForProperties(oldSystemProperties, LABELS_PROPERTIES);
			if(sysProp!=null && !sysProp.isEmpty()){
				SubStepsDetailsCarrier labelsCarrier = new SubStepsDetailsCarrier(opIdInt, STEP_ID_FOR_LABELS, sysProp, null, oldSysProp, null,false);
				list.add(labelsCarrier);
			}



		}else{
			//this is for multiple update steps in update operation
			list=  new ArrayList<SubStepsDetailsCarrier>();
			ArrayList<String> removePropertiesList= new ArrayList<String>();
			removePropertiesList.addAll(Arrays.asList(STATUS_PROPERTIES));
			removePropertiesList.addAll(Arrays.asList(TIME_PROPERTIES));
			removePropertiesList.addAll(Arrays.asList(LABELS_PROPERTIES));

			//this is first step for update
			
			//status is 1
			
			sysProp =  EaiUtility.getNewMapForProperties(systemProperties, STATUS_PROPERTIES);
			oldSysProp = EaiUtility.getNewMapForProperties(oldSystemProperties, STATUS_PROPERTIES);
			//Change Status will throw an exception if transition is not found.
			if(sysProp!=null && !sysProp.isEmpty()){
				SubStepsDetailsCarrier statusCarrier = new SubStepsDetailsCarrier(opIdInt, STEP_ID_FOR_STATUS, sysProp, null, oldSysProp, null,false);
				list.add(statusCarrier);
			}

			
			//this is base update step 2
			sysProp =  EaiUtility.getNewMapWithoutProperties(systemProperties, removePropertiesList.toArray(new String[removePropertiesList.size()]));
			custProp =  EaiUtility.getNewMapWithoutProperties(customProperties, removePropertiesList.toArray(new String[removePropertiesList.size()]));
			oldSysProp =  EaiUtility.getNewMapWithoutProperties(oldSystemProperties, removePropertiesList.toArray(new String[removePropertiesList.size()]));
			oldCustProp =  EaiUtility.getNewMapWithoutProperties(oldCustomProperties, removePropertiesList.toArray(new String[removePropertiesList.size()]));
			//if its part of update then have to have pass one step as it may be for update oh last update then custom system propertie will be null
			SubStepsDetailsCarrier updateCarrier = new SubStepsDetailsCarrier(opIdInt, STEP_ID_FOR_UPDATE_STEP1, sysProp, custProp, oldSysProp, oldCustProp,false);
			list.add(updateCarrier);
			
			
			//time is 3
			sysProp =  EaiUtility.getNewMapForProperties(systemProperties, TIME_PROPERTIES);
			oldSysProp = EaiUtility.getNewMapForProperties(oldSystemProperties, TIME_PROPERTIES);
			if(sysProp!=null && !sysProp.isEmpty()){
				SubStepsDetailsCarrier timeCarrier = new SubStepsDetailsCarrier(opIdInt, STEP_ID_FOR_TIMEESTIMATE, sysProp, null, oldSysProp, null,false);
				list.add(timeCarrier);
			}
			
			//Labels are step 4
			sysProp =  EaiUtility.getNewMapForPropertiesWithValueNull(systemProperties, LABELS_PROPERTIES);
			oldSysProp = EaiUtility.getNewMapForProperties(oldSystemProperties, LABELS_PROPERTIES);
			if(sysProp!=null && !sysProp.isEmpty()){
				SubStepsDetailsCarrier labelsCarrier = new SubStepsDetailsCarrier(opIdInt, STEP_ID_FOR_LABELS, sysProp, null, oldSysProp, null,false);
				list.add(labelsCarrier);
			}
		}
		return list;
	}
	
	

	private static final String[] TIME_PROPERTIES = new String[]{JiraConstants.SystemField.ORIGINAL_ESTIMATE,JiraConstants.SystemField.REMAINING_ESTIMATE};
	private static final String[] LABELS_PROPERTIES = new String[]{JiraConstants.SystemField.LABELS};
	private static final String[] STATUS_PROPERTIES = new String[]{JiraConstants.SystemField.STATUS,JiraConstants.SystemField.RESOLUTION};
	
	
	@Override
	protected OIMEntityObject updateEntity(final String internalId, final Object entityObject,
			final Map<String, Object> systemProperties,
			Map<String, Object> customProperties, final String externalIdValue,
			final boolean isRetry, final String operationId, final Integer subStep) throws OIMAdapterException {
		if(operationId!= null && operationId.startsWith(CREATE_STEP)){
			// temporary fix of issue
			// issue: if creation time = updation time and process stops after update but before updating oh_last_update then system recovery is not detected by base adapter
			try {
				Thread.sleep(1000); // as jira stores time till millisecond only keep break of 1 second
			} catch (InterruptedException e) {
				throw new OIMJiraAdapterException("0108", new String[]{String.valueOf(subStep),"Update"}, e);
			}
		}
		try{
			login();
			if(customProperties==null)
				customProperties = new HashMap<String, Object>();
			String notAvailableSystemFields = JiraUtility.isSystemFieldsAvailable(systemProperties);
			//Check Whether All Custom Fields which are sent for update is available or not.
			//Throw exception in case of wrong field name.
			String notAvailableCustomFields = JiraUtility.isCustomFieldsAvailable(wsClient, customProperties);
			
			if(notAvailableSystemFields!=null || notAvailableCustomFields!=null){
				String error = notAvailableSystemFields==null ? notAvailableCustomFields : 
					notAvailableCustomFields==null ? notAvailableSystemFields : (notAvailableCustomFields + "," + notAvailableSystemFields);
				throw new OIMJiraApiException(_0035,new String[]{error},null);
			}
			RemoteIssue updatedIssue;
			if(STEP_ID_FOR_UPDATE_STEP1.equals(subStep) ){
				//this is the first update operation

				//update issue
				updatedIssue = updateIssue(internalId, systemProperties, customProperties);
			}else if(STEP_ID_FOR_TIMEESTIMATE.equals(subStep)){
				//Change Status will throw an exception if transition is not found.
				String lastUpdateValueForTimeEst = (String)customProperties.get(this.getOHLastUpdateFieldName());
				//this is the for the time estimate update
				getJiraRestRequestor().addTimeEstimate(systemProperties,internalId,
						JiraUtility.getCustomFieldIdByName(wsClient,this.getOHLastUpdateFieldName()),lastUpdateValueForTimeEst);
				updatedIssue = (RemoteIssue)entityObject;
			}else if(STEP_ID_FOR_LABELS.equals(subStep)){
				//Change Status will throw an exception if transition is not found.
				String lastUpdateValueForTimeEst = (String)customProperties.get(this.getOHLastUpdateFieldName());
				//this is the for the time estimate update
				getJiraRestRequestor().addJiraLabels(systemProperties,internalId,
						JiraUtility.getCustomFieldIdByName(wsClient,this.getOHLastUpdateFieldName()),lastUpdateValueForTimeEst);
				updatedIssue = (RemoteIssue)entityObject;
			}else if(STEP_ID_FOR_STATUS.equals(subStep)){
				//Change Status will throw an exception if transition is not found.
				String lastUpdateValueForStatus = (String)customProperties.get(this.getOHLastUpdateFieldName());
				//this is the for the time estimate update
				changeStatus(internalId,lastUpdateValueForStatus,systemProperties);
				updatedIssue = (RemoteIssue)entityObject;
			}else{
				throw new OIMJiraAdapterException("0108", new String[]{String.valueOf(subStep),"Update"}, null);
			}
			OIMEntityObject oimEntityObject = getOIMEntityObjectForRemoteIssue(updatedIssue, false, true);
			return oimEntityObject;
		}finally{
			logout();
		}
	}

	@Override
	public OIMEntityObject addComment(final String internalId, final String commentTitle,
 final String commentBody, final boolean isretry, final boolean isrecovery,
			final CommentSettings commentSettings) throws OIMJiraAdapterException {
		try {
			login();
			if(internalId==null){
				OpsHubLoggingUtil.error(LOGGER, "Comments can not be added to issueKey :"+internalId, null);
				throw new OIMJiraAdapterException("0044", new String[]{internalId}, null);
			}

			RemoteComment rcomment= new RemoteComment();
			rcomment.setBody(commentBody);

			wsClient.addComment(internalId, rcomment);
			OpsHubLoggingUtil.debug(LOGGER, "Successfully added Comments to Issue :" + internalId, null);
			return OIMEntityObject.TRUE;
		}finally{
			logout();
		}
	}

	private OIMEntityObject getOIMEntityObjectForRemoteIssue(final RemoteIssue updatedIssue, final boolean isCreate, final boolean isUpdated) throws OIMAdapterException{
		OIMEntityObject oimEntityObject;
		String issueTypeName = getIssueTypeName(updatedIssue.getType());
		List<EAIKeyValue> currentState= null;
		if(!hasDbAccess){
				Map<String, Object> currentSystemProperties = getSystemProperties(updatedIssue.getKey(), updatedIssue);
				if(currentSystemProperties!=null)
					currentSystemProperties.remove(Constants.OH_ATTACHMENT);
				Map<String, Object> currentCustomProperties = getCustomProperties(updatedIssue.getKey(), updatedIssue);
				currentSystemProperties.putAll(currentCustomProperties);
				currentState =  EaiUtility.convertMapToEAIKeyValueList(currentSystemProperties);
		}
		Calendar time = isCreate?updatedIssue.getCreated():updatedIssue.getUpdated();
		if(hasDbAccess){
			RemoteServerInfo remoteServerInfo = wsClient.getServerInfo();
			String timeZoneId = remoteServerInfo.getServerTime().getTimeZoneId();
			try {
				time = JiraUtility.getTimeInOtherTimeZone(OIMCoreUtility.convertCalendarToTimestamp(time),timeZoneId);
			} catch (ParseException e) {
				throw new OIMJiraAdapterException("0023",new String[]{"getting time"},e);
			}
		}
		oimEntityObject = new OIMEntityObject(updatedIssue.getKey(), issueTypeName, updatedIssue.getProject(), currentState, time, isUpdated);
		return oimEntityObject; 
		
	}
	@Override
	public List<RemoteIssue> executeQuery(final String query) throws OIMJiraAdapterException {
		try
		{
			login();
			RemoteIssue[]  remoteIssues = wsClient.getIssuesFromJqlSearch(query, 10);
			return JiraUtility.convertToList(remoteIssues);
		}finally{
			logout();
		}
	}

	@Override
	public HashMap<String, Object> getCustomProperties(final String internalId,
			final Object entityObject) throws OIMAdapterException {
		try{
			login();
			HashMap<String,Object> customProperties = JiraUtility.getCustomProperties(wsClient, (RemoteIssue)entityObject);
			Map<String, FieldsMeta> fieldsMap =  EaiUtility.getMapOfFieldsMeta(getFieldsMetadata());
			for(String key:customProperties.keySet()){
				FieldsMeta fm = fieldsMap.get(key);
				if(fm==null || !fm.isMultiSelect()){
					String[] values = (String[])customProperties.get(key);
					if(values.length > 0){
						customProperties.put(key, values[0]);
					}else{
						customProperties.put(key, null);
					}
				}
			}
			return customProperties;
		}
		catch(OIMJiraApiException e)
		{
			OpsHubLoggingUtil.error(LOGGER, "Error occurred while calling web service from method : getCustomProperties", e);
			throw new OIMJiraAdapterException("0023",new String[]{"getCustomProperties"},e);
		}finally{
			logout();
		}
	}

	@Override
	public RemoteIssue getEntityObject(final String internalId) throws OIMJiraAdapterException {

		try{
			login();

			RemoteIssue remoteIssue = null;   
			try {
				if(internalId != null)
					remoteIssue = wsClient.getIssueByKey(internalId);
			}catch (OIMJiraPermissionException e){
				OpsHubLoggingUtil.debug(LOGGER,"Issue "+internalId +" does not exist",e);
				return null;
			}
			catch (OIMJiraApiException e) {
				OpsHubLoggingUtil.error(LOGGER, "Error occurred while calling web service from method : getIssue", e);
				throw new OIMJiraAdapterException("0023",new String[]{"getIssue"},e);
			}
			return remoteIssue;	
		}catch (OIMJiraApiException e) {
			OpsHubLoggingUtil.debug(LOGGER,"Login failed. Invalid user name or password.",e);
			throw e;
		}finally{
			logout();
		}
	}

	@Override
	public List<HashMap<String,Object>> getHistorySince(final Calendar afterTime, final Calendar maxTime,
			final String lastProcessedId, final String lastProcessedRevisionId,
			final String entityName, final String notUpdatedByUser, final Integer pageSize,
			final List<String> fields, final boolean isCaseSensititve)
			throws OIMAdapterException {
		try {
			login();
			RemoteServerInfo remoteServerInfo = wsClient.getServerInfo();
			String timeZoneId = remoteServerInfo.getServerTime().getTimeZoneId();
			List<String> issueTypesToBePolled = new ArrayList<String>();
			issueTypesToBePolled.add(entityName);
			String projectIds = "";
			return getHistorySince(OIMCoreUtility.convertCalendarToTimestamp(afterTime), OIMCoreUtility.convertCalendarToTimestamp(maxTime),lastProcessedId,lastProcessedRevisionId,issueTypesToBePolled, notUpdatedByUser,
					pageSize, fields, isCaseSensititve,projectIds,timeZoneId);
		}finally{
			logout();
		}
	}

	@Override
	public String getPropertyValueAsString(final String internalId, final String propertyName,
			final boolean isSystemProperty) throws OIMAdapterException {
		Object obj = getPropertyValue(internalId, propertyName, isSystemProperty);
		String str = obj==null?null:String.valueOf(obj);
		return str;
	}
	
	@Override
	public Object getPropertyValue(final String internalId, final String propertyName,
			final boolean isSystemProperty) throws OIMAdapterException {

		RemoteIssue remoteIssue = getEntityObject(internalId);
		if(remoteIssue==null)
		{
			OpsHubLoggingUtil.error(LOGGER, "Property Value can not be retireved for property-"+propertyName + " as entity object not found with issue key "+internalId,null);
			throw new OIMJiraAdapterException("0051", new String[]{propertyName,internalId}, null);
		}

		HashMap<String, Object> properties = null; 
		Object propertyValue = null;

		try
		{
			wsClient.login();

			if(isSystemProperty)
				propertyValue = JiraUtility.getSystemProperty(wsClient,propertyName,remoteIssue,true);
			else {
				properties = getCustomProperties(internalId, remoteIssue);
				// if properties map is null then return
				if(properties == null)
					return "";

				// get value for property
				propertyValue = properties.get(propertyName);
			}

			// if value is null then return empty string
			if(propertyValue == null)
				return "";
			else if(propertyValue instanceof String) // if is instance of string then return string
				return propertyValue.toString();
			else if(propertyValue instanceof String[])// if is instance of string[] then return string separated by comma
			{
				String commaSepStr = "";
				String[] valueStringArray = (String[]) propertyValue;
				// iterate on string[]
				for(int valueIndex=0;valueIndex<valueStringArray.length;valueIndex++)
				{
					// if string is not empty means there already exists some value and append comma to separate different values
					if(!commaSepStr.trim().isEmpty())
						commaSepStr += ",";
					// append value
					commaSepStr += valueStringArray[valueIndex];
				}
				// return comma separated string
				return commaSepStr;
			}
			else if(propertyValue instanceof Calendar)
				// if is instance of calendar then return string representation of calendar
				return ((Calendar)propertyValue).getTime().toString();
			else // else return toString
				return propertyValue;

		}finally{
			wsClient.logout();
		}
	}

	@Override
	public HashMap<String, Object> getSystemProperties(final String internalId,
			final Object entityObject) throws OIMJiraAdapterException {

		RemoteIssue issueObj = (RemoteIssue) entityObject;

		try {
			login();
			HashMap<String,Object> currentSysValues = JiraUtility.getSystemProperties(wsClient, issueObj,true);
			List<EAIKeyValue> eaiTimeTrackingRefrence = getJiraRestRequestor().getTimeTarckingInformation(issueObj.getKey());
			if(eaiTimeTrackingRefrence!=null && !eaiTimeTrackingRefrence.isEmpty()){	
				for(EAIKeyValue timeTracking: eaiTimeTrackingRefrence){
					currentSysValues.put(timeTracking.getKey(),timeTracking.getValue());
				}
			}
			// Put the labels if any into the system properties
			String[] jiraLabels = getJiraRestRequestor().getJiraLabelsOnAnIssue(issueObj.getKey());
			currentSysValues.put(JiraConstants.SystemField.LABELS, jiraLabels);
			return currentSysValues;
		} catch (OIMJiraApiException e) {
			OpsHubLoggingUtil.error(LOGGER, "Error occurred while calling web service from method : getSystemProperties", e);
			throw new OIMJiraAdapterException("0023",new String[]{"getSystemProperties"},e);
		}finally{
			logout();
		}
	}

	@Override
	public void cleanup() {
		if(hasDbAccess){
			Session jiraSession;
			try {
				jiraSession = jiraSessionFactory.openSession();
				if(jiraSession!=null && jiraSession.isOpen())
					jiraSessionFactory.cleanup(jiraSession, null);
				OpsHubLoggingUtil.debug(LOGGER, "Jira Session Cleaned UP", null);
			} catch (ORMException e) {
				OpsHubLoggingUtil.error(LOGGER, "Error in getting Jira Session for cleanup", e);
			}
		}
		JiraThreadLocal.cleanupThreadLocal();
	}
	
	/**
	 * 
	 * @param afterTime
	 * @param maxTime
	 * @param lastProcessedId
	 * @param issueTypes
	 * @param notUpdatedByUser
	 * @param syncFieldName
	 * @param syncFieldValue
	 * @param pageSize
	 * @param fields
	 * @param isCaseSensititve
	 * @param projectIds
	 * @return
	 * @throws OIMAdapterException
	 */
	public List<HashMap<String,Object>> getHistorySince(final Timestamp afterTime, final Timestamp maxTime,
			final String lastProcessedId,final String lastProcessedRevisionId, final List<String> issueTypes, final String notUpdatedByUser,
			final Integer pageSize, final List<String> fields, final boolean isCaseSensititve,final String projectIds,final String timeZoneId)
	throws OIMJiraAdapterException {

		Session jiraSession = null;
		List<HashMap<String,Object>> listOfEvents = new ArrayList<HashMap<String,Object>>();
		try
		{
			login();

			MergeEvents mergeList = null;

			//Get comma separated list of project keys to be polled.
			String projectKeyToBePolled = JiraUtility.getCommaSepratedProjectKeys(wsClient, projectIds);
			//Get comma separated list of issue type names to be polled.
			String issueTypesString = EaiUtility.getCommaSeperatedListValues(issueTypes,true);
			
			//Get comma separated list of Issue Type internal id to be polled.
			//It will be used only in case of db access but kept at common level because it validates issue types 
			String issueTypeIdsString = JiraUtility.getCommaSepratedIssueTypeIds(wsClient, issueTypes);

			//Get Last Processed Issue Key
			String lastProcessedIssueKey = null;
			if(lastProcessedId!=null && !lastProcessedId.equals("-1")) {
				RemoteIssue lastProcessedIssue = wsClient.getIssueById(lastProcessedId);
				lastProcessedIssueKey = lastProcessedIssue==null ? null : lastProcessedIssue.getKey();
			}

			if(hasDbAccess)
			{// If database access is available, events will be returned as per history.
				
				try {
					jiraSession = jiraSessionFactory.openSession();
				} catch (ORMException e) {
					throw new OIMJiraAdapterException(_0050,null,e);
				}
				
				mergeList = getEventsForAdvanceConnector(jiraSession,afterTime, maxTime, lastProcessedId, 
						lastProcessedIssueKey,lastProcessedRevisionId,
						issueTypeIdsString, issueTypesString, projectKeyToBePolled, projectIds, 
						notUpdatedByUser, pageSize);
			}
			else 
			{//If database access is not available, events will be as per current state.
				
				mergeList = getEventsForSimpleConnector(afterTime, maxTime, lastProcessedIssueKey, 
						issueTypesString, projectKeyToBePolled, pageSize,timeZoneId);
			}

			//Generate List From Iterator
			while(mergeList.hasNext()){
				listOfEvents.add(mergeList.next());
			}

		}finally{
			logout();
		}
		return listOfEvents;
	}

	/**
	 * This method returns parent issue id of given sub task issue key. If no parent found, it gives null.
	 * Usable only if jira database access is available
	 * @param subTaskIssueKey - Issue Key whose parent is desired
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	public String getParentIssueKeyOfSubTask(final String subTaskIssueKey) throws OIMJiraAdapterException
	{
		//Verify Sub-Task Issue Key exists or not.
		if(subTaskIssueKey==null || subTaskIssueKey.equals("-1")){
			OpsHubLoggingUtil.error(LOGGER, "Parent can not be found for issueKey :"+subTaskIssueKey, null);
			throw new OIMJiraAdapterException("0044", new String[]{subTaskIssueKey}, null);
		}
		
		//Check for version greater or equal 4.2
		boolean versionGTOrEQ4point2 = JiraUtility.isVersionEqualOrAbove("4.2", getJiraVersion());
		String parentId = null;
		if(versionGTOrEQ4point2){
			try {
				parentId =  getJiraRestRequestor().getParentIssueKeyForSubTask(subTaskIssueKey);
			} catch (OIMJiraApiException e) {
				throw new OIMJiraAdapterException("0023",new String[]{"getIssue"},e);
			}
		}else if(hasDbAccess){
			RemoteIssue remoteIssue;
			Session jiraSession = null;
			try {
				login();
				remoteIssue = wsClient.getIssueByKey(subTaskIssueKey);
				jiraSession =  jiraSessionFactory.openSession();
				JiraDbAccessClient dbAccessClient = new JiraDbAccessClient(jiraSession);	
				Integer parentIssueid = dbAccessClient.getParentIssueIdForSubTask(remoteIssue.getId(), remoteIssue.getType());

				//get key of parent issue
				if(parentIssueid!=null){
					RemoteIssue parentIssue = wsClient.getIssueById(String.valueOf(parentIssueid));
					parentId = parentIssue.getKey();
				}
			} catch (OIMJiraApiException e) {
				throw new OIMJiraAdapterException("0023",new String[]{"getIssue"},e);
			} catch (ORMException e) {
				throw new OIMJiraAdapterException("0055",new String[]{"getParentIssueKeyOfSubTask"},e);
			} catch (OIMJiraDBAccessException e) {
				throw new OIMJiraAdapterException(JIRARESTConstants.JiraErrorCode.ERROR_0312,null,e);
			}finally{
				logout();
			}
		}
		else {
			//If Jira Version is below 4.2 and no database access is available. Can not support fetching Parent Id of Sub-Task.
			OpsHubLoggingUtil.error(LOGGER, "Get-Parent-Issue-Key-Of-SubTask method is not supported by Simple Connector of Jira for version below 4.2. Database Configuration is required.", null);
		}
		return parentId;
	}

	/**
	 * 
	 * @param propertyName
	 * @param userName
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	public String getPropertyFromUserName(final String propertyName, final String userName) throws OIMJiraAdapterException {
		String propertyValue = "";
		try{
			login();
			if(propertyName==null || propertyName.trim().equals("") 
					|| !(propertyName.equals(JiraConstants.USER_EMAIL_KEY) || propertyName.equals(JiraConstants.USER_FULLNAME_KEY))){
				throw new OIMJiraAdapterException("0063", new String[]{"propertyName",propertyName}, null); 
			}
			if(userName==null || userName.trim().equals(""))
				return propertyValue;
			RemoteUser remoteUser = wsClient.getUser(userName);
			if(remoteUser!=null){
				if(propertyName.equals(JiraConstants.USER_EMAIL_KEY))
					propertyValue = remoteUser.getEmail();
				else if(propertyName.equals(JiraConstants.USER_FULLNAME_KEY))
					propertyValue = remoteUser.getFullname();
			}
			return propertyValue;
		}finally{
			logout();
		}
	}

	/**
	 * 
	 * @param propertyName
	 * @param propertyValue
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	public String[] getUserNameFromProperty(final String propertyName, final String propertyValue,final boolean caseSensitive) throws OIMJiraAdapterException {

		if(!hasDbAccess)
			throw new OIMJiraAdapterException("0082", new String[]{"Get-UserName-From-Property"}, null);
		Session jiraSession = null;
		String[] userList = new String[]{};
		try {
			jiraSession = jiraSessionFactory.openSession();
			JiraDbAccessClient dbAccessClient = new JiraDbAccessClient(jiraSession);
			if(propertyName==null || propertyName.trim().equals("") 
					|| !(propertyName.equals(JiraConstants.USER_EMAIL_KEY) || propertyName.equals(JiraConstants.USER_FULLNAME_KEY))){
				throw new OIMJiraAdapterException("0063", new String[]{"propertyName",propertyName}, null); 
			}
			if(propertyValue==null || propertyValue.trim().equals("")){
				return userList;
			}

			if(JiraUtility.isVersionEqualOrAbove("4.3", getJiraVersion())){
				userList = dbAccessClient.getUserNamesFromProperty(jiraSessionFactory.getSchemaName(),propertyName,propertyValue,caseSensitive);
			}
			else{
				List<Long> userIds = dbAccessClient.getListOfUserIdsFromProperty(propertyName,propertyValue,caseSensitive);
				userList = dbAccessClient.getUserNamesFromUserIds(jiraSessionFactory.getSchemaName(),this.getJiraVersion(), userIds);
			}
		} catch (ORMException e) {
			OpsHubLoggingUtil.error(LOGGER, "Error in getting " + propertyName + " :: " + propertyValue, e);
			throw new OIMJiraAdapterException(_0050,null,e);
		}finally{
			if(jiraSession!=null)
				jiraSessionFactory.cleanup(jiraSession, null);
		}				
		return userList;
	}

	/**
	 * This method returns the status value of the given issue
	 * @param issueKey
	 * @return
	 * @throws OIMJiraApiException
	 */
	public String getStatus(final String issueKey) throws OIMJiraAdapterException {
		try
		{
			login();
			RemoteIssue issue = getEntityObject(issueKey);
			String status = null;
			if(issue != null)
			{
				status = JiraUtility.getStatusNameById(wsClient, issue.getStatus());
			}
			return status;
		}finally{
			logout();
		}
		
	}

	/**
	 * This method returns true if the given user exists else returns false
	 * @param nuser
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	public boolean userExists(final String nuser) throws OIMJiraAdapterException {
		boolean userExist = false;
		try {
			login();
			RemoteUser user = wsClient.getUser(nuser);
			if(user==null)
				userExist = false;
			else
				userExist = true;
		}finally{
			logout();
		}
		return userExist;
	}

	/**
	 * This method checks that project exists or not,if it does not exist then 
	 * project with same name will created with default schemes. 
	 * @param projectName
	 * @return
	 * @throws OIMJiraAdapterException 
	 */
	public String validateAndCreateProject(final String projectName,final String permissionScheme,final String notificationScheme,final String secutiryScheme ) throws OIMJiraAdapterException  {
		return validateAndCreateProject(projectName, permissionScheme,notificationScheme, secutiryScheme,true);
	}

	/**
	 * This method checks that project exists or not,if it does not exist then 
	 * project with same name will created with default schemes. 
	 * @param projectName
	 * @return
	 * @throws EAIJiraAdapterException
	 * @throws OIMJiraAdapterException 
	 */
	public String validateAndCreateProject(final String projectName,final String permissionScheme,final String notificationScheme,final String secutiryScheme,final boolean doCreate) throws OIMJiraAdapterException {

		try{
			login();

			String projectNameValue = projectName;
			String projectKey=null;
			if(!(projectNameValue == null)){

				//ProjectNameValue is converted into uppercase because it is used to create Jira Key which needs to be in uppercase without spaces.
				projectNameValue = projectNameValue.toUpperCase().replaceAll(" ","");
				projectKey = JiraUtility.getProjectKeyByName(wsClient,projectName);
				if(projectKey == null && doCreate){
					JiraUtility.createProject(wsClient,projectNameValue,jiraUserName, permissionScheme,notificationScheme,secutiryScheme);
				}else if(!doCreate && projectKey==null){
					throw new OIMJiraAdapterException("0026",new Object[]{projectName},null);
				}
			}

			return projectKey;
		}finally{
			logout();
		}
	}

	/**
	 * This method first validate that user exist in Jira or not.
	 * If user does not exist then it will be created.
	 * @param username
	 * @param domain
	 * @return
	 * @throws OIMJiraAdapterException 
	 * @throws OIMJiraApiException 
	 * @throws EAIJiraAdapterException
	 */
	public String validateAndCreateUser(final String username,final String domain,final String groupName) throws OIMJiraAdapterException
	{
		return validateAndCreateUser(username, domain, groupName, true);

	}

	/**
	 * 
	 * @param username
	 * @param domain
	 * @param groupName
	 * @param doCreate
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	public String validateAndCreateUser(String username,final String domain,final String groupName,final boolean doCreate) throws OIMJiraAdapterException 
	{
		try{
			login();
			if(username != null && !username.trim().equals("")){   
				//user name must be in lower case in jira.
				username = username.toLowerCase();
				RemoteUser user = wsClient.getUser(username);

				if(user!=null){
					username = user.getName();
				}
				else{
					if(doCreate){
						JiraUtility.createUser(wsClient,username,domain,groupName);
					}
					else{
						throw new OIMJiraAdapterException("0025",new String[]{username},null);
					}
				}
			}
			return username;
		}
		finally{
			logout();
		}
	}

	@Override
	public void addLink(final String internalId, final String linkType, final String targetIssueKey, final String entityType, final Map<String, String> properties)throws OIMAdapterException{
		try {
			login();
			getJiraRestRequestor().addLink(internalId, targetIssueKey, linkType, null);
		}finally{
			logout();
		}
	}

	@Override
	public EAIEntityRefrences getCurrentLinks(final String internalId,final Object entityObject, final String linkType, final Map<String, String> properties) throws OIMAdapterException{
		try {
			login();
			List<EAIEntityRefrences> eaiEntityRefrences = getJiraRestRequestor().getJiraIssueLinkInformation(internalId, this.wsClient);
			if(eaiEntityRefrences==null) return null;
			EAIEntityRefrences entityRefrence = null;
			for(int i=0;i<eaiEntityRefrences.size();i++){
				entityRefrence = eaiEntityRefrences.get(i);
				if(entityRefrence.getLinkType().equals(linkType)){
					return entityRefrence;
				}
			}
		}finally{
			logout();
		}
		return null;
	}
	
	/**
	 * 
	 * @param query
	 * @param limit
	 * @return
	 * @throws OIMAdapterException
	 */
	public List executeQuery(final String query,final int limit) throws OIMJiraAdapterException {
		try
		{
			login();
			RemoteIssue[]  remoteIssues = wsClient.getIssuesFromJqlSearch(query, limit);
			return JiraUtility.convertToList(remoteIssues);
		}finally{
			logout();
		}
	}

	/**
	 * Returns name list of all projects available
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	public String[] getAllProject() throws OIMJiraAdapterException{
		String[] projects = null;
		try{
			login();
			projects  = JiraUtility.getAllProjectNames(wsClient);
		}
		finally{
			logout();
		}
		return projects;
	}

	/**
	 * Gets all comments for given issue key
	 * @param internalId
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	public EAIComment[] getComments(final String internalId) throws OIMJiraAdapterException{
		try{
			login();
			if(internalId==null || internalId.equals("-1")){
				OpsHubLoggingUtil.error(LOGGER, "Comments can not be retrieved for issueKey :"+internalId, null);
				throw new OIMJiraAdapterException("0044", new String[]{internalId}, null);
			}
			RemoteIssue remoteIssue = getEntityObject(internalId);
			return JiraUtility.getComments(wsClient, remoteIssue);
		}catch (OIMJiraApiException e) {
			OpsHubLoggingUtil.error(LOGGER, "Remote Exception occured while adding comments to Issue :"+internalId, null);
			throw e;
		}finally{
			logout();
		}
	}



	/**
	 * This method is created for internal use. It should not be used from any external source.
	 * This method returns version id from version name.
	 * @param versionNames
	 * @param projectId
	 * @return
	 * @throws OIMJiraAdapterException 
	 */
	public String[] obsoleteGetVersionIdsByNames(final String[] versionNames,final String projectName) throws OIMJiraAdapterException {
		try{
			login();
			String projectKey = JiraUtility.getProjectKeyByName(wsClient, projectName);
			if(projectKey!=null){
				return JiraUtility.getVersionIdsByNames(wsClient, versionNames, projectKey);
			}else{
				throw new OIMJiraAdapterException("0026", new String[]{" Name : " + projectName}, null);
			}
		}finally{
			logout();
		}
	}
	
	/**
	 * This method is created for internal use. It should not be used from any external source.
	 * This method returns version id from version name.
	 * @param versionIds
	 * @param projectId
	 * @return
	 * @throws OIMJiraAdapterException 
	 */
	public String[] obsoleteGetVersionNamesByIds(final String[] versionIds,final String projectName) throws OIMJiraAdapterException {
		try{
			login();
			String projectKey = JiraUtility.getProjectKeyByName(wsClient, projectName);
			if(projectKey!=null){
				return JiraUtility.getVersionNamesByIds(wsClient, versionIds, projectKey);
			}else{
				throw new OIMJiraAdapterException("0026", new String[]{" Name : " + projectName}, null);
			}
		}finally{
			logout();
		}
	}
	
	/**
	 * This method is created for internal use. It should not be used from any external source.
	 * This method returns project id from project name.
	 * @param name
	 * @return
	 * @throws OIMJiraApiException
	 */
	public String[] obsoleteGetProjectIdByName(final String[] name) throws OIMJiraApiException{
		try{
			login();		
			return JiraUtility.getProjectIdsByName(wsClient, name);
		}finally{
			logout();
		}
	}
	
	/**
	 * This method will add comments to issue with ability to log results 
	 * @param issueKey
	 * @param comments
	 * @throws OIMJiraApiException 
	 */
	public void addComment(final String issueKey,final String comments,final String sourceUpdateDate,final int jobInstanceId,final boolean isRetry) throws OIMJiraAdapterException {
		String errorMessage = null;
		Map<String, ActionResult> failedActions = null;
		boolean addComment = true;
		boolean commentAdded = false;
		String addCommentStep= "add comment";
		try {
			login();
			
			if(issueKey==null || issueKey.equals("-1")){
				OpsHubLoggingUtil.error(LOGGER, "Comments can not be added to issueKey :"+issueKey, null);
				errorMessage="Issue does not exist in Jira with key :"+issueKey;
				throw new OIMJiraAdapterException("0044", new String[]{issueKey}, null);
			}
			failedActions = EAIRecoveryUtility.getActionResults(issueKey, jobInstanceId, sourceUpdateDate);
			if(isRetry)
			{
				int failedActionsSize = failedActions.size();
				if(failedActionsSize==0 || failedActionsSize>0 && failedActions.containsKey(addCommentStep))
					addComment = true;
				else
					addComment = false;
			}

			if(addComment)
			{
				RemoteComment rcomment= new RemoteComment();
				rcomment.setBody(comments);
				wsClient.addComment(issueKey, rcomment);
				OpsHubLoggingUtil.debug(LOGGER, "Successfuly added Comments to Issue :"+issueKey, null);
			}
			commentAdded = true;

		}catch (ORMException e) {
			throw new OIMJiraAdapterException(_0030,new Object[]{"addComment"},e);
		}catch (OIMJiraApiException e) {
			errorMessage = e.getMessage();
			throw e ;
		}finally{
			logOutAndLogOrUpdateResult(issueKey, sourceUpdateDate, jobInstanceId, errorMessage, failedActions,
					commentAdded, addCommentStep, "addComments");
		}
	}

	/**
	 * @param issueKey
	 * @param sourceUpdateDate
	 * @param jobInstanceId
	 * @param errorMessage
	 * @param failedActions
	 * @param isOperationCompleted
	 * @param addCommentStep
	 * @throws OIMJiraAdapterException
	 */
	private void logOutAndLogOrUpdateResult(final String issueKey, final String sourceUpdateDate,
			final int jobInstanceId, String errorMessage, final Map<String, ActionResult> failedActions, final boolean isOperationCompleted,
			final String step, final String operation ) throws OIMJiraAdapterException {
		logout();			
		if(errorMessage==null)
			errorMessage= "Unexpected error occurred.";
		boolean logged = false;
		try {
			if(failedActions!=null)
				logged = EAIRecoveryUtility.logOrUpdateResult(failedActions.get(step),issueKey, sourceUpdateDate,jobInstanceId, isOperationCompleted, step, errorMessage);
		} catch (ORMException e) {
			throw new OIMJiraAdapterException(_0030,new Object[]{operation},e);
		}
		if(!logged)
		{
			throw new OIMJiraAdapterException(_0030,new Object[]{operation},null);
		}
	}


	
	protected void removeFieldsFromConflicts(final HashMap<String, Object> conflictFields,final String subStepNumber) throws OIMAdapterException
	{
		String subStep = getSubStepExecutionNumber(subStepNumber);
		if(subStep!=null){
			Integer num = Integer.valueOf(subStep);
			if(num.equals(STEP_ID_FOR_STATUS)){
				if(conflictFields.containsKey(SYSTEMPROPERTIES)){
					HashMap sysProp = (HashMap)conflictFields.get(SYSTEMPROPERTIES);
					sysProp.remove(JiraConstants.SystemField.STATUS);
					sysProp.remove(JiraConstants.SystemField.RESOLUTION);
				}
			}
		}
	}
	
	/**
	 * This method checks for valid transition , and updates status. throws an exception if transition is not valid.
	 * @param issueKey
	 * @param transitionName
	 * @param fieldValues
	 * @throws OIMJiraApiException
	 */
	private void checkTransitionAndProgressWorkflow(final String issueKey, final String transitionName,
			final RemoteFieldValue[] fieldValues) throws OIMJiraApiException {

		String actionId = JiraUtility.isValidTransition(wsClient, issueKey, transitionName);
		if(actionId!=null){
			RemoteIssue remoteIssue = wsClient.progressWorkflowAction(issueKey, actionId, fieldValues);
			OpsHubLoggingUtil.debug(LOGGER, "Change Status Done Successfully.Progressed workflow of "+remoteIssue.getKey()+" to: " + remoteIssue.getStatus(), null);
		}
		else{
			OpsHubLoggingUtil.debug(LOGGER, "Status Transition to : "+transitionName+" is not available  for issue "+issueKey,null);
		}
	}

	/**
	 * 
	 * @param internalId
	 * @param lastUpdateValue
	 * @param systemProperties
	 * @throws OIMJiraAdapterException
	 */
	private void changeStatus(final String internalId, final String lastUpdateValue, final Map<String, Object> systemProperties) throws OIMJiraAdapterException {

		if(systemProperties==null)
			return;
		
		String transitionName = null;
		String status = (String)systemProperties.remove(JiraConstants.SystemField.STATUS);
		String resolution = (String)systemProperties.remove(JiraConstants.SystemField.RESOLUTION);
		RemoteIssue remoteIssue = getEntityObject(internalId);
		String currentStatus = JiraUtility.getStatusNameById(wsClient, remoteIssue.getStatus());

		if((status==null  && resolution!=null) || (currentStatus.equals(status) && resolution!=null)){
			try{
				login();
				RemoteFieldValue[] fieldValues= JiraUtility.getRemotFieldValue(wsClient, internalId,null,resolution,lastUpdateValue,this.getOHLastUpdateFieldName());
				wsClient.updateIssue(remoteIssue.getKey(), fieldValues);
				return;
			}finally{
				logout();
			}
		}else if(status==null || currentStatus.equals(status)){
			OpsHubLoggingUtil.debug(LOGGER, "Current Status and Updated Status are same. " + currentStatus, null);
			return;
		}
		//change status for simple connector where database is not available
		if(!hasDbAccess)
		{
			//Code to parse uploaded workflow xml and update status
			try{
				
				JiraWorkFlowParser workflowParser = new JiraWorkFlowParser(wsClient, null);
				if(workflow==null || workflow.trim().isEmpty())
					throw new OIMJiraAdapterException("0088", new String[] {internalId}, null);
				transitionName = workflowParser.fetchTransitionFromWorkFlow(workflow, currentStatus, status);					
			} catch (ParserConfigurationException | SAXException | IOException e) {
				throw new OIMJiraAdapterException("0043",new String[]{e.getMessage()},e);
			} catch (OIMJiraApiException e) {
				throw new OIMJiraAdapterException("0023",new String[]{CHANGE_STATUS},e);
			}
		}
		else if(hasDbAccess){
			Session jiraSession = null;
			try {
				jiraSession = jiraSessionFactory.openSession();
				transitionName = getAvailableTransition(internalId, status,jiraSession);
			} catch (ORMException e) {
				throw new OIMJiraAdapterException(_0050,null,e);
			}finally{
				jiraSessionFactory.cleanup(jiraSession, null);
			}	
		}
		
		if(transitionName!=null && JiraUtility.isValidTransition(wsClient,internalId,transitionName)!=null)
		{
			try{
				login();
				RemoteFieldValue[] fieldValues = JiraUtility.getRemotFieldValue(wsClient, internalId,transitionName,resolution,lastUpdateValue,this.getOHLastUpdateFieldName());
				checkTransitionAndProgressWorkflow(internalId,transitionName,fieldValues);
			}finally{
				logout();
			}
		}
		else{
			throw new OIMJiraAdapterException("0022",new String[]{internalId,status},null);
		}
	}

	/**
	 * 
	 * @param internalId
	 * @param systemProperties
	 * @param jiraSession
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	private String getAvailableTransition(final String internalId,String status, final Session jiraSession) throws OIMJiraAdapterException
	{
		if(status != null && !status.isEmpty()) {
			RemoteIssue issueObj = getEntityObject(internalId);
			String currentStatus;
			try {
				currentStatus = JiraUtility.getStatusNameById(wsClient,issueObj.getStatus());
				RemoteProject project =wsClient.getProjectByKey(issueObj.getProject()); 
				String projectId = project.getId();
				status = getTransitionName(issueObj.getType(), projectId, currentStatus, status, jiraSession);
				OpsHubLoggingUtil.debug(LOGGER, "Transition : " + status + " found for current status : " 
						+ currentStatus + " and target status : " + status, null);
			} catch (OIMJiraDBAccessException e) {
				OpsHubLoggingUtil.error(LOGGER, "Failed In getting transition name " + e.getLocalizedMessage(), e);
				throw new OIMJiraAdapterException("0024",new String[]{internalId},e);
			} catch (OIMJiraApiException e) {
				OpsHubLoggingUtil.error(LOGGER, "Error occurred while calling web service from method : getAvailableTransition", e);
				throw new OIMJiraAdapterException("0023",new String[]{"getAvailableTransition"},e);
			}
		}
		return status;

	}

	/**
	 * Method which return id of the custom field in hash map
	 * @param map
	 * @return
	 * @throws OIMJiraAdapterException 
	 * @throws OIMJiraApiException 
	 */
	private RemoteCustomFieldValue[] getCustomFields(final Map<String, Object> map, final HashMap<String, String> customFieldsIdName) throws OIMJiraAdapterException
	{

		RemoteCustomFieldValue value[]=null;
		if(map != null){
			value = new RemoteCustomFieldValue[map.size()];
			Iterator<Entry<String, Object>> itr =  map.entrySet().iterator();
			int i=0;
			String key=null;
			List<String> nonAvailableCustomFields = new ArrayList<String>();
			while(itr.hasNext())
			{
				Entry<String, Object> rCustomFields = itr.next(); 
				key = rCustomFields.getKey().toString();
				RemoteCustomFieldValue remoteCustomFieldValue = new RemoteCustomFieldValue();
				String customFieldId = customFieldsIdName.get(key);
				if(customFieldId == null || customFieldId.isEmpty())
				{
					nonAvailableCustomFields.add(key);
					i++;
					continue;
				}
				remoteCustomFieldValue.setCustomfieldId(customFieldsIdName.get(key));
				Object custFldValue = rCustomFields.getValue();
				String values[] = null;
				if(custFldValue!=null){
					if(custFldValue instanceof String[]){
						values = (String[])rCustomFields.getValue();
					}else {
						values = new String[] {rCustomFields.getValue()==null?null:(rCustomFields.getValue()).toString()};
					}
				}
				remoteCustomFieldValue.setValues(values);
				value[i] = remoteCustomFieldValue;
				i++;
			}
			if(!nonAvailableCustomFields.isEmpty()){
				OpsHubLoggingUtil.error(LOGGER, "Jira does not have field(s) : " + nonAvailableCustomFields.toString(), null);
				throw new OIMJiraApiException(_0035,new String[]{nonAvailableCustomFields.toString()},null);
			}
		}
		return value;

	}

	/**
	 * 
	 * @param stepNumber
	 * @return
	 */
	private String getSubStepExecutionNumber(final String stepNumber) {

		if(stepNumber==null)
			return "-1";
		String subStepNumber=null;
		String[] stepNumberArray = stepNumber.split("\\.");
		if(stepNumber!=null && stepNumber.contains(".") && stepNumberArray.length >= 3){

			int index = stepNumber.lastIndexOf('.');
			subStepNumber=stepNumber.substring(index + 1);
		}
		return subStepNumber;

	}

	/**
	 * 
	 * @param issueTypeId
	 * @param projectId
	 * @param currentStatus
	 * @param targetStatus
	 * @param jiraSession
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	private String getTransitionName(final String issueTypeId, final String projectId,final String currentStatus,final String targetStatus, final Session jiraSession) throws OIMJiraAdapterException 
	{

		try{
			JiraDbAccessClient dbAccessClient = new JiraDbAccessClient(jiraSession);
			JiraWorkFlowParser workFlowParser = new JiraWorkFlowParser(wsClient,dbAccessClient);	
			return workFlowParser.getTransitionName(issueTypeId, projectId, currentStatus, targetStatus);
		} catch (ParserConfigurationException | SAXException | IOException e) {
			throw new OIMJiraAdapterException("0043",new String[]{e.getMessage()},e);
		} catch (OIMJiraApiException e) {
			throw new OIMJiraAdapterException("0023",new String[]{"getTransitionName"},e);
		}

	}

	/**
	 * This method updates the issue for given issue id.
	 * @param issueId
	 * @param systemProperties
	 * @param customProperties
	 * @param lastUpdateFieldName
	 * @param lastUpdateFieldValue
	 * @param isRecovery
	 * @throws OIMJiraAdapterException 
	 */
	private RemoteIssue updateIssue(final String issueId,Map<String, Object> systemProperties,Map<String, Object> customProperties) throws OIMJiraAdapterException
	{

			if(systemProperties == null && customProperties == null) {
				return getEntityObject(issueId);
			}
			if(systemProperties==null)
				systemProperties = new HashMap<String, Object>();
			if(customProperties==null)
				customProperties = new HashMap<String, Object>();
			
			//If all the custom fields are available, check whether all the fields are available 
			//in edit screen or not for given issueId. Throw an exception if field does not available. 
			JiraUtility.isCustomFieldsEditable(wsClient, customProperties, issueId);

			int fieldSize = systemProperties.size() + customProperties.size();

			RemoteFieldValue[] actionParams = new RemoteFieldValue[fieldSize];

			Iterator<Entry<String,Object>> custom = customProperties.entrySet().iterator();
			
			int i=0;
			String customFieldKey = null;
			Object customFieldValue= null;
			RemoteFieldValue customRemoteFieldValue = null;
			Entry<String,Object> customField = null;
			
			// Get custom fields in a project
			RemoteIssue issue = wsClient.getIssueByKey(issueId);
			String issueType = JiraUtility.getIssueTypeNameById(wsClient, issue.getType());
			HashMap<String, String> customFieldsIdName = getJiraRestRequestor().getCustomFieldsIDNamePairForProject(issue.getProject(), issueType);
			if(customFieldsIdName == null){
				customFieldsIdName = JiraUtility.getCustomFieldIdsByNames(wsClient, customProperties.keySet());
			}
			List<String> nonAvailableCustomFields = new ArrayList<String>();
			//initialize RemoteFieldValue for custom fields
			while(custom.hasNext())
			{
				customField = custom.next();
				customFieldKey = customField.getKey().toString();
				customRemoteFieldValue = new RemoteFieldValue();
				String customFieldId = customFieldsIdName.get(customFieldKey);
				if(customFieldId == null || customFieldId.isEmpty())
				{
					nonAvailableCustomFields.add(customFieldKey);
					i++;
					continue;
				}
				customRemoteFieldValue.setId(customFieldId);
				customFieldValue = customField.getValue(); 
				if(customFieldValue != null){
					if(customFieldValue instanceof String[]) {
						customRemoteFieldValue.setValues((String[])customFieldValue);
					}else{
						customRemoteFieldValue.setValues(new String[]{customFieldValue.toString()});
					}
				}
				actionParams[i] = customRemoteFieldValue;
				i++;
			}
			if(!nonAvailableCustomFields.isEmpty()){
				OpsHubLoggingUtil.error(LOGGER, "Jira does not have field(s) : " + nonAvailableCustomFields.toString(), null);
				throw new OIMJiraApiException(_0035,new String[]{nonAvailableCustomFields.toString()},null);
			}

			Iterator<Entry<String,Object>> systemPropItr = systemProperties.entrySet().iterator();
			
			RemoteFieldValue systemRemoteFieldValue = null;
			Entry<String,Object>  systemField = null;
			String systemFieldKey = null;
			Object systemFieldValue= null;
			//initialize RemoteFieldValue for system fields
			while(systemPropItr.hasNext())
			{
				systemField = systemPropItr.next();
				systemFieldKey = systemField.getKey().toString();
				systemFieldValue = systemField.getValue();
				
				systemRemoteFieldValue = new RemoteFieldValue();
				systemRemoteFieldValue.setId(systemFieldKey);
				if(systemFieldValue != null && systemFieldKey.equals(JiraConstants.SystemField.ISSUETYPE))
					systemRemoteFieldValue.setValues(new String[]{JiraUtility.getIssueTypeIdByName(wsClient, systemFieldValue.toString())});
				else if(systemFieldValue != null && systemFieldKey.equals(JiraConstants.SystemField.PRIORITY))
					systemRemoteFieldValue.setValues(new String[]{JiraUtility.getPriorityIdByName(wsClient, systemFieldValue.toString())});
				else if(systemFieldValue != null && systemFieldKey.equals(JiraConstants.SystemField.AFFECTVERSIONS))
					systemRemoteFieldValue.setValues(JiraUtility.getVersionIdsByNames(wsClient, (String[])systemFieldValue,issue.getProject()));
				else if(systemFieldValue != null && systemFieldKey.equals(JiraConstants.SystemField.COMPONENTS))
					systemRemoteFieldValue.setValues(JiraUtility.getCompnentIdsByNames(wsClient, (String[])systemFieldValue,issue.getProject()));
				else if(systemFieldValue != null && systemFieldKey.equals(JiraConstants.SystemField.FIXVERSIONS))
					systemRemoteFieldValue.setValues(JiraUtility.getVersionIdsByNames(wsClient, (String[])systemFieldValue,issue.getProject()));
				else if(systemFieldValue != null)
					systemRemoteFieldValue.setValues(new String[]{systemFieldValue.toString()});
				actionParams[i] = systemRemoteFieldValue;
				i++;
			}

			if(actionParams==null || actionParams.length==0)
				throw new OIMJiraAdapterException("0033", null,null);		

			for(RemoteFieldValue remotefield: actionParams)
				validateRemoteFieldValue(remotefield, issueId);

			RemoteIssue updatedIssue = wsClient.updateIssue(issueId, actionParams);
			OpsHubLoggingUtil.debug(LOGGER,"Issue successfully updated in Jira " + issue.getKey(),null);
			return updatedIssue;

	}

	/**
	 * This method validates all the mandatory fields before creating an issue in jira
	 * @param issue
	 * @return
	 * @throws OIMJiraApiException
	 */
	private void validateMandatoryIssueFields(final RemoteIssue issue) throws OIMJiraAdapterException
	{
		RemoteFieldValue mandatoryField = new RemoteFieldValue();

		// validating project field
		mandatoryField.setId(JiraConstants.SystemField.PROJECTKEY);
		mandatoryField.setValues(new String[]{issue.getProject()}) ;
		validateRemoteFieldValue(mandatoryField, issue.getKey());		

		// validating issue type field		
		mandatoryField.setId(JiraConstants.SystemField.ISSUETYPE);		
		mandatoryField.setValues(new String[]{issue.getType()}) ;
		validateRemoteFieldValue(mandatoryField,issue.getKey());

		// validating summary field		
		mandatoryField.setId(JiraConstants.SystemField.SUMMARY);		
		mandatoryField.setValues(new String[]{issue.getSummary()});
		validateRemoteFieldValue(mandatoryField, issue.getKey());

		// validating assignee field		
		mandatoryField.setId(JiraConstants.SystemField.ASSIGNEE);		
		mandatoryField.setValues(new String[]{issue.getAssignee()});
		validateRemoteFieldValue(mandatoryField, issue.getKey());		

	}

	/**
	 * This method validates passed remote field value object for null or invalid values. 
	 * Throws exception if validation fails. This method will validate only the first value from the array of values.
	 * @param rf
	 * @param issueKey
	 * @throws OIMJiraApiException
	 */
	private void validateRemoteFieldValue(final RemoteFieldValue rf, final String issueKey) throws OIMJiraAdapterException
	{
		if(rf == null){
			return;
		}
		String id = rf.getId(); // id holds name of the field
		String[] values = rf.getValues(); // values holds array of field values which should contain only one string value. 
		if(id==null || id.equals("") || values==null || values.length!=1){
			return;
		}		

		/* Validating field values for null or empty string. Not validating assignee field as null values may 
		 * be allowed for assignee field depending upon jira configuration */		
		if(!id.equals(JiraConstants.SystemField.ASSIGNEE)){
			if(values[0]==null){							
				throw new OIMJiraApiException("0029", new String[]{id},null);
			}
		}				

		// Now validating specific jira fields for their acceptable values in jira
		if(id.equals(JiraConstants.SystemField.PRIORITY)){
			String priorityNm = JiraUtility.getPriorityNameById(wsClient, values[0]);
			if(priorityNm==null)
				throw new OIMJiraApiException("0034", new String[]{"priority"},null);				
		}
		else if(id.equals(JiraConstants.SystemField.ISSUETYPE)){
			String issueTypeName = JiraUtility.getIssueTypeNameById(wsClient,values[0]);
			if(issueTypeName==null)
				throw new OIMJiraApiException("0034", new String[]{"Issue Type"},null);			
		}		
		else if(id.equals(JiraConstants.SystemField.PROJECTKEY))	{
			RemoteProject project = wsClient.getProjectByKey(values[0]);
			if(project==null)								
				throw new OIMJiraApiException("0034", new String[]{"Project"},null);	
		}	
		else if(id.equals(JiraConstants.SystemField.REPORTER)){
			RemoteUser user = wsClient.getUser(values[0]);
			if(user==null)							
				throw new OIMJiraApiException("0034", new String[]{"Reporter"},null);			
		}				
		else if(id.equals(JiraConstants.SystemField.ASSIGNEE)){
			String assingee  = values[0];		
			if (assingee!=null && !assingee.equals("")){
				if(!userExists(assingee))
					throw new OIMJiraApiException("0034", new String[]{"Assignee"},null);
			}
		}
	}

	/**
	 * This method returns events as per history based.
	 * @param afterTime
	 * @param maxTime
	 * @param lastProcessedId
	 * @param lastProcessedIssueKey
	 * @param issueTypeIdsString
	 * @param issueTypesString
	 * @param projectKeyToBePolled
	 * @param projectIds
	 * @param syncFieldName
	 * @param syncFieldValue
	 * @param notUpdatedByUser
	 * @param pageSize
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	private MergeEvents getEventsForAdvanceConnector(final Session jiraSession,final Timestamp afterTime, final Timestamp maxTime,final String lastProcessedId,
			final String lastProcessedIssueKey,final String lastProcessedRevisionId,final String issueTypeIdsString, final String issueTypesString,final String projectKeyToBePolled,final String projectIds,
			final String notUpdatedByUser,final Integer pageSize) throws OIMJiraAdapterException
			{

		Integer lastProcessedEventId = lastProcessedId == null ? null : Integer.valueOf(lastProcessedId);
		Integer lastProcessedChangeGroupId = lastProcessedRevisionId == null ? null : Integer.valueOf(lastProcessedRevisionId);
		
		JiraDbAccessClient jiraDbClient = new JiraDbAccessClient(jiraSession);

		//Get All Create Events
		Iterator createIssueIdItr = jiraDbClient.getCreateEvents(afterTime,maxTime,lastProcessedEventId,issueTypeIdsString,
				 projectIds,jiraUserName,pageSize, getJiraVersion(), wsClient);
		
		//Get All Issues matching projects to be poller , issue types to be polled and sync field is set If criteria is configured.
		Iterator changeGroupItr = null;
		Iterator<JiraAction> actionItr = null;
		String query = jiraDbClient.getAllIssues(projectIds, issueTypeIdsString);

			//Get All Update Events
			changeGroupItr = jiraDbClient.getUpdateEvents(afterTime,maxTime,lastProcessedEventId,lastProcessedChangeGroupId,
					notUpdatedByUser,query,pageSize);
			//Get All Comment Events
			actionItr = jiraDbClient.getCommentEvents(afterTime,maxTime,lastProcessedEventId,lastProcessedChangeGroupId,notUpdatedByUser,
					query,pageSize);

		//Merge Create and Update Events on Time Basis
		MergeEvents mergeList = new MergeEvents(createIssueIdItr, changeGroupItr,actionItr, pageSize);

		return mergeList;
			}

	/**
	 * This method returns events as per current state.
	 * @param afterTime
	 * @param maxTime
	 * @param lastProcessedIssueKey
	 * @param issueTypesString
	 * @param projectKeyToBePolled
	 * @param syncFieldName
	 * @param syncFieldValue
	 * @param pageSize
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	private MergeEvents getEventsForSimpleConnector(final Timestamp afterTime, final Timestamp maxTime,final String lastProcessedIssueKey, 
			final String issueTypesString,final String projectKeyToBePolled,final Integer pageSize,final String timeZoneId) throws OIMJiraAdapterException
			{
		//Get Create Events
		List createEvents = JiraUtility.getEventsOfType(wsClient, JQL.JQL_EVENT_TYPE_CREATED, afterTime,maxTime,lastProcessedIssueKey,
				issueTypesString,projectKeyToBePolled,timeZoneId);

		//Get Update Events
		List updateEvents = JiraUtility.getEventsOfType(wsClient, JQL.JQL_EVENT_TYPE_UPDATED, afterTime,maxTime,lastProcessedIssueKey,
				issueTypesString,projectKeyToBePolled,timeZoneId);

		//Merge Create and Update Events on Time Basis
		MergeEvents mergeList = new MergeEvents(createEvents.iterator(), updateEvents.iterator(),null, pageSize);

		return mergeList;
			}
	
	/* in case of id is matching issue key regex, url would be like following
	 * 	http://10.13.28.10:8080/browse/@ID
	 * 		[where id is issue key like SAMP-555]
	 * otherwise it will be considered as following url
	 * http://10.13.28.10:8080/secure/ViewIssue.jspa?id=@ID 
	 * 		[where id is integer/{OriginalId}/any other url pattern used by another end system
	 */
	@Override
	public String getRemoteEntityLink(final String id) throws OIMJiraAdapterException {
		try {
			boolean isIssueKey= id.matches("\\d+");
			int length= jiraWebServiceURL.indexOf("/rpc/soap/", -1);
			String modilfyService = EaiUtility.getBaseURLFromString(jiraWebServiceURL);
			if(length>1)
				modilfyService= jiraWebServiceURL.subSequence(0, length).toString();
			
			if(id!=null && id.equalsIgnoreCase(Constants.ORIGINAL_ID)){
				return modilfyService.concat("/secure/ViewIssue.jspa?id=" + id);
			}else{
				if(isIssueKey) {
					return modilfyService.concat("/secure/ViewIssue.jspa?id=" + id);
				}
				else{
					modilfyService = modilfyService.concat("/browse/" + id);
					return modilfyService;
				}
			}
		} catch (MalformedURLException e) {
			throw new OIMJiraAdapterException("0094", new String[] { jiraWebServiceURL }, e);
		}
	}

	@Override	
	public Map<String,Object> getFieldMap(final String fieldName, final String vale, final boolean isSystemMap) {
		Map<String,Object> map = new HashMap<String, Object>();
		if(isSystemMap)
		{			
			map.put(fieldName, vale);
		}else{
			map.put(fieldName, vale);
		}
		return map;
	}
	
	@Override
	/**
	 * After creating issue jira returns the display name
	 * so getInternalId returns the actual entity id
	 */
	public String getInternalId(final String displayId) throws OIMAdapterException {
		
		if(displayId!=null){
			RemoteIssue issue= getEntityObject(displayId);
			return issue.getId();			
		}
		else
			return "";
	}
	
	@Override
	public InputStream getAttachmentInputStream(final String attachmentURI) throws OIMJiraAdapterException{
		InputStream is = null;	
		try {
			login();
			
			OpsHubAuthenticator opsHubAuthenticator = OpsHubAuthenticator.getOpsHubAuthenticator();
			opsHubAuthenticator.registerCredentialForAuthentication(attachmentURI, this.jiraUserName, this.jiraUserPassword,null);
			Authenticator.setDefault(opsHubAuthenticator);
			
			URL url = new URL(attachmentURI);
			URLConnection conn = url.openConnection();
			String userData = this.jiraUserName + ":" + this.jiraUserPassword;
	        String encodedUserPass = EaiUtility.encodeBase64(userData.getBytes());
	        conn.addRequestProperty("Authorization", "BASIC " + encodedUserPass);	        
			is = conn.getInputStream();
		} catch (Exception e) {
			throw new OIMJiraAdapterException("0092", new String[] { e.getLocalizedMessage() }, e);
		}finally{
			logout();
		}
		return is;
	}
	
	@Override
	public Map<String, String> getMappedPropertyName()
			throws OIMAdapterException {
		Map<String, String> fieldMap =  new HashMap<String, String>();
		fieldMap.put(OHFieldMappingInterface.OH_ASSIGNEDTO, JiraConstants.SystemField.ASSIGNEE);
		fieldMap.put(OHFieldMappingInterface.OH_REPORTEDBY, JiraConstants.SystemField.REPORTER);
		fieldMap.put(OHFieldMappingInterface.OH_STATUS, JiraConstants.SystemField.STATUS);
		return fieldMap;
	}

	@Override
	public boolean userExists(final String userName, final boolean isCaseSensitive)
			throws OIMAdapterException {
		boolean userExist = false;
		try {
			login();
			RemoteUser user = wsClient.getUser(userName);
			if(user==null)
				userExist = false;
			else
				userExist = true;
		}finally{
			logout();
		}
		return userExist;
	}

	@Override
	public boolean projectExists(final String projectName, final boolean isCaseSensitiv)
			throws OIMAdapterException {
		boolean projectExists = false;
		try {
			login();
			String projectKey = JiraUtility.getProjectKeyByName(wsClient, projectName);
			if(projectKey==null)
				projectExists = false;
			else
				projectExists = true;
		}finally{
			logout();
		}
		return projectExists;
	}	
	
	
	@Override
	/* This method fetches emailId values for given username
	 * @param: username,casesensitive
	 * @return: emailList
	 * */
	public List<String> getEmailFromUserName(final String userName,final boolean caseSensitive) throws OIMAdapterException 
	{
		List<String> emailList =new ArrayList<String>();
		String userEmail = getPropertyFromUserName(JiraConstants.USER_EMAIL_KEY, userName);
		emailList.add(userEmail);
		return emailList;

	}

	@Override
	/* This method fetches userfullname values for given emialId
	 * @param: email,casesensitive
	 * @return: fullrNameList
	 * */
	public List<String> getUserFullNameFromEmail(final String email,
			final boolean caseSensitive) throws OIMAdapterException {

		if(hasDbAccess){
			List<String> fullNameList =new ArrayList<String>();
			String[] userName = getUserNameFromProperty(JiraConstants.USER_EMAIL_KEY, email,caseSensitive);
			String userFullName = null;
			for(int i=0;i<userName.length;i++)
			{
				userFullName = getPropertyFromUserName(JiraConstants.USER_FULLNAME_KEY, userName[i]);
				fullNameList.add(userFullName);
			}
			return fullNameList;
		}else{
			throw new OIMJiraAdapterException("0095",new String[]{"Get User Fullname from Email"},null);
		}
	}

	@Override
	/* This method fetches username values for given emialId
	 * @param: email,casesensitive
	 * @return: userNameList
	 * */
	public List<String> getUserNameFromEmail(final String email, final boolean caseSensitive)
	throws OIMAdapterException {


		if(hasDbAccess){
			List<String> userNameList =new ArrayList<String>();
			String[] userName = getUserNameFromProperty(JiraConstants.USER_EMAIL_KEY, email,caseSensitive);
			for(int i=0;i<userName.length;i++)
			{
				userNameList.add(userName[i]);
			}
			return userNameList;
		}else{
			throw new OIMJiraAdapterException("0095",new String[]{"Get UserName from Email"},null);
		}
	}

	@Override
	/* This method fetches enumeration values of given field FieldName
	 * @param: fieldNAme
	 * @return: enumrationList
	 * */
	public List<String> getEnumerationsForField(final String fieldName)
	throws OIMAdapterException {
		List<String> enumerationList = new ArrayList<String>();
		Session jiraSession = null;
		try{
			login();

			if (fieldName.equalsIgnoreCase(JiraConstants.SystemField.PRIORITY))
			{
				RemotePriority[] priorityList= wsClient.getPriorities();
				for(int i=0; i<priorityList.length;i++)
				{
					enumerationList.add(priorityList[i].getName());
				} 
			}
			else if (fieldName.equalsIgnoreCase(JiraConstants.SystemField.ISSUETYPE))
			{
				List<String> issueList=wsClient.getAllIssueTypeNameList();
				enumerationList=issueList;
			}
			else if (fieldName.equalsIgnoreCase(JiraConstants.SystemField.STATUS))
			{
				RemoteStatus[] statusList=wsClient.getStatuses();
				for(int i=0; i<statusList.length;i++)
				{
					enumerationList.add(statusList[i].getName());
				} 
			}
			else if (fieldName.equalsIgnoreCase(JiraConstants.SystemField.PROJECTNAME))
			{
				RemoteProject[] projectList=wsClient.getAllProjects();
				for(int i=0; i<projectList.length;i++)
				{
					enumerationList.add(projectList[i].getName());
				} 
			}
			else if (fieldName.equalsIgnoreCase(JiraConstants.SystemField.RESOLUTION))
			{
				RemoteResolution[] resolutionList=wsClient.getResolutions();
				for(int i=0; i<resolutionList.length;i++)
				{
					enumerationList.add(resolutionList[i].getName());
				} 
			}
			else if (fieldName.equalsIgnoreCase(JiraConstants.SystemField.FIXVERSIONS)||fieldName.equalsIgnoreCase(JiraConstants.SystemField.AFFECTVERSIONS))
			{
				List<String> versionList=wsClient.getallVersions(wsClient.getAllProjects());
				enumerationList.addAll(versionList);
			}
			else if (fieldName.equalsIgnoreCase(JiraConstants.SystemField.COMPONENTS))
			{
				List<String> componentList=wsClient.getallComponents(wsClient.getAllProjects());
				enumerationList.addAll(componentList);
			}
			else{
				/**
				 * If custom field is of type single list or multiple list, enumerations wont be
				 * fetched if database is not available, regardless of version.
				 * If database information is not there we are throwing exception 
				 * saying lack of database as rest is not capable of differenciate what options are for what
				 * custom fields.
				 */
				if(hasDbAccess){
					try {
						jiraSession =  jiraSessionFactory.openSession();
						JiraDbAccessClient dbAccessClient = new JiraDbAccessClient(jiraSession);
						JiraCustomFields customConf =dbAccessClient.getCustomFieldConfForField(fieldName);
						enumerationList=customConf.getOptions(); 
					} catch (ORMException e) {
						throw new OIMJiraAdapterException(_0050,null,e);
					}
				}else{
					throw new OIMJiraAdapterException("0095",new String[]{"Enumeration for Custom Field"},null);
				}
			}
		}
		finally{
			logout();
			if(jiraSession!=null && jiraSession.isOpen()){
				jiraSessionFactory.cleanup(jiraSession, null);
			}
		}
		return enumerationList;
	}
	
	@Override
	/* This method fetches list of metadata of all system and custom fields 
	 * @return: metadata
	 * */
	public List<FieldsMeta> getFieldsMetadata() throws OIMAdapterException {

		List<FieldsMeta> metadata = new ArrayList<FieldsMeta>();

		metadata.add(setFieldMetadata(JiraConstants.SystemField.ASSIGNEE,JiraConstants.SystemField.DisplayName.ASSIGNEE,DataType.USER,true,false,false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.ISSUETYPE,JiraConstants.SystemField.DisplayName.ISSUETYPE,DataType.LOOKUP,true, true, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.FIXVERSIONS,JiraConstants.SystemField.DisplayName.FIXVERSIONS,DataType.LOOKUP,true, false, true));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.COMPONENTS,JiraConstants.SystemField.DisplayName.COMPONENTS,DataType.LOOKUP,true, false, true));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.AFFECTVERSIONS,JiraConstants.SystemField.DisplayName.AFFECTVERSIONS,DataType.LOOKUP,true, false, true));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.DESCRIPTION,JiraConstants.SystemField.DisplayName.DESCRIPTION,DataType.TEXT,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.DUEDATE,JiraConstants.SystemField.DisplayName.DUEDATE,DataType.DATE_STRING,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.ENVIRONMENT,JiraConstants.SystemField.DisplayName.ENVIRONMENT,DataType.TEXT,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.PRIORITY,JiraConstants.SystemField.DisplayName.PRIORITY,DataType.LOOKUP,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.SUMMARY,JiraConstants.SystemField.DisplayName.SUMMARY,DataType.TEXT,true, true, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.PROJECTNAME,JiraConstants.SystemField.DisplayName.PROJECTNAME,DataType.LOOKUP,true, true, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.VOTES,JiraConstants.SystemField.DisplayName.VOTES,DataType.TEXT,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.STATUS,JiraConstants.SystemField.DisplayName.STATUS,DataType.LOOKUP,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.RESOLUTION,JiraConstants.SystemField.DisplayName.RESOLUTION,DataType.LOOKUP,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.REPORTER,JiraConstants.SystemField.DisplayName.REPORTER,DataType.USER,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.UPDATED,JiraConstants.SystemField.DisplayName.UPDATED,DataType.DATE_STRING,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.CREATED,JiraConstants.SystemField.DisplayName.CREATED,DataType.DATE_STRING,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.ORIGINAL_ESTIMATE,JiraConstants.SystemField.DisplayName.ORIGINAL_ESTIMATE,DataType.TEXT,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.REMAINING_ESTIMATE,JiraConstants.SystemField.DisplayName.REMAINING_ESTIMATE,DataType.TEXT,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.TIME_SPENT,JiraConstants.SystemField.DisplayName.TIME_SPENT,DataType.TEXT,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.ISSUEKEY,JiraConstants.SystemField.DisplayName.ISSUEKEY,DataType.TEXT,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.ISSUEID,JiraConstants.SystemField.DisplayName.ISSUEID,DataType.TEXT,true, false, false));
		metadata.add(setFieldMetadata(JiraConstants.SystemField.LABELS,JiraConstants.SystemField.DisplayName.LABELS,DataType.TEXT,true, false, true));
		
		/**
		 * Database is not used for fetching the field details for Jira with any version.
		 * Details are now fetched from RestAPI
		 */
		Session jiraSession = null;
		try{
			/**
			 * Check for Jira Version. If version is above 5.0 then get custom fields from REST api.
			 * If version if below 5.0 then we need to check for db Access
			 * If database access is there then we will get fields from database.
			 * If we are not having db access then we will fetch any issue that is lastly updated by integration user and fetch custom fields
			 * from that issue. This we are doing due to lack of API. 
			 */
			List<JiraCustomFields> customFields = new ArrayList<JiraCustomFields>();
			String version  = getJiraVersion();
			if(JiraUtility.isVersionEqualOrAbove("5.0", version))
				customFields = getJiraRestRequestor().getCustomFields(this);
			else {
				if(hasDbAccess){
					try{
						login();
						jiraSession = jiraSessionFactory.openSession();
						JiraDbAccessClient dbClient = new JiraDbAccessClient(jiraSession);
						RemoteField[] fields = wsClient.getCustomFields();
						for (int fieldsLen = 0; fieldsLen < fields.length; fieldsLen++) {
							String fieldName = fields[fieldsLen].getName();
							String typeKey = dbClient.getCustomFieldConfForField(fieldName).getCustomFieldTypeKey();
							FieldsMeta field = new FieldsMeta();
							setDatatTypeForField(field, typeKey);
							field.setDisplayName(fieldName);
							field.setInternalName(fieldName);
							field.setMandatory(false);
							field.setSystemField(false);
							metadata.add(field);
						}
					}finally{
						logout();
					}
					return metadata;
				}
				else
					customFields = getJiraRestRequestor().getCustomFields(this);
			}
			// This call returns the List of JiraCustomFields depending upon the version of Jira
			for(int fieldCount=0; fieldCount<customFields.size();fieldCount++){
				FieldsMeta field = new FieldsMeta();
				JiraCustomFields issueField = customFields.get(fieldCount);
				String typeKey = issueField.getCustomFieldTypeKey();
				setDatatTypeForField(field, typeKey);

				field.setDisplayName(issueField.getCustomFieldName());
				field.setInternalName(issueField.getCustomFieldName());
				field.setMandatory(false);
				field.setSystemField(false);
				metadata.add(field);
				}	
		}catch(Exception e){
			// log proper error in case of any issue in getting field metadata
			OpsHubLoggingUtil.error(LOGGER,"Failed to get field metadata because " + e.getMessage(),e);
			throw new OIMJiraAdapterException(_0030,new String[]{"Get Custom Field Metadata"},e);
		}
		return metadata;
	}
	
	private void setDatatTypeForField(final FieldsMeta customFieldName, final String typeKey) {
		if(typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_MULTISELECT)
				||typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_MULTICHECK)
				||typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_MULTIGROUP)									
				||typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_MULTIVERSION)
				||typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_VERSIONPICKER)
				||typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_SELECT))
		{
			customFieldName.setDataType(DataType.LOOKUP);
			//Exclude single select list before setting the multi-select boolean
			if(!(typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_VERSIONPICKER)
					|| typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_SELECT)))
				customFieldName.setMultiSelect(true);
		}else if(typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_USERPICKER)
				|| 	 typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_MULTIUSER) )
		{
			customFieldName.setDataType(DataType.USER);
			if(typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_MULTIUSER)){
				customFieldName.setMultiSelect(true);
			}
		}else if(typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_DATEPICKER)
				||typeKey.equalsIgnoreCase(JiraConstants.CustomField.CUSTOMFIELD_TYPE_DATETIME)){
			customFieldName.setDataType(DataType.DATE_STRING);
		}
		else
			customFieldName.setDataType(DataType.TEXT);
	}

	@Override
	/* This method fetches list of user along with its metadata for given system
	 * @param:
	 * @return: userList
	 * */
	public List<UserMeta> getUserList() throws OIMAdapterException {

		List<UserMeta> userList;
		Session jiraSession = null;

		if(hasDbAccess){

			try{
				jiraSession =  jiraSessionFactory.openSession();
				JiraDbAccessClient dbAccessClient = new JiraDbAccessClient(jiraSession);
				userList=dbAccessClient.getUserMeta(this.getJiraVersion());

			}
			catch (ORMException e) {
				throw new OIMJiraAdapterException(_0050,null,e);
			}

			finally{
				if(jiraSession!=null && jiraSession.isOpen()){
					jiraSessionFactory.cleanup(jiraSession, null);
				}
			}
		}
		else{
			throw new OIMJiraAdapterException("0095",new String[]{"Get User Metadata Metadata"},null);
		}
		return userList;

	}

	@Override
	/* This method returns boolean value for attachment support in Jira system
	 * @param: isSourceSystem
	 * 
	 *  */
	public boolean isAttachmentSupported(final boolean isSourceSystem)
	throws OIMAdapterException {
		return true;
	}

	@Override
	/* This method returns boolean value for Comments and Notes support in Jira system
	 * @param: isSourceSystem
	 * 
	 *  */
	public boolean isCommentsOrNotesSupported(final boolean isSourceSystem)
	throws OIMAdapterException {
		if(isSourceSystem)
			return hasDbAccess;
		return true;
	}

	@Override
	/* This method returns boolean value for Relationship support in Jira system
	 * @param: isSourceSystem
	 * 
	 *  */
	public boolean isRelationShipSupported(final boolean isSourceSystem)
	throws OIMAdapterException {
		return JiraUtility.isVersionEqualOrAbove("4.3", getJiraVersion());
	}
	
	/* This method sets systemField metadata into FieldsMeta object */
	private FieldsMeta setFieldMetadata(final String internalName, final String displayName,final DataType dataType,final boolean isSystemField,final boolean isMandatory,final boolean isMultiSelect){
		FieldsMeta fieldMetadata = new FieldsMeta(internalName,displayName,dataType,isSystemField);
		fieldMetadata.setMandatory(isMandatory);
		fieldMetadata.setMultiSelect(isMultiSelect);
		return fieldMetadata;
	}

	@Override
	public String getHtmlReplacementForNewLine() {
		return null;
	}

	@Override
	public boolean isCommentOrNotesHTML() throws OIMAdapterException {
		return false;
	}
		
	@Override
	public String getScopeId(final String entityId, final Object entityObject, final Map<String, Object> systemProperties) throws OIMAdapterException {
		RemoteIssue remoteIssue ;
		try{
			login();
			if(entityObject!=null && entityObject instanceof RemoteIssue){
				remoteIssue = (RemoteIssue) entityObject;
				return remoteIssue.getProject();
			}else if(entityId!=null){
				remoteIssue = getEntityObject(entityId);;
				return remoteIssue.getProject();
			}else if(systemProperties!=null){
				String key = getScopeFromProperties(systemProperties);
				if(key==null){
					OpsHubLoggingUtil.error(LOGGER, "Null/Invalid value for Project",null);
					throw new OIMJiraAdapterException("0029",new String[]{"Project"},null);
				}else{
					return key;
				}
			}else{
				OpsHubLoggingUtil.error(LOGGER, "Null/Invalid value for Project",null);
				throw new OIMJiraAdapterException("0029",new String[]{"Project"},null);
			}
		}finally{
			logout();
		}
	}
	
	private String getScopeFromProperties(final Map<String, Object> systemProperties) throws OIMJiraAdapterException{
		String projectkey = null;
		String projectName; 
		String foundProjectKeybyName =null; 			
		if(systemProperties!=null && (projectkey = (String)systemProperties.get(JiraConstants.SystemField.PROJECTKEY))!=null &&  !projectkey.isEmpty()) {
			projectkey = JiraUtility.validateProjectKey(wsClient,(String)systemProperties.get(JiraConstants.SystemField.PROJECTKEY));
		}else if(systemProperties!=null && (projectName = (String)systemProperties.get(JiraConstants.SystemField.PROJECTNAME))!=null && !projectName.isEmpty()) {
			foundProjectKeybyName = JiraUtility.getProjectKeyByName(wsClient,projectName);
		}
		return projectkey==null?foundProjectKeybyName:projectkey;		
	}

	@Override
	public String getActualEntityType(final String entityId, final Object entityObject, final Map<String, Object> systemProperties) throws OIMAdapterException {
		RemoteIssue remoteIssue ;
		try{
			login();
			if(entityObject!=null && entityObject instanceof RemoteIssue){
				remoteIssue = (RemoteIssue) entityObject;
				return this.getIssueTypeName(remoteIssue.getType());
			}else if(entityId!=null){
				remoteIssue = getEntityObject(entityId);;
				return this.getIssueTypeName(remoteIssue.getType());
			}else if(systemProperties!=null){
				String issueType = (String)systemProperties.get(JiraConstants.SystemField.ISSUETYPE); 
				if(issueType!=null && !issueType.isEmpty()){
					JiraUtility.getIssueTypeIdByName(wsClient,issueType);
					return issueType;
				}else{
					throw new OIMJiraAdapterException("0029", new String[]{JiraConstants.SystemField.ISSUETYPE}, null);
				}
			}else{
				throw new OIMJiraAdapterException("0029", new String[]{JiraConstants.SystemField.ISSUETYPE}, null);
			}
		}finally{
			logout();
		}
	}

	@Override
	public MaxEntityCarrier getMaxUpdateTime(final Calendar afterTime,final String afterRevisionId) throws OIMJiraAdapterException {
		
		MaxEntityCarrier maxUpdateTime=null;
		MaxEntityCarrier maxCreationTime=null;
		//Get Maximum time of create/update event.
		try{
			login();

			if(hasDbAccess){
				Session jiraSession;
				try {
					jiraSession = jiraSessionFactory.openSession();
				} catch (ORMException e) {
					throw new OIMJiraAdapterException(_0050,null,e);
				}
				JiraDbAccessClient jiraDbClient = new JiraDbAccessClient(jiraSession);
				maxCreationTime= jiraDbClient.getMaxCreationTime(wsClient, getJiraVersion());
				maxUpdateTime = jiraDbClient.getMaxUpdateTime(wsClient, jiraProjectIds);
			}
			else {
				maxUpdateTime= JiraUtility.getMaxTimeOfUpdateEvent(wsClient);
			}

			if(maxCreationTime!=null && maxUpdateTime!=null){
				if(maxCreationTime.getMaxTime().getTimeInMillis() >= maxUpdateTime.getMaxTime().getTimeInMillis())
					return maxCreationTime;
				else
					return maxUpdateTime;
			}else if(maxCreationTime==null && maxUpdateTime!=null){
				return maxUpdateTime;
			}else if(maxUpdateTime==null && maxCreationTime!=null){
				return maxCreationTime;
			}else{
				return new MaxEntityCarrier(null,null,null);
			}
		}finally{
			logout();
		}

	}

	@Override
	public boolean isHistoryAvailable() {
		return this.hasDbAccess;
	}

	@Override
	public Calendar getLastChangeTimeByIntegration(final Calendar afterTime, final String internalId,
			final String integrationUserName) throws OIMJiraAdapterException {
		if(hasDbAccess){
			Session jiraSession;
			try {
				jiraSession = jiraSessionFactory.openSession();
			} catch (ORMException e) {
				throw new OIMJiraAdapterException(_0050,null,e);
			}
			JiraDbAccessClient jiraDbClient = new JiraDbAccessClient(jiraSession);
			try{
				login();
				MaxEntityCarrier max = jiraDbClient.getMaxEntityCarrier(wsClient, internalId);
				if(max!=null)
					return max.getMaxTime();
				else return null;
			} finally{
				logout();
			}
		}
		return null;
	}


	@Override
	public List<EntityPrimaryDetailsCarrier> getEntitiesCreatedByIntegrationAfter(final Calendar afterTime,
			final String transactionId, final String integrationUserName, final Map<String, Object> properties)
			throws OIMAdapterException {
		try{
			login();
			
			String query = getOHLastUpdateFieldName()+" ~ \""+this.getAdapterIntegration().getIntegrationId()+"=\"";
			
			String timestampString = null;
			if(hasDbAccess)
				timestampString = JiraUtility.getTimeInJQLFormat(OIMCoreUtility.convertCalendarToTimestamp(afterTime));
			else{
				RemoteServerInfo remoteServerInfo = wsClient.getServerInfo();
				String timeZoneId = remoteServerInfo.getServerTime().getTimeZoneId();
				timestampString = JiraUtility.getTimeInJQLFormat(OIMCoreUtility.convertCalendarToTimestamp(afterTime),timeZoneId);
			}
			query = query + " and (" + JiraConstants.JQL.JQL_EVENT_TYPE_CREATED + " > \"" +timestampString + "\")"; 
			String intialRecordsQuery = query + " ORDER BY issuekey asc";

			String lastProcessedIssueKey="";

			RemoteIssue[] listOfMatchingIssue;
			List<EntityPrimaryDetailsCarrier> remoteIssueList = new ArrayList<EntityPrimaryDetailsCarrier>();
			//Query for first 100 items, and then do repetitive queries if required
			do
			{
				//if it query for first 100 records
				if(lastProcessedIssueKey.isEmpty())
				{
					listOfMatchingIssue = wsClient.getIssuesFromJqlSearch(intialRecordsQuery, JiraConstants.JQL_QUERY_LIMIT);
					if(listOfMatchingIssue==null || listOfMatchingIssue.length==0){
						OpsHubLoggingUtil.debug(LOGGER,"No issues found matching criteria.", null);
						return null;
					}
				}
				// for all subsequent queries append one more criteria that matches with issue kye
				else{
					String nextRecords = query + " and issuekey > " + "\"" + lastProcessedIssueKey + "\"" + " ORDER BY issuekey asc";
					listOfMatchingIssue = wsClient.getIssuesFromJqlSearch(nextRecords,JiraConstants.JQL_QUERY_LIMIT);
					if(listOfMatchingIssue==null || listOfMatchingIssue.length==0)
						break;
				}

				//get key of last issue so that can be used as offsed
				RemoteIssue lastIssueProcessed = listOfMatchingIssue[listOfMatchingIssue.length-1];
				lastProcessedIssueKey = lastIssueProcessed.getKey();

				//iterate through all issues
				for(int i=0;i<listOfMatchingIssue.length;i++){
					RemoteIssue currentIssue = listOfMatchingIssue[i];
					String issueTypeName = getIssueTypeName(currentIssue.getType());
					//this way we maintain window for issues
					EntityPrimaryDetailsCarrier entityDetails = new EntityPrimaryDetailsCarrier(listOfMatchingIssue[i].getKey(), listOfMatchingIssue[i].getProject(), issueTypeName);
					remoteIssueList.add(entityDetails);
				}
				//repeat above query if size of list equals query limit
			}while(listOfMatchingIssue!=null && listOfMatchingIssue.length==JiraConstants.JQL_QUERY_LIMIT);

			return remoteIssueList;
		}finally{
			logout();
		}
		
	}

	@Override
	public void removeLink(final String internalId, final String linkType,
			final String linkGlobalId, final String entityType,
			final Map<String, String> properties) throws OIMAdapterException {
		throw new OIMJiraAdapterException("0096", new String[]{"Remove-Link"}, null);
	}

	@Override
	public String getOHLastUpdateFieldName() {
		return Constants.OHLASTUPDATED;
	}

	@Override
	public OIMEntityObject getOIMEntityObject(final String internalId,
			final String operationId, final String subStepNo) throws OIMAdapterException {
		try{
			login();
			RemoteIssue updatedIssue = this.getEntityObject(internalId);
			OIMEntityObject oimEntityObject = getOIMEntityObjectForRemoteIssue(updatedIssue, false, false);
			return oimEntityObject;
		}finally{
			logout();
		}
	}
	
	public String getIssueTypeName(final String type) throws OIMJiraApiException{
		if(ISSUE_TYPE_ID_NAME_MAPPING.containsKey(type)){
			return ISSUE_TYPE_ID_NAME_MAPPING.get(type);
		}else{
			try{
				login();
				String issueTypeName = JiraUtility.getIssueTypeNameById(wsClient,type);
				ISSUE_TYPE_ID_NAME_MAPPING.put(type,issueTypeName);
				return issueTypeName;
			}finally{
				logout();				
			}
		}
	}
	
	@Override
	public List<String> getEntitySupportingLinks(final boolean isSourceSystem) {
		List<String> entitiesTypesSupported = new ArrayList<String>();
			try {
				login();
				entitiesTypesSupported=wsClient.getAllIssueTypeNameList();
			} catch (OIMJiraApiException e) {
				throw new RuntimeException(e.getLocalizedMessage());
			}finally{
				logout();
			}
		return entitiesTypesSupported;
	}

	@Override
	public List<String> getSupportedLinkTypes(final String entityName, final boolean isSource) {
		List<String> linkTypeList = new ArrayList<String>();
		try {
			linkTypeList=getJiraRestRequestor().getallLinkTypes();
			linkTypeList.add(JiraConstants.SystemField.PARENT);
		} catch (OIMJiraApiException e) {
			throw new RuntimeException(e.getLocalizedMessage());
		}
	return linkTypeList;

	}

	@Override
	public String getDateFormat(final String fieldName, final boolean isSource) {
		if(isSource == true){
			String format = null;
			try {
				List<JiraCustomFields> customFields = getJiraRestRequestor().getCustomFields(this);
				for (Iterator<JiraCustomFields> iterator = customFields.iterator(); iterator.hasNext();) {
					JiraCustomFields jiraCustomFields = iterator.next();
					if(fieldName.equals(jiraCustomFields.getCustomFieldName())){
						String type = jiraCustomFields.getCustomFieldTypeKey();
						if(JiraConstants.CustomField.CUSTOMFIELD_TYPE_DATEPICKER.equals(type))
							format = JiraConstants.SystemField.DATE_PICKER_FORMAT;
					}
				}
			}catch(Exception e) {
				throw new RuntimeException("Error occured while getting type of custom field from Rest Response");
			}
			
			if(format==null)
				return JiraConstants.SystemField.DATETIME_FORMAT;
			else {
				return format;
			}
		}
		else {
			if(fieldName.equals(JiraConstants.SystemField.DUEDATE))
				return JiraConstants.SystemField.DATE_PICKER_FORMAT;
			else {
				try {
					List<JiraCustomFields> customFields = getJiraRestRequestor().getCustomFields(this);
					for (Iterator<JiraCustomFields> iterator = customFields.iterator(); iterator.hasNext();) {
						JiraCustomFields jiraCustomFields = iterator.next();
						if(fieldName.equals(jiraCustomFields.getCustomFieldName())){
							String type = jiraCustomFields.getCustomFieldTypeKey();
							if(type.equals(JiraConstants.CustomField.CUSTOMFIELD_TYPE_DATEPICKER))
								return JiraConstants.SystemField.DATE_PICKER_FORMAT;
							else if(type.equals(JiraConstants.CustomField.CUSTOMFIELD_TYPE_DATETIME))
								return JiraConstants.CustomField.CUSTOM_DATE_TIME_FORMAT;
						}
					}
				}catch(Exception e) {
					throw new RuntimeException("Error occured while getting type of custom field from Rest Response");
				}
			}
		}
		return JiraConstants.SystemField.DATETIME_FORMAT;
	}
	
	public String getJiraUserName(){
		return jiraUserName;
	}
	public JiraWebServiceClient getClient(){
		return wsClient;
	}

	@Override
	public boolean isLinkHistorySupported() {
		return false;
	}

	@Override
	public boolean isLinkRemoveSupported() {
		return false;
	}

	@Override
	public String getLinkedEntityScopeId(final String linkEntityType,
			final Map<String, String> properties) throws OIMAdapterException {
		try{
			login();
			return getScopeFromProperties((Map)properties);
		}finally{
			logout();
		}
	}

	@Override
	public boolean isAttachmentUpdateSupported() {
		return false;
	}

	@Override
	public boolean isAttachmentDeleteSupported() {
		return false;
	}

	@Override
	public boolean isAttachmentHistorySupported() {
		return false;
	}

	@Override
	public EAIAttachment addAttachment(final String internalId,
			final InputStream byteInputStream, final String label, final String fileName,
			final String contentType, final String addedByUser, final Map<String, Object> additionalParameters) throws OIMAdapterException {
		try {
			login();
			if(internalId==null || internalId.equals("-1")){
				OpsHubLoggingUtil.error(LOGGER, "Attachment can not be added to issueKey :"+internalId, null);
				throw new OIMJiraAdapterException("0044", new String[]{internalId}, null);
			}
			byte[] byteArray = EaiUtility.getBytesFromInputStream(byteInputStream);
			String base64String = EaiUtility.encodeBase64(byteArray);
			wsClient.addBase64EncodedAttachmentsToIssue(internalId, new String[]{fileName}, new String[]{base64String});
			List<EAIAttachment> eaiAttachments = JiraUtility.getAttachmentList(wsClient, internalId, null);
			if(eaiAttachments==null)
				return null;
			for (EAIAttachment eaiAttachment : eaiAttachments) {
				if(eaiAttachment.getFileName().equals(fileName))
					return eaiAttachment;
			}
			return null;
		} catch (IOException e) {
			OpsHubLoggingUtil.error(LOGGER, "Attachment can not be added for  :"+internalId + ", error occurred " + e.getLocalizedMessage(), e);
			throw new OIMJiraAdapterException("0093", new String[] { e.getLocalizedMessage() }, e);
		}finally{
			logout();
		}
	}

	@Override
	public void deleteAttachment(final String internalId, final EAIAttachment attachment)
			throws OIMAdapterException {
		throw new OIMJiraAdapterException("0096", new String[]{"Delete-Attachement"}, null);
	}

	
	@Override
	public List<EAIAttachment> getCurrentAttachments(final String internalId,
			final Object entityObject) throws OIMAdapterException {
		try {
			login();
			return JiraUtility.getAttachmentList(wsClient, internalId, null);
		} catch (Exception e) {
			throw new OIMJiraAdapterException("0092", null, e);
		}finally{
			logout();
		}
	}

	@Override
	public List<LinkTypeMeta> getSupportedLinkTypesMeta(final String entityName,
			final boolean isSourceSystem) throws OIMAdapterException {
		try {
			return getJiraRestRequestor().getallLinkMeta();
		} catch (OIMJiraApiException e) {
			throw new RuntimeException(e.getLocalizedMessage());
		}
	}

	@Override
	public EAIAttachment updateAttachment(final String internalId,
			final EAIAttachment attachment, final InputStream byteInputStream)
			throws OIMAdapterException {
		throw new OIMJiraAdapterException("0096", new String[]{"Update-Attachment"}, null);
	}

	@Override
	public List<String> getUserNameFromFullName(final String fullName, final boolean caseSensitive) throws OIMAdapterException {
		String[] arrayOfNames = getUserNameFromProperty(JiraConstants.USER_FULLNAME_KEY,fullName,caseSensitive);
		return Arrays.asList(arrayOfNames);
	}
	public String getCommentInterval() {
		return commentInterval;
	}

}
