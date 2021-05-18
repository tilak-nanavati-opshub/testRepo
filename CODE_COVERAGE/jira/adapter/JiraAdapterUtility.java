/**
 * Copyright C 2011 OpsHub, Inc. All rights reserved
 */
package com.opshub.eai.jira.adapter;

import java.util.Collections;
import java.util.List;

import com.opshub.eai.core.adapters.OIMConnector;
import com.opshub.eai.core.exceptions.ConnectorLoaderException;
import com.opshub.eai.core.exceptions.OIMAdapterException;
import com.opshub.eai.core.utility.CommonConnectorLoader;
import com.opshub.eai.jira.common.JiraConstants;
import com.opshub.eai.jira.exceptions.OIMJiraAdapterException;
import com.opshub.eai.jira.rest.JIRARESTProcesser;
import com.opshub.eai.metadata.UserMeta;

public class JiraAdapterUtility {
	private static final String EMAIL_PATTERN = "^\\S+@\\S+\\.\\S+$";
	/**
	 * This method fetched system associated with given processInstanceId & systemTypeOrSystemDisplayName.  
	 * Loads system adapter on the basis of system found and returns Email of user associated with given userName.
	 * @param processInstanceId - workflow id, which will be passed to the method at runtime through transformation handler.
	 * @param systemTypeOrSystemDisplayName -system type name. In case of multiple systems of same type, system display name.
	 * @param userName
	 * @return Email Id of user. Returns empty string incase of no match found.
	 * @throws OIMJiraAdapterException 
	 */
	public static String getEmailFromUserName(String processInstanceId, String systemTypeOrSystemDisplayName,String userName) throws OIMJiraAdapterException {
		CommonConnectorLoader connectorLoader;
		JiraAdapter jiraAdapter = null;
		try {
			connectorLoader = new CommonConnectorLoader(processInstanceId, systemTypeOrSystemDisplayName,true);
			jiraAdapter = (JiraAdapter)connectorLoader.getOIMAdapter(null).getOimConnector();
		} catch (ConnectorLoaderException e) {
			throw new OIMJiraAdapterException("0060",null, e); 
		}
		return jiraAdapter.getPropertyFromUserName(JiraConstants.USER_EMAIL_KEY,userName);
	}

	/**
	 * This method fetched system associated with given processInstanceId & systemTypeOrSystemDisplayName.  
	 * Loads system adapter on the basis of system found and returns full name of user associated with given userName.
	 * @param processInstanceId - workflow id, which will be passed to the method at runtime through transformation handler.
	 * @param systemTypeOrSystemDisplayName -system type name. In case of multiple systems of same type, system display name.
	 * @param userName
	 * @return full name of user. Returns empty string in case of no match found.
	 * @throws OIMJiraAdapterException 
	 */
	public static String getFullNameFromUserName(String processInstanceId, String systemTypeOrSystemDisplayName,String userName) throws OIMJiraAdapterException {
		CommonConnectorLoader connectorLoader;
		JiraAdapter jiraAdapter = null;
		try {
			connectorLoader = new CommonConnectorLoader(processInstanceId, systemTypeOrSystemDisplayName,true);
			jiraAdapter = (JiraAdapter)connectorLoader.getOIMAdapter(null).getOimConnector();
		} catch (ConnectorLoaderException e) {
			throw new OIMJiraAdapterException("0060",null, e); 
		}
		return jiraAdapter.getPropertyFromUserName(JiraConstants.USER_FULLNAME_KEY,userName);
	}

	/**
	 * This method fetched system associated with given processInstanceId & systemTypeOrSystemDisplayName.  
	 * Loads system adapter on the basis of system found and returns username of user associated with given userEmail.
	 * If multiple user names are matched with given emailId, and "True" is passed, it will throw exception. 
	 * Else, will return first matching user name.
	 * @param processInstanceId - workflow id, which will be passed to the method at runtime through transformation handler.
	 * @param systemTypeOrSystemDisplayName -system type name. In case of multiple systems of same type, system display name.
	 * @param userEmail
	 * @param logErrorOnMultipleMatch
	 * @return UserName associated with userEmail. Returns empty string in case of no match found. Throws exception incase of 
	 * multiple user found and logErrorOnMultipleMatch = "True"
	 * @throws OIMJiraAdapterException
	 */
	public static String getUserNameFromEmail(String processInstanceId, String systemTypeOrSystemDisplayName,String userEmail,String logErrorOnMultipleMatch,String isCaseSensitiveStr) throws OIMJiraAdapterException {
		String userName = "";
		CommonConnectorLoader connectorLoader;
		JiraAdapter jiraAdapter = null;
		boolean caseSensitive = Boolean.parseBoolean(isCaseSensitiveStr);
		try {
			connectorLoader = new CommonConnectorLoader(processInstanceId, systemTypeOrSystemDisplayName,true);
			jiraAdapter = (JiraAdapter)connectorLoader.getOIMAdapter(null).getOimConnector();
		} catch (ConnectorLoaderException e) {
			throw new OIMJiraAdapterException("0060",null, e); 
		}
		String[] userNames = jiraAdapter.getUserNameFromProperty(JiraConstants.USER_EMAIL_KEY,userEmail,caseSensitive);
		if(userNames.length==0){
			return userName;
		}
		if(userNames.length>1 && logErrorOnMultipleMatch.equalsIgnoreCase("True")){
			throw new OIMJiraAdapterException("0061", new String[] { userEmail }, null);
		}else if(userNames.length>0 || logErrorOnMultipleMatch.equalsIgnoreCase("False")){
			userName = userNames[0];
		}else{
			throw new OIMJiraAdapterException("0062", new String[]{"logErrorOnMultipleMatch",logErrorOnMultipleMatch}, null); 
		}
		return userName;
	}

	/**
	 * This method fetched system associated with given processInstanceId & systemTypeOrSystemDisplayName.  
	 * Loads system adapter on the basis of system found and returns username of user associated with given full name.
	 * If multiple user names are matched with given fullname, and "True" is passed, it will throw exception. 
	 * Else, will return first matching user name.
	 * @param processInstanceId - workflow id, which will be passed to the method at runtime through transformation handler.
	 * @param systemTypeOrSystemDisplayName -system type name. In case of multiple systems of same type, system display name.
	 * @param fullName
	 * @param logErrorOnMultipleMatch
	 * @return UserName associated with fullName. Returns empty string in case of no match found. Throws exception incase of 
	 * multiple user found and logErrorOnMultipleMatch = "True"
	 * @throws OIMJiraAdapterException
	 */
	public static String getUserNameFromFullName(String processInstanceId, String systemTypeOrSystemDisplayName,String fullName,String logErrorOnMultipleMatch,String isCaseSensitiveStr) throws OIMJiraAdapterException {
		String userName = "";
		CommonConnectorLoader connectorLoader;
		boolean caseSensitive = Boolean.parseBoolean(isCaseSensitiveStr);
		List<String> userNames;
		try {
			connectorLoader = new CommonConnectorLoader(processInstanceId, systemTypeOrSystemDisplayName,true);
			OIMConnector jiraObject = connectorLoader.getOIMAdapter(null).getOimConnector();
			// either instance of JIRARestAdapter or of JiraAdapter
			if(jiraObject instanceof JIRARestAdapter){
				JIRARestAdapter objJiraRestAdapter = (JIRARestAdapter) jiraObject;
				userNames = objJiraRestAdapter.getUserNameFromFullName(fullName,caseSensitive);
			}else{
				JiraAdapter objJiraAdapter = (JiraAdapter) jiraObject;
				userNames = objJiraAdapter.getUserNameFromFullName(fullName,caseSensitive);
			}
		}catch (ConnectorLoaderException e) {
			throw new OIMJiraAdapterException("0060",null, e); 
		} catch (OIMAdapterException e) {
			throw new OIMJiraAdapterException("0060",null, e);
		}
		
		if(userNames.size()==0){
			return userName;
		}
		if(userNames.size()>1 && logErrorOnMultipleMatch.equalsIgnoreCase("True")){
			throw new OIMJiraAdapterException("0065", new String[] { fullName }, null);
		}else if(userNames.size()>0 || logErrorOnMultipleMatch.equalsIgnoreCase("False")){
			userName = userNames.get(0);
		}else{
			throw new OIMJiraAdapterException("0062", new String[]{"logErrorOnMultipleMatch",logErrorOnMultipleMatch}, null); 
		}
		return userName;
	}
	
	/**
	 * This method is created for internal use. It should not be used from any external source.
	 * This method returns version id from version name.
	 * @param versionNames
	 * @param projectId
	 * @return
	 * @throws OIMJiraAdapterException 
	 */
	public String obsoleteGetVersionIdsByNames(String processInstanceId, String systemTypeOrSystemDisplayName,String versionNames,String projectName) throws OIMJiraAdapterException 
	{
		if(versionNames==null || projectName==null) 
			return "";
		
		try {
			CommonConnectorLoader connectorLoader = new CommonConnectorLoader(processInstanceId, systemTypeOrSystemDisplayName,true);
			JiraAdapter jiraAdapter = (JiraAdapter)connectorLoader.getOIMAdapter(null).getOimConnector();
			String[] versionIds = jiraAdapter.obsoleteGetVersionIdsByNames(new String[]{versionNames}, projectName);
			if(versionIds!=null && versionIds.length>0){
				return versionIds[0];
			}else{
				return "";
			}
				
		} catch (ConnectorLoaderException e) {
			throw new OIMJiraAdapterException("0060",null, e); 
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
	public String obsoleteGetVersionNamesByIds(String processInstanceId, String systemTypeOrSystemDisplayName,String versionIds,String projectName) throws OIMJiraAdapterException 
	{
		if(versionIds==null || projectName==null) 
			return "";
		try {
			CommonConnectorLoader connectorLoader = new CommonConnectorLoader(processInstanceId, systemTypeOrSystemDisplayName,true);
			JiraAdapter jiraAdapter = (JiraAdapter)connectorLoader.getOIMAdapter(null).getOimConnector();
			String[] versionNames = jiraAdapter.obsoleteGetVersionNamesByIds(new String[]{versionIds}, projectName);
			if(versionNames!=null && versionNames.length>0){
				return versionNames[0];
			}else{
				return "";
			}
		} catch (ConnectorLoaderException e) {
			throw new OIMJiraAdapterException("0060",null, e); 
		}
	}

	/**
	 * method is used to find username from useremail while instantiate jira
	 * adapter. This method is for jira on-demand instance only.(Not used for
	 * on-premises).
	 * 
	 * @param jiraUserEmail
	 * @param jiraRestProcessor
	 * @return
	 * @throws OIMJiraAdapterException
	 */
	public static String fetchUserNameFromUserEmail(final String jiraUserEmail,
			final JIRARESTProcesser jiraRestProcessor) throws OIMJiraAdapterException {
		if (jiraUserEmail.matches(EMAIL_PATTERN)) {
			UserMeta userInfo = jiraRestProcessor.getSingleUserFromEmail(jiraUserEmail);
			return userInfo.getUserName();
		}else{
			throw new OIMJiraAdapterException("0270", new String[] { jiraUserEmail }, null);
		}
	}

	public static boolean sortAndCompareLists(final List<String> array1, final List<String> array2) {
		if (array1 != null) {
			Collections.sort(array1);
		}
		if (array2 != null) {
			Collections.sort(array2);
		}
		return array1.equals(array2);
	}
}
