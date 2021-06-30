/**
 * Copyright C 2012 OpsHub, Inc. All rights reserved
 */
package com.opshub.eai.salesforce.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.log4j.Logger;

import com.opshub.eai.Constants;
import com.opshub.eai.EAIAttachment;
import com.opshub.eai.EAIComment;
import com.opshub.eai.EAIEntityRefrences;
import com.opshub.eai.EAIKeyValue;
import com.opshub.eai.EaiUtility;
import com.opshub.eai.OIMCriteriaStorageInformation;
import com.opshub.eai.OIMEntityObject;
import com.opshub.eai.core.carriers.MaxEntityCarrier;
import com.opshub.eai.core.carriers.NonHistoryBasedInfoCarrier;
import com.opshub.eai.core.exceptions.OIMAdapterException;
import com.opshub.eai.core.interfaces.HasLinkSupport;
import com.opshub.eai.core.interfaces.HasReverseLinkSupport;
import com.opshub.eai.core.interfaces.IAllComments;
import com.opshub.eai.core.interfaces.IAttachmentProcessor;
import com.opshub.eai.core.interfaces.ICommentSupport;
import com.opshub.eai.core.interfaces.ICommentTypes;
import com.opshub.eai.core.interfaces.ILinkSupport;
import com.opshub.eai.core.interfaces.IMetadataInfo;
import com.opshub.eai.core.interfaces.OHFieldMappingInterface;
import com.opshub.eai.core.interfaces.RulesVerifyInterface;
import com.opshub.eai.metadata.DataType;
import com.opshub.eai.metadata.FieldsMeta;
import com.opshub.eai.metadata.LinkTypeMeta;
import com.opshub.eai.metadata.interfaces.HasFieldsMeta;
import com.opshub.eai.salesforce.common.SalesForceChildRelationship;
import com.opshub.eai.salesforce.common.SalesForceEntityMetadata;
import com.opshub.eai.salesforce.common.SalesForceEntityType;
import com.opshub.eai.salesforce.common.SalesForceFieldMeta;
import com.opshub.eai.salesforce.common.SalesForceHistoryItem;
import com.opshub.eai.salesforce.common.SalesForcePickValue;
import com.opshub.eai.salesforce.exceptions.OIMSalesForceAPIException;
import com.opshub.eai.salesforce.exceptions.OIMSalesForceAdapterException;
import com.opshub.eai.salesforce.util.SalesForceConstants;
import com.opshub.eai.salesforce.util.SalesForceUtility;
import com.opshub.logging.OpsHubLoggingUtil;
import com.opshub.reconcile.common.EntityInformation;
import com.opshub.reconcile.common.LookUpQueryConfig;
import com.opshub.reconcile.core.Interface.ISystemEntityLookup;


/**
 * 
 * The class is used to read create update various object in Salesforce. Adapter Implementation for Salesforce system.
 * 
 *
 */
public class SalesForceAdapter extends SalesForceBaseAdapter implements HasFieldsMeta, IAttachmentProcessor,
		HasLinkSupport, RulesVerifyInterface, ILinkSupport, HasReverseLinkSupport, IAllComments, ICommentTypes,
		ISystemEntityLookup {

	/**
	 * Logger variable
	 */
	private static final Logger LOGGER = Logger.getLogger (SalesForceAdapter.class);
	/**
	 * last updated fieldname
	 */
	private String ohLastUpdateFieldName;
	/**
	 * saleforce API connector
	 */
	private SalesForceAPIConnector sfApiConnector;

	private SalesForceCommentImpl salesForceCommentImpl;

	/**
	 * Constructor
	 * @param integrationId integrationId
	 * @param integrationUser salesforce username
	 * @param userPassword password(This must be password + security token 
	 * @param entityType entity type
	 * @param loginUrl login ws URL
	 * @param systemId system Id
	 * @throws OIMAdapterException 
	 */
	public SalesForceAdapter(final Integer integrationId, final String integrationUser,final String userPassword,
			final String entityType, final String loginUrl, final Integer systemId, final String sfAPIVersion,
			final String version) throws OIMAdapterException {
		super(integrationId,integrationUser,userPassword,entityType,loginUrl,systemId,sfAPIVersion);
		/**
		 * Creating new API Connector object. This API Connector object will be
		 * responsible for making any api call to salesforce
		 */
		this.sfApiConnector = new SalesForceAPIConnector(loginUrl, integrationUser, userPassword, sfAPIVersion,
				entityType, version);
		setSFAPIConnector(sfApiConnector);
	}

	public SalesForceAPIConnector getSalesForceAPIConnector() {
		return this.sfApiConnector;
	}

	@Override
	public ICommentSupport getCommentImpl() {
		if (this.salesForceCommentImpl == null) {
			this.salesForceCommentImpl = new SalesForceCommentImpl(getSalesForceAPIConnector(), this);
		}
		return this.salesForceCommentImpl;
	}

	@Override
	public List<EAIKeyValue> getAllCommentTypes(final String entityType) {
		List<EAIKeyValue> commentTypes = new ArrayList<>();
		commentTypes.add(new EAIKeyValue(SalesForceConstants.COMMENT_PUBLIC, SalesForceConstants.COMMENT_PUBLIC));
		commentTypes.add(new EAIKeyValue(SalesForceConstants.COMMENT_PRIVATE, SalesForceConstants.COMMENT_PRIVATE));
		return commentTypes;
	}

	/**
	 * Returns property value for given property
	 */
	@Override
	public Object getPropertyValue(final String internalId, final String propertyName,
			final boolean isSystemProperty) throws OIMSalesForceAdapterException {
		return getPropertyValueAsString(internalId, propertyName, isSystemProperty);
	}


	/**
	 * Returns property value as string
	 */
	@Override
	public String getPropertyValueAsString(final String internalId,
			final String propertyName, final boolean isSystemProperty)
					throws OIMSalesForceAdapterException {
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add(propertyName);
		/* Getting entity data and then retrieving field value from that */
		Map<String, Object> entityData = sfApiConnector.getEntityData(getEntityType(), internalId,fieldNames);
		if(entityData != null)
			return String.valueOf(entityData.get(propertyName)!=null?entityData.get(propertyName):"");

		return SalesForceConstants.EMPTY_STRING;
	}

	/**
	 * This method will return entity data for given entity id
	 */
	@Override
	public List<EAIKeyValue> getEntityObject(final String internalId) throws OIMSalesForceAdapterException {
		/* Getting entity data */
		final Map<String, Object> entityData = sfApiConnector.getEntityData(internalId);
		if(entityData == null || entityData.isEmpty())
			return null;
		/* returning EAIKeyValue list for entity data map */
		return EaiUtility.convertMapToEAIKeyValue(entityData);
	}

	@Override
	public Iterator<NonHistoryBasedInfoCarrier> getEntityHistorySince(
			final Calendar afterTime, final Calendar maxTime, final String lastProcessedId,
			final String lastProcessedRevisionId, final String entityName,
			final String notUpdatedByUser, final Integer pageSize, final IMetadataInfo metadata,
 final boolean isCaseSensititve, final OIMCriteriaStorageInformation criteriaStorageInfo)
			throws OIMAdapterException {
		List<NonHistoryBasedInfoCarrier> changedEntities = sfApiConnector.getEntitiesChangedAfterTime(afterTime,
				maxTime, getMappedFields(), this.getSfMappingItems(), notUpdatedByUser);
		return changedEntities.iterator();
	}

	/**
	 * Execute given query 
	 */
	@Override
	public ArrayList<Map<String, Object>> executeQuery(final String query) throws OIMSalesForceAdapterException {
		/* executing given query */
		return sfApiConnector.executeQuery(query);
	}


	/**
	 * Execute given query filter and returns result
	 * @param queryCriteria query filter should be like status='assigned'
	 * @param afterTime time after which entities needs to be returned
	 * @return list 
	 * @throws OIMSalesForceAdapterException
	 */
	public List<String> executeQueryWithCriteria(final String queryCriteria, final Calendar afterTime, final Calendar maxTime) throws OIMSalesForceAdapterException {
		/* getting entity type information */
		SalesForceEntityType entityObj = sfApiConnector.getEntityTypeByName(getEntityType());
		/* creating query which would be executed */

		String selectFieldList="Id";
		String query = SalesForceUtility.constructQuery(selectFieldList, entityObj.getName(), afterTime, maxTime, SalesForceConstants.UPDATED_ON,queryCriteria);
		OpsHubLoggingUtil.debug(LOGGER, "executeQueryWithCriteria : query --> " + query, null);

		/* executing query and retrieving result*/ 
		ArrayList<Map<String, Object>> result = executeQuery(query);
		List<String> entities = new ArrayList<String>();

		/* Iterate through result and getting id from it */
		for(Map<String, Object> historyObject : result) {
			if(historyObject != null && !historyObject.isEmpty()) {
				entities.add(historyObject.get("Id")==null?null:historyObject.get("Id").toString());
			}
		}
		return entities;
	}


	/**
	 * Returns maximum updated time in system
	 */
	@Override
	public MaxEntityCarrier getMaxUpdateTime(final Calendar afterTime,
			final String afterRevisionId) throws OIMSalesForceAdapterException {
		String lastEntity = "-1";
		Calendar maxTime = null;
		/* getting entity type information */
		SalesForceEntityType entityObj = sfApiConnector.getEntityTypeByName(getEntityType());
		/* getting history */
		List<SalesForceHistoryItem> historyData = sfApiConnector.getHistory(entityObj, afterTime, null);

		if(historyData == null || historyData.size() == 0)
			return new MaxEntityCarrier("-1", afterTime, afterRevisionId);

		//iterate on list and get max time
		for (SalesForceHistoryItem historyItem : historyData) {
			// get creation and modification time
			Calendar createdCal = SalesForceUtility.getCalendarSFTime(historyItem.getUpdatedDate());
			/* comparing time*/
			if(maxTime == null || createdCal.getTimeInMillis() >= maxTime.getTimeInMillis()){
				maxTime = createdCal;
				lastEntity = historyItem.getEntityId();
			}
		}
		return new MaxEntityCarrier(lastEntity, maxTime, "0");	
	}

	/**
	 * Returns field map
	 */
	@Override
	public Map<String, Object> getFieldMap(String fieldName, final String value,
			final boolean isSystem) throws OIMSalesForceAdapterException {
		/* getting field meta data */
		SalesForceFieldMeta sfFieldMeta = getFieldMetaByLabel(getEntityType(), fieldName);
		if(sfFieldMeta != null) // if given field name is null, then getting it from metadata
			fieldName = sfFieldMeta.getFieldName();
		/* returning map */
		Map<String,Object> map = new HashMap<String, Object>();
		map.put(fieldName, value);
		return map;
	}



	/**
	 * Returns remote entity link
	 */
	@Override
	public String getRemoteEntityLink(final String id) throws OIMSalesForceAdapterException {
		/* returning remote entity link */
		return sfApiConnector.getRemoteEntityLink(id);
	}

	/**
	 * Returns system properties of given entity
	 */
	@Override
	public Map<String, Object> getSystemProperties(final String internalId,
			final Object entityObject) throws OIMSalesForceAdapterException {
		/* returning all properties */
		return sfApiConnector.getEntityData(internalId);
	}

	/**
	 * Returns custom properties of given entity
	 */
	@Override
	public Map<String, Object> getCustomProperties(final String internalId,
			final Object entityObject) throws OIMSalesForceAdapterException {
		/* returning all properties */
		return sfApiConnector.getEntityData(internalId);
	}

	/**
	 * Returns last updated field name
	 * 
	 * @return
	 */
	@Override
	public String getOHLastUpdateFieldName() throws OIMSalesForceAdapterException {
		if (ohLastUpdateFieldName == null) {
			List<FieldsMeta> fieldsMetaList = getFieldsMetadata();
			fieldsMetaList.forEach(var -> {
				if (Constants.OHLASTUPDATED.equals(var.getDisplayName())) {
					ohLastUpdateFieldName = var.getInternalName();
				}
			});
		}
		return ohLastUpdateFieldName;
	}

	/**
	 * Returns OIMEntityObject
	 */
	@Override
	public OIMEntityObject getOIMEntityObject(final String internalId,
			final String operationId, final String stepNumber) throws OIMSalesForceAdapterException {

		/* getting entity data */
		Map<String, Object> entityDetails = sfApiConnector.getEntityData(internalId);

		Calendar creationDate;
		String lastDate = entityDetails.containsKey(SalesForceConstants.UPDATED_ON)?SalesForceConstants.UPDATED_ON:SalesForceConstants.CREATED_ON;
		creationDate = SalesForceUtility.getCalendarSFTime((String) entityDetails.get(lastDate));
		/* returning oim entity object */
		return new OIMEntityObject(internalId, getEntityType(), getScopeId(internalId, new Object(), null),
				EaiUtility.convertMapToEAIKeyValueList(entityDetails), creationDate, true);
	}

	/**
	 * Returns field meta data.
	 */
	@Override
	public List<FieldsMeta> getFieldsMetadata() throws OIMSalesForceAdapterException {
		List<FieldsMeta> fieldsMetaData = new ArrayList<FieldsMeta>();
		/* getting entity meta data for given entity type */
		SalesForceEntityMetadata entityMetaData = sfApiConnector.getMetaData(getEntityType());
		List<SalesForceFieldMeta> sfFieldsMeta = entityMetaData.getFieldsMeta();

		List<String> fieldSupportedThroughLink = getEntitySupportingLinks(false);

		/* iterating through all of fields meta data */
		for(SalesForceFieldMeta sfFieldMeta : sfFieldsMeta) {
			FieldsMeta fieldMeta = new FieldsMeta();
			/* if it's containing pickup value list then its type of lookup */
			if(sfFieldMeta.getPickValues() != null && !sfFieldMeta.getPickValues().isEmpty())
				fieldMeta.setDataType(DataType.LOOKUP);
			else // if it'd pickup list is empty, then checking for it's data type
				fieldMeta.setDataType(getDataType(sfFieldMeta.getDataType(), sfFieldMeta.isHtmlFormatted(),sfFieldMeta.getReferenceTo()));

			fieldMeta.setLength(Integer.parseInt(sfFieldMeta.getLength()));
			fieldMeta.setMandatory(sfFieldMeta.isRequired());
			/* if it's multipickup list then setting multiselet to true */
			if(SalesForceConstants.SF_MULTISELECT.equals(sfFieldMeta.getDataType()))
				fieldMeta.setMultiSelect(true);
			else
				fieldMeta.setMultiSelect(false);
			fieldMeta.setSystemField(!sfFieldMeta.isCustom());
			/* Checking whether field is type of reference */
			if(SalesForceConstants.SF_REFERENCE.equalsIgnoreCase(sfFieldMeta.getDataType())) {
				if(sfFieldMeta.getReferenceTo() != null && !sfFieldMeta.getReferenceTo().isEmpty()) {
					List<String> referenceFieldNames = sfFieldMeta.getReferenceTo();
					if(referenceFieldNames != null && !referenceFieldNames.isEmpty()) {
						String referenceObjName =referenceFieldNames.get(0);
						if(!fieldSupportedThroughLink.contains(referenceObjName)) {
							FieldsMeta tmpFieldMeta = new FieldsMeta();

							tmpFieldMeta.setDataType(fieldMeta.getDataType());
							tmpFieldMeta.setLength(fieldMeta.getLength());
							tmpFieldMeta.setMandatory(fieldMeta.isMandatory());
							tmpFieldMeta.setMultiSelect(fieldMeta.isMultiSelect());
							tmpFieldMeta.setSystemField(fieldMeta.isSystemField());
							tmpFieldMeta.setReadOnly(fieldMeta.isReadOnly());


							/* if it's reference field, then displaying it's reference entity object name and name field of reference object with filed display name */
							if(referenceFieldNames.contains(SalesForceConstants.USER_ENTITY_TYPE)) {
								tmpFieldMeta.setDataType(DataType.USER);
							}	
							tmpFieldMeta.setDisplayName(sfFieldMeta.getLabel());
							tmpFieldMeta.setInternalName(sfFieldMeta.getFieldName());
							fieldsMetaData.add(tmpFieldMeta);
						}
					}
				}
			}
			else {
				fieldMeta.setDisplayName(sfFieldMeta.getLabel());
				fieldMeta.setInternalName(sfFieldMeta.getFieldName());
				fieldsMetaData.add(fieldMeta);
			}
		}

		return fieldsMetaData;
	}

	/**
	 * Returns all of values for given field
	 */
	@Override
	public List<String> getEnumerationsForField(final String fieldName)
			throws OIMSalesForceAdapterException {
		List<String> fieldValues = new ArrayList<String>();

		if(fieldName == null || "".equals(fieldName))
			return fieldValues;

		SalesForceEntityMetadata entityMetaData = sfApiConnector.getMetaData(getEntityType());
		List<SalesForceFieldMeta> sfFieldsMeta = entityMetaData.getFieldsMeta();
		/* iterating ang getting enumeration for fields metadata */
		for(SalesForceFieldMeta sfFieldMeta : sfFieldsMeta) {
			if(fieldName.equalsIgnoreCase(sfFieldMeta.getFieldName())) {
				List<SalesForcePickValue> pickValues = sfFieldMeta.getPickValues();
				for(SalesForcePickValue pickValue : pickValues) {
					fieldValues.add(pickValue.getValue());
				}

			}
		}
		return fieldValues;
	}

	/**
	 * Returns all of values for given field
	 */
	@Override
	public List<String> getEnumerationsForField(final String fieldName, final boolean refresh)
			throws OIMSalesForceAdapterException {
		/* if refresh is needed then refreshing the cached metadata */
		if(refresh)
			sfApiConnector.clearFieldMetaCache();
		
		return getEnumerationsForField(fieldName);
	}


	/**
	 * Returns true if attachments are supported
	 */
	@Override
	public boolean isAttachmentSupported(final boolean isSourceSystem)
			throws OIMSalesForceAdapterException {
		return sfApiConnector.isAttachmentSupported(getEntityType());
	}

	/**
	 * Returns true if relationships are supported
	 */
	@Override
	public boolean isRelationShipSupported(final boolean isSourceSystem)
			throws OIMSalesForceAdapterException {
		List<String> links = getSupportedLinkTypes(getEntityType(),isSourceSystem);
		return !(links == null || links.size() ==0);
			
	}

	/**
	 * Returns datatype for salesforce object
	 * @param sfDataType
	 * @return
	 */
	private DataType getDataType(final String sfDataType, final boolean isHtmlFormatted, final List<String> referenceTo) {
		if(sfDataType == null || "".equals(sfDataType))
			return null;
		/*checking it's data type and then returning actual DataType object */
		if(SalesForceConstants.SF_BOOLEAN.equalsIgnoreCase(sfDataType))
			return DataType.BOOLEAN;
		else if(SalesForceConstants.SF_DATE.equalsIgnoreCase(sfDataType) || SalesForceConstants.SF_TIME.equalsIgnoreCase(sfDataType)
				|| SalesForceConstants.SF_DATETIME.equalsIgnoreCase(sfDataType))
			return DataType.DATE_STRING;
		else if(SalesForceConstants.SF_REFERENCE.equalsIgnoreCase(sfDataType) && referenceTo.size()>0 && referenceTo.get(0).equals("User"))
			return DataType.EMAIL_AS_USER;
		else if(SalesForceConstants.SF_MULTISELECT.equalsIgnoreCase(sfDataType) || SalesForceConstants.SF_PICKLIST.equalsIgnoreCase(sfDataType))
			return DataType.LOOKUP;
		else if(SalesForceConstants.SF_TEXTAREA.equalsIgnoreCase(sfDataType) && isHtmlFormatted)
			return DataType.HTML;
		else
			return DataType.TEXT;
	}

	/**
	 * Returns all of supported link names
	 */
	@Override
	public List<String> getEntitySupportingLinks(final boolean isSourceSystem)
			throws OIMSalesForceAdapterException {
		HashMap<String,String> linkTypesMap = new HashMap<String, String>();
		// iterate on all child relations and get link types
		List<SalesForceChildRelationship> childRelations = sfApiConnector.getMetaData(getEntityType()).getChildRelationships();
		for(int entity=0;entity<childRelations.size();entity++){
			String childSObject = childRelations.get(entity).getChildSObject();
			// get properties for child sobject
			SalesForceEntityType entityDetails = sfApiConnector.getEntityTypes().get(childSObject);
			if(entityDetails == null)
				continue;
			// if associated entity is layoutable i.e. is an entity that can be seen on UI.
			if(entityDetails.isLayoutable())
				linkTypesMap.put(childRelations.get(entity).getChildSObject(),null);
		}
		return new ArrayList<String>(linkTypesMap.keySet());
	}

	/**
	 * Returns all of supported link types
	 */
	@Override
	public List<String> getSupportedLinkTypes(final String entityName, final boolean isSourceSystem) throws OIMSalesForceAdapterException {
		HashMap<String,String> linkTypesMap = new HashMap<String, String>();
		// iterate on all child relations and get link types
		List<SalesForceChildRelationship> childRelations = sfApiConnector.getMetaData(entityName).getChildRelationships();
		for(int entity=0;entity<childRelations.size();entity++){
			String childSObject = childRelations.get(entity).getChildSObject();
			// get properties for child sobject
			SalesForceEntityType entityDetails = sfApiConnector.getEntityTypes().get(childSObject);
			if(entityDetails == null)
				continue;
			// if associated entity is layoutable i.e. is an entity that can be seen on UI.
			if(entityDetails.isLayoutable()) {
				//Getting actual field label for this link
				SalesForceFieldMeta fieldMeta = sfApiConnector.getFieldMeta(entityDetails.getName(), childRelations.get(entity).getFieldName());
				if(fieldMeta!=null)
					linkTypesMap.put(fieldMeta.getLabel(),null);
			}
		}
		return new ArrayList<String>(linkTypesMap.keySet());
	}

	
	/**
	 * Returns mapped properties
	 */
	@Override
	public Map<String, String> getMappedPropertyName()
			throws OIMSalesForceAdapterException {
		Map<String, String> fieldMap =  new  HashMap<String, String>();
		fieldMap.put(OHFieldMappingInterface.OH_STATUS, "Status");
		return  fieldMap;
	}

	/**
	 * Returns all current link information
	 * @param internalId entity id
	 * @return all current links
	 * @throws OIMSalesForceAdapterException
	 */
	public List<EAIEntityRefrences> getAllCurrentLinks(final String internalId,final List<EAIKeyValue> entityObj) throws OIMSalesForceAdapterException {
		List<EAIEntityRefrences> allLinks = new ArrayList<EAIEntityRefrences>();		// get all supported link types
		 List<EAIKeyValue> entityObject;
		//if links not mapped do not called the getCurrentLink API call to improve performance
		
		if(this.getSfMappingItems()!=null && !this.getSfMappingItems().isLinkMapped()){
			return allLinks;
		}
		
		List<String> allLinkTypes = getSupportedLinkTypes(getActualEntityType(internalId, null, null), false);
		if(allLinkTypes == null)
			return allLinks;
		
		//get entity object
		if(entityObj == null)
			entityObject = getEntityObject(internalId);
		else 
			entityObject = entityObj;
		
		// iterate on link types and get EAIEntityRefrences
		for (String linkType : allLinkTypes) {
			allLinks.add(getCurrentLinks(internalId, entityObject, linkType, null));
		}
		return allLinks;
	}




	@Override
	public boolean isLinkHistorySupported() {
		return false;
	}


	@Override
	public boolean isLinkRemoveSupported() {
		return true;
	}


	@Override
	public List<LinkTypeMeta> getSupportedLinkTypesMeta(final String entityName,
			final boolean isSourceSystem) throws OIMSalesForceAdapterException {
		List<LinkTypeMeta> linksTypeMeta = new ArrayList<LinkTypeMeta>();
		// iterate on all child relations and get link types
		List<SalesForceChildRelationship> childRelations = sfApiConnector.getMetaData(entityName).getChildRelationships();
		for(int entity=0;entity<childRelations.size();entity++){
			String childSObject = childRelations.get(entity).getChildSObject();
			// get properties for child sobject
			SalesForceEntityType entityDetails = sfApiConnector.getEntityTypes().get(childSObject);
			if(entityDetails == null)
				continue;
			// if associated entity is layoutable i.e. is an entity that can be seen on UI.
			if(entityDetails.isLayoutable()) {
				//Getting actual field label for this link
				SalesForceFieldMeta fieldMeta = sfApiConnector.getFieldMeta(entityDetails.getName(), childRelations.get(entity).getFieldName());
				if(fieldMeta!=null)
					linksTypeMeta.add(new LinkTypeMeta(fieldMeta.getLabel(),null));
			}
		}
		return linksTypeMeta;
	}


	@Override
	public void removeLink(final String internalId, final String linkType,
			final String targetIssueID, final String linkEntityType,
			final Map<String, String> properties) throws OIMSalesForceAdapterException {
		// else get current entity linked to parent
		List<EAIKeyValue> entityDetails = getEntityObject(internalId);

		SalesForceFieldMeta fieldMetaForLink = getReferenceFieldMetaByLabel( linkType);

		if(fieldMetaForLink!= null) {
			String linkTypeFieldName = fieldMetaForLink.getFieldName();

			EAIKeyValue linkInfo = EaiUtility.getEAIKeyValue(entityDetails, linkTypeFieldName);
			if(linkInfo == null || linkInfo.getValue() == null)
				return;
			// if current entity is the same as the one that needs to be removed then
			String currentLinkedEntity = (String) linkInfo.getValue();
			if(currentLinkedEntity.equals(targetIssueID)){
				HashMap<String, Object> systemProperties = new HashMap<String, Object>();
				systemProperties.put(linkTypeFieldName, "");
				createOrUpdateEntity(internalId, systemProperties, null);
			}
		}
	}


	@Override
	public void addLink(final String internalId, final String linkType,
			final String targetIssueID, final String linkEntityType,
			final Map<String, String> properties) throws OIMSalesForceAdapterException {
		HashMap<String, Object> systemProperties = new HashMap<String, Object>();
		SalesForceFieldMeta fieldMetaForLink = getReferenceFieldMetaByLabel(linkType);
		if(fieldMetaForLink!=null){
			systemProperties.put(fieldMetaForLink.getFieldName(), targetIssueID);
			createOrUpdateEntity(internalId, systemProperties, null);
		}
		else
			throw new OIMSalesForceAdapterException("0001",new String[]{"adding link for " + internalId, "not found reference '" + linkType + "' field meta data"},null);
	}


	@Override
	public EAIEntityRefrences getCurrentLinks(final String internalId,
			Object entityObject, final String linkType,
			final Map<String, String> properties) throws OIMSalesForceAdapterException {
		EAIEntityRefrences entityRef = new EAIEntityRefrences(linkType);
		if(entityObject == null)
			entityObject = getEntityObject(internalId);

		SalesForceFieldMeta fieldMeta = getFieldMetaByLabel(getEntityType(), linkType);
		if(fieldMeta!=null) {
			String linkTypeFieldName = fieldMeta.getFieldName();

			EAIKeyValue linkInfo = EaiUtility.getEAIKeyValue((List<EAIKeyValue>) entityObject, linkTypeFieldName);
			if(linkInfo == null || linkInfo.getValue() == null)
				return entityRef;
			String currentLinkedEntity = (String) linkInfo.getValue();
			// get metadata for the internal id entity type
			List<SalesForceChildRelationship> childRelations = sfApiConnector.getMetaData(getEntityType()).getChildRelationships();
			for(int entity=0;entity<childRelations.size();entity++){

				String fieldName = childRelations.get(entity).getFieldName();

				// if field name matches link type then get entity type of linked entity
				if(fieldName !=null && fieldName.equalsIgnoreCase(linkTypeFieldName)){
					// get properties for child sobject
					String childSObject =childRelations.get(entity).getChildSObject();
					SalesForceEntityType entitymeta = sfApiConnector.getEntityTypes().get(childSObject);
					if(entitymeta == null)
						continue;
					// if associated entity is layoutable i.e. is an entity that can be seen on UI.
					if(entitymeta.isLayoutable())
						entityRef.addLinks(childSObject, getScopeId(internalId, null, null), "", currentLinkedEntity);
				}
			}
		}
		return entityRef;
	}


	@Override
	public String getLinkedEntityScopeId(final String linkEntityType,
			final Map<String, String> properties) throws OIMSalesForceAdapterException {
		return getScopeId(null, null, null);
	}


	@Override
	public EAIAttachment updateAttachment(final String internalId,
			final EAIAttachment attachment, final InputStream byteInputStream)
					throws OIMSalesForceAdapterException {
		/**
		 * In SF, attachment content can not be update from UI and hence
		 * throwing an error in case of updating attachment
		 */
		throw new OIMSalesForceAdapterException("0001", new String[]{"updating attachment", "attachment update is not supported"}, null);
	}

	/**
	 * method is used to delete an attachment from the system accouding to the
	 * internal id and attachment id
	 */
	@Override
	public void deleteAttachment(final String internalId,
			final EAIAttachment attachment) throws OIMSalesForceAdapterException {
		if (!attachment.getAttachmentURI().startsWith(SalesForceConstants.Version.DEFAULT)
				&& !attachment.getAttachmentURI().startsWith(SalesForceConstants.Version.LIGHTNING)) {
			sfApiConnector.getSalesforceGeneralAttachment().deleteAttachment(attachment.getAttachmentURI());
		} else {
			String[] attachmentURL = attachment.getAttachmentURI()
					.split(SalesForceConstants.AttachmentConstant.SEPRATOR);
			if (SalesForceConstants.Version.LIGHTNING.equalsIgnoreCase(attachmentURL[0])) {
				sfApiConnector.getSalesforceLightningAttachment().deleteAttachment(attachmentURL[1]);
			} else {
				sfApiConnector.getSalesforceGeneralAttachment().deleteAttachment(attachmentURL[1]);
			}
		}
	}	

	/*
	 * To delete all Entity of specific type
	 * 
	 * @see
	 * com.opshub.eai.core.adapters.OIMConnector#deleteEntites(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public void deleteEntites(final String projectName, final String entityType) throws OIMAdapterException {
		sfApiConnector.deleteAllEntites(entityType);
	}

	@Override
	public List<EAIAttachment> getCurrentAttachments(final String internalId,
			final Object entityObject) throws OIMSalesForceAdapterException {
		/**
		 * if attachments not mapped, do not call getAttachment API call to
		 * improve performace.
		 */
		List<EAIAttachment> generalAttachment = sfApiConnector.getSalesforceGeneralAttachment().getAttachments(internalId);
		List<EAIAttachment> lightningAttachment = sfApiConnector.getSalesforceLightningAttachment()
				.getAttachments(internalId);
		return ListUtils.union(generalAttachment, lightningAttachment);
	}

	@Override
	public InputStream getAttachmentInputStream(final String attachmentURI)
			throws OIMSalesForceAdapterException {
		if (!attachmentURI.startsWith(SalesForceConstants.Version.DEFAULT)
				&& !attachmentURI.startsWith(SalesForceConstants.Version.LIGHTNING)) {
			return this.sfApiConnector.getSalesforceGeneralAttachment().getAttachmentContent(attachmentURI);
		} else {
			String[] attachmentURLData = attachmentURI.split(SalesForceConstants.AttachmentConstant.SEPRATOR);
			String version = attachmentURLData[0];
			String attachmentId = attachmentURLData[1];
			if (SalesForceConstants.Version.LIGHTNING.equalsIgnoreCase(version) && attachmentId != null) {
				return this.sfApiConnector.getSalesforceLightningAttachment().getAttachmentContent(attachmentId);
			} else {
				return this.sfApiConnector.getSalesforceGeneralAttachment().getAttachmentContent(attachmentId);
			}
		}
	}


	@Override
	public EAIAttachment addAttachment(final String internalId,
			final InputStream byteInputStream, final String label, final String fileName,
			final String contentType, final String addedByUser, final Map<String, Object> additionalParameters)
					throws OIMSalesForceAdapterException {
		byte[] contentByte;
		try {
			contentByte = EaiUtility.getBytesFromInputStream(byteInputStream);
		} catch (IOException e) {
			throw new OIMSalesForceAdapterException("0001", new String[]{"adding attachment for entity " + internalId, e.getLocalizedMessage()}, e);
		}

		/* Salesforce needs content to be encode using Base64 encoding, so encoding actual data */
		String base64Str = EaiUtility.encodeBase64(contentByte);
		String addedbyUserId = getIntegrationUserId();
		if(addedByUser !=null && !addedByUser.isEmpty()){
			String userId = sfApiConnector.getUserIdFromUserName(addedByUser);
			if(!(userId == null || userId.isEmpty() || userId.equals("-1")))
				addedbyUserId = userId;
		}

		/* adding new attachment */
		String attachmentId = sfApiConnector.getSalesForceAttachment().addAttachment(internalId, fileName, fileName,
				contentType,
				base64Str, addedbyUserId);
		
		EAIAttachment attachment = new EAIAttachment();
		attachment.setAddedByUser(addedbyUserId);
		attachment.setAttachmentURI(attachmentId);
		attachment.setFileName(fileName);
		attachment.setLabel(fileName);
		Calendar lastUpdatedTime= Calendar.getInstance();
		attachment.setUpdateTimeStamp(lastUpdatedTime.getTimeInMillis());
		
		return attachment;
	}

	@Override
	protected OIMEntityObject createEntity(final String externalId, final Map<String, Object> systemProperties,
			final Map<String, Object> customProperties, final boolean isRetry)
			throws OIMSalesForceAdapterException {
		return createOrUpdateEntity("-1", systemProperties, customProperties);
	}

	@Override
	protected OIMEntityObject updateEntity(final String internalId, final Object entityObject,
			final Map<String, Object> systemProperties, final Map<String, Object> customProperties,
			final String externalIdValue, final boolean isRetry, final String operationId,
			final Integer subStep) throws OIMSalesForceAdapterException {
		return createOrUpdateEntity(internalId, systemProperties, customProperties);
	}

	/**
	 * Create or updated given entity
	 * 
	 * @param internalId
	 * @param systemProperties
	 * @param customProperties
	 * @return
	 * @throws OIMSalesForceAdapterException
	 */
	private OIMEntityObject createOrUpdateEntity(String internalId, final Map<String, Object> systemProperties,
			final Map<String, Object> customProperties) throws OIMSalesForceAdapterException {
		// if system properties is null and custom properties is null then throw error
		if (systemProperties == null && customProperties == null) {
			return OIMEntityObject.FALSE;
		}

		Map<String, Object> entityDetails = new HashMap<String, Object>();
		Map<String, Object> entityDetailFinalVal = new HashMap<String, Object>();
		boolean assignmentRuleHeaderApplicable = SalesForceUtility.isAssignmentRuleHeaderApplicable(getEntityType());
		OpsHubLoggingUtil.debug(LOGGER, "Assignment rule header applicable: " + assignmentRuleHeaderApplicable, null);
		
		String assignmentRuleHeader = null;
		if (assignmentRuleHeaderApplicable) {
			assignmentRuleHeader = getAssignmentRuleHeaderFromFieldSettings(super.getFieldSettings());
			OpsHubLoggingUtil.debug(LOGGER, "Assignment rule header value set: " + assignmentRuleHeader, null);
		}

		if (systemProperties != null) {
			entityDetails.putAll(systemProperties);
		}

		if (customProperties != null) {
			entityDetails.putAll(customProperties);
		}


		// checking for reference kind of fields
		if(entityDetails != null && !entityDetails.isEmpty()) {
			Iterator<String> fieldNameItr = entityDetails.keySet().iterator();

			while(fieldNameItr.hasNext()) {
				String fieldName = fieldNameItr.next();
				Object fieldValue = entityDetails.get(fieldName);

				// handling boolean kind of fields
				SalesForceFieldMeta fieldMeta = sfApiConnector.getFieldMeta(getEntityType(), fieldName);
				if(SalesForceConstants.SF_BOOLEAN.equalsIgnoreCase(fieldMeta.getDataType()) && fieldValue!=null) {
					EAIKeyValue entityValueFinal = getAndConvertValueforBoolean(fieldName, fieldValue);
					entityDetailFinalVal.put(entityValueFinal.getKey(), entityValueFinal.getValue());
				}
				// handling multiselect 
				else if(SalesForceConstants.SF_MULTISELECT.equalsIgnoreCase(fieldMeta.getDataType()) && fieldValue!=null) {
					EAIKeyValue entityValueFinal = getAndConvertMultiPickList(fieldName, fieldValue);
					entityDetailFinalVal.put(entityValueFinal.getKey(), entityValueFinal.getValue());
				}
				// handling user field
				else if(SalesForceConstants.SF_REFERENCE.equalsIgnoreCase(fieldMeta.getDataType()) && fieldMeta.getReferenceTo()!=null && !fieldMeta.getReferenceTo().isEmpty() && fieldMeta.getReferenceTo().contains(SalesForceConstants.USER_ENTITY_TYPE)){
					entityDetailFinalVal.put(fieldName, sfApiConnector.getAndConvertValueforUser(fieldName, fieldValue));
					}
				else
					entityDetailFinalVal.put(fieldName, fieldValue);
				}
			}


		// checking whether its a create or update operation
		if ("-1".equals(internalId)) {
			// for account, case or lead, call create entity with assignment rule header
			internalId = assignmentRuleHeaderApplicable
					? sfApiConnector.createEntity(getEntityType(), entityDetailFinalVal, assignmentRuleHeader)
					: sfApiConnector.createEntity(getEntityType(), entityDetailFinalVal);
		} else {
			if (assignmentRuleHeaderApplicable) {
				sfApiConnector.updateEntity(getEntityType(), internalId, entityDetailFinalVal, assignmentRuleHeader);
			} else {
				sfApiConnector.updateEntity(getEntityType(), internalId, entityDetailFinalVal);
			}
		}

		return getOIMEntityObject(internalId, null, null);

	}

	private String getAssignmentRuleHeaderFromFieldSettings(final Map<String, Object> fieldSettings) {
		String assignmentRuleHeader = null;
		if (MapUtils.isNotEmpty(fieldSettings)
				&& MapUtils.isNotEmpty((Map<String, Object>) fieldSettings.get(SalesForceConstants.OWNER_ID))) {
			Map<String, Object> ownerIdSettings = (Map<String, Object>) fieldSettings.get(SalesForceConstants.OWNER_ID);
			assignmentRuleHeader = (String) ownerIdSettings.get(SalesForceConstants.ASSIGNMENT_RULE_HEADER);
		}
		return assignmentRuleHeader != null ? assignmentRuleHeader : "";
	}

	/**
	 * Returns field meta by giving label name
	 * @param entityType entity type
	 * @param labelName label name
	 * @return meta data
	 * @throws OIMSalesForceAPIException
	 */
	private SalesForceFieldMeta getFieldMetaByLabel(final String entityType, final String labelName) throws OIMSalesForceAPIException {
		SalesForceEntityMetadata entityMeta = sfApiConnector.getMetaData(entityType);
		List<SalesForceFieldMeta> fieldsMetaData = entityMeta.getFieldsMeta();

		if(fieldsMetaData!=null && !fieldsMetaData.isEmpty()) {
			for(SalesForceFieldMeta tmpMeta : fieldsMetaData) {
				if(tmpMeta.getLabel().equalsIgnoreCase(labelName))
					return tmpMeta;
			}
		}

		return null;
	}

	
	/**
	 * Returns actual true/false for different kind of boolean fields
	 * @param fieldName field name
	 * @param fieldValue field value
	 * @return true/false
	 * @throws OIMSalesForceAdapterException
	 */
	private EAIKeyValue getAndConvertValueforBoolean(final String fieldName, final Object fieldValue) throws OIMSalesForceAdapterException {
		EAIKeyValue entityValueFinal = new EAIKeyValue();

		entityValueFinal.setKey(fieldName);

		if(SalesForceConstants.BOOLEAN_YES.equalsIgnoreCase(fieldValue.toString()))					
			entityValueFinal.setValue(true);
		else if(SalesForceConstants.BOOLEAN_NO.equalsIgnoreCase(fieldValue.toString()))
			entityValueFinal.setValue(false);
		else if(SalesForceConstants.BOOLEAN_1.equalsIgnoreCase(fieldValue.toString())) 
			entityValueFinal.setValue(true);
		else if(SalesForceConstants.BOOLEAN_0.equalsIgnoreCase(fieldValue.toString())) 
			entityValueFinal.setValue(false);
		else 
			entityValueFinal.setValue(fieldValue);
		return entityValueFinal;
	}


	/**
	 * Returns string array for multipicklist type field
	 * @param fieldName field name
	 * @param fieldValue field value
	 * @return string array
	 */
	private EAIKeyValue getAndConvertMultiPickList(final String fieldName, final Object fieldValue) {
		EAIKeyValue entityValueFinal = new EAIKeyValue();
		entityValueFinal.setKey(fieldName);
		if(fieldValue instanceof String []) {
			String [] multiValues = (String []) fieldValue;
			String actualValue = "";
			for(int i=0;i<multiValues.length;i++) {
				actualValue = actualValue + multiValues[i] + ";";
			}
			entityValueFinal.setValue(actualValue);
		}
		else if(fieldValue instanceof ArrayList<?>) {
			ArrayList<?> multiValues = (ArrayList<?>) fieldValue;
			String actualValue = "";
			if(multiValues != null) {
				for(int i=0;i<multiValues.size();i++) {
					actualValue = actualValue + multiValues.get(i) + ";";
				}
			}
			entityValueFinal.setValue(actualValue);
		}
		else
			entityValueFinal.setValue(fieldValue);

		return entityValueFinal;
	}

	/**
	 * Returns field meta data for given field label in given entity tye
	 * @param entityType entity type
	 * @param fieldLabel field label
	 * @return field meta data
	 * @throws OIMSalesForceAPIException 
	 */
	private SalesForceFieldMeta getReferenceFieldMetaByLabel( final String fieldLabel) throws OIMSalesForceAdapterException {
		SalesForceEntityMetadata entityMeta = sfApiConnector.getMetaData(getEntityType());
		List<SalesForceFieldMeta> fieldsMetaData = entityMeta.getFieldsMeta();

		if(fieldsMetaData!=null && !fieldsMetaData.isEmpty()) {
			for(SalesForceFieldMeta tmpMeta : fieldsMetaData) {
				if(SalesForceConstants.SF_REFERENCE.equalsIgnoreCase(tmpMeta.getDataType()) 
						&& tmpMeta.getLabel().equalsIgnoreCase(fieldLabel))
					return tmpMeta;
			}
		}

		return null;
	}

	@Override
	public void cleanup() {
		try{
			super.cleanup();
			sfApiConnector.cleanup();
		}catch(Exception e){
			OpsHubLoggingUtil.error(LOGGER, "Error occurred while cleaning up adapter", e);
		}
	}


	@Override
	public List<EAIComment> getAllComments(final String internalId) throws OIMAdapterException {
		return sfApiConnector.getComments(null, null, internalId, null);
	}

	@Override
	public List<EntityInformation> executeSystemQuery(final LookUpQueryConfig queryConfig) throws OIMAdapterException {
		String query = queryConfig.getQuery();
		List<String> response = executeQueryWithCriteria(query, null, null);
		List<EntityInformation> entities = new ArrayList<>();
		for (String entityId : response) {
			entities.add(new EntityInformation(entityId));
		}
		return entities;
	}

}

