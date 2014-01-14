/*******************************************************************************
 * Copyright (C) 2013 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 ******************************************************************************/
package au.com.redboxresearchdata.fascinator.harvester;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.indexer.Indexer;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.harvester.impl.GenericHarvester;
import com.googlecode.fascinator.common.messaging.MessagingException;
import com.googlecode.fascinator.common.messaging.MessagingServices;
import com.googlecode.fascinator.common.storage.StorageUtils;

/**
 * Basic class that harvests JSON documents.
 *  
 * 
  { "type":"DocumentType", 
   	"data": {
   		"data":[
	   		 {
	   			"<id-field>":"<id>",
	   			"owner":"owner",
	   			"attachmentList" : ["attachment1data", "attachment2data"],
		        "customProperties" : ["customProperty1"],
		        "varMap" : {
		            "customProperty1" : "${fascinator.home}/packages/<oid>.attachment1ext"            
		        },
		        "attachmentDestination" : {
		            "attachment1data":["<oid>.attachment1ext","filename2","$customProperty1"], 
		            "attachment2data":["filename3"]
		        },
		        "attachment2data" : {
		            ... contents of attachment2 ...                    
		        },
		        "attachment1data": {
		        	... contents of attachment1...
		 		}
	 		}, 
	 		{
	            "<id-field>":"<id>",
	            "owner":"owner",
	            "command":"delete"                        
        	},
        	{
	          "<id-field>":"<id>",
	          "owner":"owner",
	          "command":"attach",
	          "attachmentList" : ["attachment1"],
	          "attachmentDestination" : {               
	              "attachment1":["attachment1.filename"]
	          },
	          "attachment1" : {
	              ... contents of attachment1 ....
	          }                    
	        }
 *  	]
 *  }
 * 
 * }
 * 
 * Sub-classes classes need to perform document validation.
 * 
 * @author Shilo Banihit
 * 
 */
public abstract class BaseJsonHarvester extends GenericHarvester {

	/** Logging */
	private static Logger log = LoggerFactory.getLogger(BaseJsonHarvester.class);
	
	/** Handling Types */
	public static final String HANDLING_TYPE_OVERWRITE = "overwrite",
						  	   HANDLING_TYPE_PARK = "park",
						       HANDLING_TYPE_IGNORE_IF_EXISTS = "ignore_if_exists";
	
	/** Commands */
	public static final String COMMAND_DELETE = "delete",
							   COMMAND_HARVEST = "harvest",
							   COMMAND_ATTACH = "attach";

	/** Default payload ID */
	protected static final String DEFAULT_PAYLOAD_ID = "harvestClient.json";

	protected String id, name, type;

	/** Storage instance that the Harvester will use to manage objects */
	protected Storage storage;
	
	/** Indexer*/
    protected Indexer indexer;

	/** JSON object to harvest - should be nulled after a successful harvest **/
	private JsonSimple data;

	/**
	 * List of objects to harvest. Will be cleared as soon as a new harvest is
	 * requested
	 **/
	protected List<HarvestItem> harvestList;

	/**
	 * List of successfully harvested objects, cleared as soon as a new harvest is requested.
	 */
	protected List<String> successOidList;

	/** The entire harvest list, cleared as soon as a new harvest is requested */
	protected List<HarvestItem> itemList;
	
	/** The identifier of the harvest request, cleared as soon as a new harvest is requested. */
	protected String harvestRequestId;

	/** Messaging services */
	protected MessagingServices messaging;

	protected File rulesConfigFile;
	
	protected File rulesFile;

	protected File configFile;

	protected DigitalObject rulesObject;

	protected DigitalObject rulesConfigObject;

	protected JsonSimple harvestConfig;
	
	private JsonSimple rulesConfig;

	protected String idField;

	protected String idPrefix;

	protected String mainPayloadId;

	protected boolean shouldPark;
	
	protected String handlingType;
	
	/** If this is ready to harvest. */
	protected boolean isReady;
	
	/** Error message */
	protected String errorMessage;
		
	public BaseJsonHarvester(String id, String name) {
		super(id, name);		
	}

	/** Gets called by init(File) and init(String) after setting the config, annoyingly, config is private. */
	@Override
	public void init() throws HarvesterException {
		try {
			messaging = MessagingServices.getInstance();			
			try {
				harvestConfig = new JsonSimple(configFile);
			} catch (IOException e) {
				errorMessage = "IO Error loading main configuration file.";
				throw new Exception(e);								
			}			
			String rulesConfigFilePath = harvestConfig.getString(null, "harvester", "rulesConfig");
			log.debug("Initialising Harvester, using config path:" + rulesConfigFilePath);
			rulesConfigFile = new File(rulesConfigFilePath);
			if (!rulesConfigFile.exists()) {
				errorMessage = "Rules config file not found: " + rulesConfigFilePath;
				throw new Exception(errorMessage);				
			}
			rulesConfig = new JsonSimple(rulesConfigFile);			
			String rulesFilePath = rulesConfig.getString("", "indexer", "script", "rules");
			log.debug("Initialising Harvester, checking if rulesFilePath exists:" + rulesFilePath);
			rulesFile = new File(rulesFilePath);
			if (!rulesFile.exists()) {
				// try again this time appending the base directory of the rules config path						
				rulesFilePath = FilenameUtils.getFullPath(rulesConfigFilePath) + rulesFilePath;
				log.debug("Initialising Harvester, nope wasn't there, trying if this exists:" + rulesFilePath);
				rulesFile = new File(rulesFilePath);
				if (!rulesFile.exists()) {
					errorMessage = "Rules file not found '"+rulesFilePath+"', please check the set up..."; 				
					throw new Exception(errorMessage);
				}
			}
			log.info("Using rules file path:" + rulesFilePath);
			handlingType = harvestConfig.getString(HANDLING_TYPE_OVERWRITE,  "harvester", "handlingType");
			idField = harvestConfig.getString("", "harvester", "idField");
			if (idField == null) {
				throw new HarvesterException(
						"harvester.idField is not defined in the harvest configuration.");
			}
			log.debug("idField is:" + idField);
			idPrefix = harvestConfig.getString("", "harvester",
					"recordIDPrefix");
			if (idPrefix == null) {
				throw new HarvesterException(
						"harvester.recordIDPrefix is not defined in the harvest configuration.");
			}
			mainPayloadId = harvestConfig.getString(DEFAULT_PAYLOAD_ID,
					"harvester", "mainPayloadId");
			rulesConfigObject = updateHarvestFile(rulesConfigFile);
			rulesObject = updateHarvestFile(rulesFile);			
		} catch (MessagingException ex) {
			errorMessage = "Failed to start connection:" + ex.getMessage();
			log.error(errorMessage);
			throw new HarvesterException(ex);
		} catch (Exception e) {
			log.error(errorMessage);
			throw new HarvesterException(e);
		}
		isReady = true;
	}
	
	@Override
	public void init(File configFile) throws PluginException {
		this.configFile = configFile;
		super.init(configFile);				
	}

	/**
	 * Sets the Storage instance that the Harvester will use to manage objects.
	 * 
	 * @param storage
	 *            a storage instance
	 */
	public void setStorage(Storage storage) {
		this.storage = storage;
	}

	/**
	 * Convenience method to start the harvest of objects.
	 * 
	 * @param sourceJsonStr
	 *            - JSON string to harvest - see the class comments for the
	 *            basic structure
	 * @return List of Object Ids.
	 * @throws HarvesterException
	 */
	public List<HarvestItem> harvest(JsonSimple data, String type, String requestId)
			throws HarvesterException {		
		this.data = data;
		this.type = type;
		this.harvestRequestId = requestId;
		return processHarvestList();
	}

	public List<HarvestItem> processHarvestList() throws HarvesterException {
		if (data != null) {
			buildHarvestList();
			for (HarvestItem item : harvestList) {
				processJson(item);
				successOidList.add(item.getOid());
			}
		}
		return harvestList;
	}

	/**
	 * Gets a list of digital object IDs successfully harvested.
	 * 
	 * If there are no objects, this method should return an empty list, not
	 * null.
	 * 
	 * @return a list of object IDs, possibly empty
	 * @throws HarvesterException
	 *             if there was an error retrieving the objects
	 */
	public Set<String> getObjectIdList() throws HarvesterException {
		HashSet<String> oidSet = new HashSet<String>();
		if (data != null && successOidList != null) {
			oidSet.addAll(successOidList);
		}
		return oidSet;
	}

	/**
	 * Builds the harvest list. Sub-classes should override this method if these
	 * expect more fields than what's outlined as 'basic'.
	 * 
	 */
	public void buildHarvestList() throws HarvesterException {
		harvestList = new ArrayList<HarvestItem>();
		itemList = new ArrayList<HarvestItem>();
		successOidList = new ArrayList<String>();
		JSONArray dataArray = data.getArray("data");
		if (dataArray == null) {
			log.debug("Data is not an array.");
			JsonSimple jsonObj = new JsonSimple(data.getObject("data"));
			log.debug("Data is: " + jsonObj.toString(true));
			addToHarvestList(jsonObj, harvestRequestId);
		} else {
			log.debug("Data is an array");
			for (JsonSimple jsonObj : JsonSimple.toJavaList(dataArray)) {
				addToHarvestList(jsonObj, harvestRequestId);
			}
		}
	}

	/**
	 * Validates the JSON document before adding to the harvest list.
	 * 
	 * @param jsonObj
	 */
	protected void addToHarvestList(JsonSimple jsonObj, String harvestRequestId) {
		HarvestItem item = new HarvestItem("", jsonObj, false, true, false);
		item.setHid(getHarvestItemId(jsonObj));
		item.setHrid(harvestRequestId);
		itemList.add(item);
		// validation is deferred to sub-classes
		if (isValidJson(jsonObj)) {
			harvestList.add(item);
		} else {
			item.setValid(false);
		}
	}

	/**
	 * Generic implementation is an MD5 hash of the type, id prefix and the id
	 * field, in that order.
	 * 
	 * @param jsonData
	 * @return Object ID string
	 */
	protected String getOid(JsonSimple jsonData) {
		return DigestUtils.md5Hex(type + ":" + idPrefix
				+ jsonData.getString("", idField));
	}

	/**
	 * Generic implementation is a type 4 UUID String
	 * 
	 * @param jsonData
	 * @return Object ID string
	 */
	protected String getHarvestItemId(JsonSimple jsonData) {
		return UUID.randomUUID().toString();
	}	

	/**
	 * Performs validation of JSON.
	 * 
	 * @param json
	 * @return true if JSON is valid
	 */
	protected abstract boolean isValidJson(JsonSimple json);

	/**
	 *  Processes JSON.
	 * 
	 *  Checks the 'command' field. Defaults to 'harvest' which is creating the DigitalObject, create attachments, etc.
	 * 
	 * @param item - Harvest Item	  
	 * @throws HarvesterException
	 */
	protected void processJson(HarvestItem item) throws HarvesterException {
		JsonSimple jsonData = (JsonSimple) item.getData();
		String oid = getOid(jsonData);
		// check the command
		String command = jsonData.getString(null, "command");
		if (command == null || COMMAND_HARVEST.equalsIgnoreCase(command)) {
			doHarvest(jsonData, oid, item);
		} else {
			if (COMMAND_DELETE.equalsIgnoreCase(command)) {
				doDelete(jsonData, oid, item);
			} else if (COMMAND_ATTACH.equalsIgnoreCase(command)) {
				doAttach(jsonData, oid, item);
			}
		}		
	}
	
	/**
	 * Harvests the incoming JSON document.
	 * 
	 * @param jsonData
	 * @param oid
	 * @param item
	 * @throws HarvesterException
	 */
	protected void doHarvest(JsonSimple jsonData, String oid, HarvestItem item) throws HarvesterException {
		// create metadata
		JsonObject meta = new JsonObject();
		meta.put("dc.identifier", idPrefix + jsonData.getString(null, idField));
		String handledAs = storeJsonInObject(
				jsonData, meta, oid, getPayloadId(mainPayloadId, oid), idPrefix);
		item.setOid(oid);
		item.setHandledAs(handledAs);
		if (HANDLING_TYPE_OVERWRITE.equalsIgnoreCase(handledAs)) {
			item.setShouldBeTransformed(true);									
		} 
		item.setHarvested(true);
		try {
			setObjectMetadata(oid, jsonData, meta, handledAs);
		} catch (StorageException e) {
			throw new HarvesterException(e);
		}
	}
	
	/**
	 * Deletes the object specified by the oid, also deletes all attachments outside of storage as specified in the original harvest request (i.e. mainPayloadId)
	 * 
	 * @param jsonData
	 * @param oid
	 * @param item
	 * @throws HarvesterException
	 */
	protected void doDelete(JsonSimple jsonData, String oid, HarvestItem item) throws HarvesterException {
		try {
			DigitalObject object = storage.getObject(oid);
			// delete the attachments specified in the original harvest request outside of storage, i.e. starts with "$"
			Payload payload = object.getPayload(getPayloadId(mainPayloadId, oid));
			JsonSimple origHarvestRequest = new JsonSimple(payload.open());
			JSONArray attachmentListArray = origHarvestRequest.getArray("attachmentList");
			if (attachmentListArray != null) {
				for (Object attachmentDataObj : attachmentListArray) {
					String attachmentDataName = attachmentDataObj.toString();					
					JSONArray destinationFileNameArray = origHarvestRequest.getArray("attachmentDestination", attachmentDataName);					
					for (Object destinationFileName : destinationFileNameArray) {
						String destinationFileNameStr = destinationFileName.toString(); 
						if (destinationFileNameStr.startsWith("$")) {
							// resolve the name and delete it from the FS
							String varfilename = getVar(origHarvestRequest, destinationFileNameStr, oid);				
							if (varfilename == null) { 	
								log.debug("addAttachment::Invalid variable substitution, ignoring:" + destinationFileNameStr);
							} else {
								FileUtils.deleteQuietly(new File(varfilename));
							} 				
						}
					}
				}
			}								
			for (String pid : object.getPayloadIdList()) {
				indexer.remove(oid, pid);
			}
			storage.removeObject(oid);
			item.setOid(oid);
			item.setHarvested(true);
		} catch (Exception e) {
			throw new HarvesterException(e);
		}
	}
	
	/**
	 * Attaches the data specified in the JSON document and triggers the transformation.
	 * 
	 * @param jsonData
	 * @param oid
	 * @param item
	 * @throws HarvesterException
	 */
	protected void doAttach(JsonSimple jsonData, String oid, HarvestItem item) throws HarvesterException {
		try {
			DigitalObject object = storage.getObject(oid);
			addAttachments(oid, object, jsonData, HANDLING_TYPE_OVERWRITE);
			item.setHarvested(true);
			item.setShouldBeTransformed(true); 
			item.setOid(oid);
		} catch (StorageException e) {
			throw new HarvesterException(e);
		}		
	}
	
	/**
	 * Returns the payload id, replacing all references to "<oid>" with the object's oid.
	 * 
	 * @param payloadId
	 * @param oid
	 * @return
	 */
	protected String getPayloadId(String payloadId, String oid) {
		return payloadId.replace("<oid>", oid);
	}

	/**
	 * Sets the object's metadata. 
	 * 
	 */	
	protected void setObjectMetadata(String oid, JsonSimple dataJson, JsonObject meta, String handledAs)
			throws HarvesterException, StorageException {
		// get the object
		DigitalObject object = storage.getObject(oid);

		if (object == null) {
			log.error("Object was not saved:" + oid);
			throw new HarvesterException("Digital object is not in storage.");
		}

		// update object metadata
		Properties props = object.getMetadata();
		if (props == null) {
			log.error("Object has no metadata:" + oid);
			throw new HarvesterException("Digital object has no metadata.");
		}
		
		if (HANDLING_TYPE_OVERWRITE.equalsIgnoreCase(handledAs)) {
			props.setProperty("objectId", object.getId());
			props.setProperty("scriptType",
					rulesConfig.getString(null, "indexer", "script", "type"));
			// Set our config and rules data as properties on the object
			props.setProperty("rulesOid", rulesObject.getId());
			props.setProperty("rulesPid", rulesObject.getSourceId());
			props.setProperty("jsonConfigOid", rulesConfigObject.getId());
			props.setProperty("jsonConfigPid", rulesConfigObject.getSourceId());

			JsonObject params = rulesConfig.getObject("indexer", "params");
			for (Object key : params.keySet()) {
				props.setProperty(key.toString(), params.get(key).toString());
			}
		} else {
			props.setProperty("parked", "true");
		}
		saveCustomObjectMetadata(oid, object, props, dataJson, handledAs);
		// done with the object
		object.close();
	}
	
	/**
	 * Child classes can optionally set their own custom metadata, after the main object metadata is set.
	 * 
	 * @param object
	 * @param metadata
	 * @param dataJson
	 * @param handledAs
	 */
	protected void saveCustomObjectMetadata(String oid, DigitalObject object, Properties metadata, JsonSimple dataJson, String handledAs) throws HarvesterException {
		
	}

	/**
	 * Get an individual uploaded file as a digital object: default
	 * implementation does not support this.
	 * 
	 * @return a list of one object ID
	 * @throws HarvesterException
	 *             if there was an error retrieving the objects
	 */
	public Set<String> getObjectId(File uploadedFile) throws HarvesterException {
		throw new HarvesterException("Processing uploaded files not supported");
	}

	/**
	 * Tests whether there are more objects to retrieve.
	 * 
	 * @return true if there are more objects to retrieve, false otherwise
	 */
	public boolean hasMoreObjects() {
		return data != null;
	}

	/**
	 * Tests whether there are more deleted objects to retrieve. This method
	 * should return true if called before getDeletedObjects.
	 * 
	 * Default implementation does not support deletion: always return false
	 * 
	 * @return true if there are more deleted objects to retrieve, false
	 *         otherwise
	 */
	public boolean hasMoreDeletedObjects() {
		return false;
	}
	
	/**
	 * Iterates over the document's "attachmentList" array and creates the attachment.
	 * 
	 * @param oid
	 * @param object
	 * @param jsonData
	 * @param handledAs
	 * @throws HarvesterException
	 */
	protected void addAttachments(String oid, DigitalObject object, JsonSimple jsonData, String handledAs) throws HarvesterException {		
		// creating attachments...
		JSONArray attachmentListArray = jsonData.getArray("attachmentList");
		if (attachmentListArray != null) {
			for (Object attachmentDataObj : attachmentListArray) {
				String attachmentDataName = attachmentDataObj.toString();
				JsonObject attachmentData = jsonData.getObject(attachmentDataName);
				JSONArray destinationFileNameArray = jsonData.getArray("attachmentDestination", attachmentDataName);
				log.debug("attachmentData:");
				log.debug(new JsonSimple(attachmentData).toString(true));
				for (Object destinationFileName : destinationFileNameArray) {
					addAttachment(oid, object, destinationFileName.toString(), attachmentData, handledAs, jsonData);
				}
			}
		} else {
			log.debug("No attachmentList specified.");
		}
	}
	
	/** 
	 * Attach JSON payloads to the object, replaces <oid> references, resolves the variable maps.
	 * 
	 * @param oid
	 * @param object
	 * @param filename
	 * @param contents
	 * @param handledAs
	 * @throws HarvesterException
	 */
	protected void addAttachment(String oid, DigitalObject object, String filename, JsonObject contents, String handledAs, JsonSimple jsonData) throws HarvesterException {
		try {
			if (HANDLING_TYPE_PARK.equalsIgnoreCase(handledAs)) {
				filename = filename + ".parked";
			}
			filename = filename.replace("<oid>", oid);
			if (filename.startsWith("$")) {
				String varfilename = getVar(jsonData, filename, oid);				
				if (varfilename == null) { 	
					log.debug("addAttachment::Invalid variable substitution, ignoring:" + filename);
				} else {
					// add a copy of the attachment...
					FileUtils.writeStringToFile(new File(varfilename), new JsonSimple(contents).toString(true), "UTF-8");
				} 				
			} else {
				// an attachment
				for (String pid : object.getPayloadIdList() ) {
					if (pid.equalsIgnoreCase(filename)) {
						object.updatePayload(filename, IOUtils.toInputStream(new JsonSimple(contents).toString(true), "UTF-8"));
						return;
					}
				}
				object.createStoredPayload(filename, IOUtils.toInputStream(new JsonSimple(contents).toString(true), "UTF-8"));
			}						
		} catch (Exception e) {
			throw new HarvesterException(e);
		}
	}
	
	/**
	 * Resolves the variable value from the "varMap" field in the document.
	 * 
	 * @param jsonData
	 * @param varName
	 * @param oid
	 * @return
	 */
	protected String getVar(JsonSimple jsonData, String varName, String oid) {
		String val = jsonData.getString(null, "varMap", varName.replace("$", ""));
		if (val != null) {
			val = val.replace("<oid>", oid);
		}
		return val;
	}

	/**
	 * Creates an object from the JSON document.
	 * 
	 * @param dataJson
	 * @param metaJson
	 * @param oid
	 * @param payloadId
	 * @param idPrefix
	 * @return
	 * @throws HarvesterException
	 */
	protected String storeJsonInObject(JsonSimple dataJson,
			JsonObject metaJson, String oid, String payloadId, String idPrefix)
			throws HarvesterException {
		// Does the object already exist?
		DigitalObject object = null;
		String handledAs = null;
		String renderPending = "true";
		log.debug("Current handling type is:" + handlingType);
		try {
			object = storage.getObject(oid);
			// no exception thrown, object exists, determine if we should
			// overwrite...			
			if (HANDLING_TYPE_PARK.equalsIgnoreCase(handlingType)) {
				log.debug("Parking incoming JSON.");
				handledAs = HANDLING_TYPE_PARK;
				String parkedPayloadId = payloadId + ".parked";
				storeJsonInPayload(dataJson.getJsonObject(), metaJson, object, parkedPayloadId,
						idPrefix);
				renderPending = "false";
			} else {
				log.debug("Overwriting with incoming JSON.");
				if (HANDLING_TYPE_OVERWRITE.equalsIgnoreCase(handlingType)) {
					// merge it, overwriting similar fields...
					storeJsonInPayload(dataJson.getJsonObject(), metaJson, object, payloadId,
							idPrefix);
				} else {
					handledAs = HANDLING_TYPE_IGNORE_IF_EXISTS;
					renderPending = "false";
				}
			}
		} catch (StorageException ex) {
			// @TODO: Programming by exception, find a better way, perhaps add
			// storage.objectExists()?
			// This is going to be brand new object
			log.debug("Brand new Object created with incoming JSON.");
			try {
				object = StorageUtils.getDigitalObject(storage, oid);
				storeJsonInPayload(dataJson.getJsonObject(), metaJson, object, payloadId,
						idPrefix);
				handledAs = HANDLING_TYPE_OVERWRITE;
			} catch (StorageException ex2) {
				throw new HarvesterException(
						"Error creating new digital object: ", ex2);
			}
		}				
		try {
			addAttachments(oid, object, dataJson, handledAs);
			object.getMetadata().setProperty("render-pending", renderPending);
			object.close();
		} catch (StorageException e) {
			throw new HarvesterException(
					"Error closing digital object: ", e);
		}
		return handledAs;
	}
	
	

	/**
	 * Store the processed data and metadata in a payload
	 * 
	 * @param dataJson
	 *            an instantiated JSON object containing data to store
	 * @param metaJson
	 *            an instantiated JSON object containing metadata to store
	 * @param object
	 *            the object to put our payload in
	 * @throws HarvesterException
	 *             if an error occurs
	 */
	protected void storeJsonInPayload(JsonObject dataJson, JsonObject metaJson,
			DigitalObject object, String payloadId, String idPrefix)
			throws HarvesterException {
		Payload payload = null;
		JsonSimple json = new JsonSimple();
		try {
			// New payloads
			payload = object.getPayload(payloadId);
			// log.debug("Updating existing payload: '{}' => '{}'",
			// object.getId(), payloadId);

			// Get the old JSON to merge
			try {
				json = new JsonSimple(payload.open());
			} catch (IOException ex) {
				log.error("Error parsing existing JSON: '{}' => '{}'",
						object.getId(), payloadId);
				throw new HarvesterException("Error parsing existing JSON: ",
						ex);
			} finally {
				payload.close();
			}

			// Update storage
			try {
				InputStream in = streamMergedJson(dataJson, metaJson, json,
						idPrefix);
				object.updatePayload(payloadId, in);

			} catch (IOException ex2) {
				throw new HarvesterException("Error processing JSON data: ",
						ex2);
			} catch (StorageException ex2) {
				throw new HarvesterException("Error updating payload: ", ex2);
			}
		} catch (StorageException ex) {
			// Create a new Payload
			try {
				// log.debug("Creating new payload: '{}' => '{}'",
				// object.getId(), payloadId);
				InputStream in = streamMergedJson(dataJson, metaJson, json,
						idPrefix);
				payload = object.createStoredPayload(payloadId, in);

			} catch (IOException ex2) {
				throw new HarvesterException("Error parsing JSON encoding: ",
						ex2);
			} catch (StorageException ex2) {
				throw new HarvesterException("Error creating new payload: ",
						ex2);
			}
		}
	}

	/**
	 * Merge the newly processed data with an (possible) existing data already
	 * present, also convert the completed JSON merge into a Stream for storage.
	 * 
	 * @param dataJson
	 *            an instantiated JSON object containing data to store
	 * @param metaJson
	 *            an instantiated JSON object containing metadata to store
	 * @param existing
	 *            an instantiated JsonSimple object with any existing data
	 * @throws IOException
	 *             if any character encoding issues effect the Stream
	 */
	protected InputStream streamMergedJson(JsonObject dataJson,
			JsonObject metaJson, JsonSimple existing, String idPrefix)
			throws IOException {
		// Overwrite and/or create only nodes we consider new data
		existing.getJsonObject().put("recordIDPrefix", idPrefix);
		JsonObject existingData = existing.writeObject("data");
		existingData.putAll(dataJson);
		JsonObject existingMeta = existing.writeObject("metadata");
		existingMeta.putAll(metaJson);

		// Turn into a stream to return
		String jsonString = existing.toString(true);
		return IOUtils.toInputStream(jsonString, "UTF-8");
	}

	/**
	 * Update the harvest file in storage if required
	 * 
	 * @param file
	 *            The harvest file to store
	 * @return DigitalObject The storage object with the file
	 * @throws StorageException
	 *             If storage failed
	 */
	protected DigitalObject updateHarvestFile(File file)
			throws StorageException {
		// Check the file in storage
		log.debug("Checking file in storage:" + file.getAbsolutePath());
		DigitalObject object = StorageUtils.checkHarvestFile(storage, file);
		// log.info("=== Check harvest file: '{}'=> '{}'", file.getName(),
		// object);
		if (object != null) {
			if (messaging != null) {
				// If we got an object back its new or updated
				JsonObject message = new JsonObject();
				message.put("type", "harvest-update");
				message.put("oid", object.getId());
				try {
					messaging.queueMessage("houseKeeping", message.toString());
				} catch (MessagingException ex) {
					log.error("Error sending message: ", ex);
				}
			} else {
				log.error("Messaging is not initialised.");
			}
		} else {
			// Otherwise grab the existing object
			String oid = StorageUtils.generateOid(file);
			object = StorageUtils.getDigitalObject(storage, oid);
			// log.info("=== Try again: '{}'=> '{}'", file.getName(), object);
		}
		return object;
	}

	protected JsonSimple getData() {
		return data;
	}

	protected void setData(JsonSimple data) {
		this.data = data;
	}

	public List<String> getSuccessOidList() {
		return successOidList;
	}

	public List<HarvestItem> getItemList() {
		return itemList;
	}

	public boolean isShouldPark() {
		return shouldPark;
	}

	public void setShouldPark(boolean shouldPark) {
		this.shouldPark = shouldPark;
	}

	public void setRulesFile(File rulesFile) {
		this.rulesFile = rulesFile;
	}

	public JsonSimple getHarvestConfig() {
		return harvestConfig;
	}

	public void setHarvestConfig(JsonSimple harvestConfig) {
		this.harvestConfig = harvestConfig;
	}

	public File getConfigFile() {
		return configFile;
	}

	public void setConfigFile(File configFile) {
		this.configFile = configFile;
	}

	public boolean isReady() {
		return isReady;
	}

	public void setReady(boolean isReady) {
		this.isReady = isReady;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public JsonSimple getRulesConfig() {
		return rulesConfig;
	}

	public void setRulesConfig(JsonSimple rulesConfig) {
		this.rulesConfig = rulesConfig;
	}
	
	public boolean getCommit() {
		return harvestConfig.getBoolean(false, "harvester", "commit");
	}

	public Indexer getIndexer() {
		return indexer;
	}

	public void setIndexer(Indexer indexer) {
		this.indexer = indexer;
	}
}
