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
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.PluginDescription;
import com.googlecode.fascinator.api.harvester.Harvester;
import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.googlecode.fascinator.common.harvester.impl.GenericHarvester;
import com.googlecode.fascinator.common.messaging.MessagingException;
import com.googlecode.fascinator.common.messaging.MessagingServices;
import com.googlecode.fascinator.common.storage.StorageUtils;

/**
 *  Basic class that harvests JSONSimple documents. 
 *  
 *  The basic JSON Structure should be:
 *  
 *  {
 *  	"type":"DocumentType",
 *  	"data":JSON object/array to harvest.
 *  }
 * 
 *  Sub-classes classes need to perform document validation. 
 *  
 * @author Shilo Banihit
 *
 */
public abstract class GenericJsonHarvester extends GenericHarvester {

	/** Logging */
    private Logger log = LoggerFactory.getLogger(MessagingServices.class);
    
    /** Default payload ID */
    protected static final String DEFAULT_PAYLOAD_ID = "metadata.json";
    
	protected String id, name, type;
	
	/** Storage instance that the Harvester will use to manage objects */
    protected Storage storage;
    
    /** JSON object to harvest - should be nulled after a successful harvest **/
    private JsonSimple data;
    
    /** List of objects to harvest. Will be cleared as soon as a new harvest is requested **/
    protected List<HarvestItem> harvestList;
    
    private List<String> successOidList;
    
    /** The entire harvest list */
    private List<HarvestItem> itemList;
    
    /** Messaging services */
    protected MessagingServices messaging;
    
    protected File rulesFile;
    
    protected File configFile;
    
    protected DigitalObject rulesObject;
    
    protected DigitalObject configObject;
    
    protected JsonSimple harvestConfig;

	protected String idField;

	protected String idPrefix;

	protected String mainPayloadId;
	
	protected boolean shouldPark;
    
    public GenericJsonHarvester(String id, String name) {
		super(id, name);
	}
    
    @Override
    public void init() throws HarvesterException {
		try {
            messaging = MessagingServices.getInstance();
        } catch (MessagingException ex) {
            log.error("Failed to start connection: {}", ex.getMessage());
            throw new HarvesterException(ex);
        }
	}
    
	/**
     * Sets the Storage instance that the Harvester will use to manage objects.
     * 
     * @param storage a storage instance
     */
    public void setStorage(Storage storage) {
    	this.storage = storage;
    }

    /**
     * Convenience method to start the harvest of objects.
     * 
     * @param sourceJsonStr - JSON string to harvest - see the class comments for the basic structure 
     * @return List of Object Ids.
     * @throws HarvesterException 
     */
    public List<HarvestItem> harvest(JsonSimple data, String type, File configFile, File rulesFile) throws HarvesterException{
    	try {
			this.data = data;
			this.configFile = configFile;
			this.rulesFile = rulesFile;
			harvestConfig = new JsonSimple(configFile);
			idField = harvestConfig.getString("", "harvester", "idField");
			if (idField == null) {
				throw new HarvesterException("harvester.idField is not defined in the harvest configuration.");
			}
			idPrefix = harvestConfig.getString("", "harvester", "recordIDPrefix");
			if (idPrefix == null) {
				throw new HarvesterException("harvester.recordIDPrefix is not defined in the harvest configuration.");
			}
			mainPayloadId = harvestConfig.getString(DEFAULT_PAYLOAD_ID, "harvester", "payloadId");
			configObject = updateHarvestFile(configFile);
			rulesObject = updateHarvestFile(rulesFile);
		} catch (StorageException e) {
			throw new HarvesterException(e);
		} catch (IOException e) {
			throw new HarvesterException(e);
		}
    	return processHarvestList();
    }
    
    public List<HarvestItem> processHarvestList() throws HarvesterException {
    	if (data!=null) {
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
     *  If there are no objects, this method should return an empty list, not null.
     * 
     * @return a list of object IDs, possibly empty
     * @throws HarvesterException if there was an error retrieving the objects
     */
    public Set<String> getObjectIdList() throws HarvesterException {
    	HashSet<String> oidSet= new HashSet<String>();
    	if (data != null && successOidList != null) {
    		oidSet.addAll(successOidList);
    	}
    	return oidSet;
    }
    
    /**
     * Builds the harvest list. Sub-classes should override this method if these expect more fields than what's outlined as 'basic'.
     * 
     */
    public void buildHarvestList() throws HarvesterException {
      	type = data.getString(null, "type");
		harvestList = new ArrayList<HarvestItem>();
		itemList = new ArrayList<HarvestItem>();
		successOidList = new ArrayList<String>();
		JSONArray dataArray = data.getArray("data");
		if (dataArray == null) { 
			log.debug("Data is not an array.");
			addToHarvestList(new JsonSimple(data.getObject("data")));
		} else {
			log.debug("Data is an array");
			for (JsonSimple jsonObj : JsonSimple.toJavaList(dataArray)) {
				addToHarvestList(jsonObj);
			}
		}
    }
    
    protected void addToHarvestList(JsonSimple jsonObj) {
    	HarvestItem item = new HarvestItem("", jsonObj, false, true, false);
    	item.setHid(getHarvestId(jsonObj));
    	itemList.add(item);
    	// validation is deferred to sub-classes
		if (isValidJson(jsonObj)) {
			harvestList.add(item);
		} else {
			item.setValid(false);
		}
    }
    /**
     * Generic implementation is a combination of type, id prefix and the id field, in that order.
     * 
     * @param jsonData
     * @return Object ID string
     */
    protected String getOid(JsonSimple jsonData) {
    	return DigestUtils.md5Hex(type+  ":" + idPrefix + jsonData.getString("", idField));
    }
    
    /**
     * Generic implementation is a combination of type, the entire json.
     * 
     * @param jsonData
     * @return Object ID string
     */
    protected String getHarvestId(JsonSimple jsonData) {
    	return DigestUtils.md5Hex(type+  ":" + jsonData.toString() + ":" + System.currentTimeMillis());
    }
    
    /**
     * Performs validation of JSON.
     * 
     * @param json
     * @return true if JSON is valid
     */
    protected abstract boolean isValidJson(JsonSimple json);
    
    /**
     * Processes JSON, creating the DigitalObject, create attachments, etc. 
     *  
     * @param json
     * @return Object Identifier
     * @throws Exception 
     */
    protected void processJson(HarvestItem item) throws HarvesterException {
    	JsonSimple jsonData = (JsonSimple) item.getData();
    	String oid = getOid(jsonData);
    	// create metadata
        JsonObject meta = new JsonObject();
        meta.put("dc.identifier", idPrefix + jsonData.getString(null, idField));
    	boolean wasStoredInMainPayload = storeJsonInObject(jsonData.getJsonObject(), meta, oid, mainPayloadId, idPrefix);
    	item.setShouldBeTransformed(wasStoredInMainPayload);
    	item.setOid(oid);
    	try {
			processObject(oid, wasStoredInMainPayload);
			item.setHarvested(true);
		} catch (StorageException e) {
			throw new HarvesterException(e);
		}
    }
    
    protected void processObject(String oid, boolean wasStoredInMainPayload) throws HarvesterException, StorageException {
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
        if (wasStoredInMainPayload) {
	        // TODO - objectId is redundant now?
	        props.setProperty("objectId", object.getId());
	        props.setProperty("scriptType", harvestConfig.getString(null,
	                "indexer", "script", "type"));
	        // Set our config and rules data as properties on the object
	        props.setProperty("rulesOid", rulesObject.getId());
	        props.setProperty("rulesPid", rulesObject.getSourceId());
	        props.setProperty("jsonConfigOid", configObject.getId());
	        props.setProperty("jsonConfigPid", configObject.getSourceId());
	        
	        JsonObject params = harvestConfig.getObject("indexer", "params");
	        for (Object key : params.keySet()) {
	            props.setProperty(key.toString(), params.get(key).toString());
	        }
        } else {
        	props.setProperty("parked", "true");
        }

        // done with the object
        object.close();
    }

    /**
     * Get an individual uploaded file as a digital object: default implementation does not support this.
     * 
     * @return a list of one object ID
     * @throws HarvesterException if there was an error retrieving the objects
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
	
	protected boolean storeJsonInObject(JsonObject dataJson, JsonObject metaJson,
            String oid, String payloadId, String idPrefix) throws HarvesterException {
		// Does the object already exist?
        DigitalObject object = null;
        boolean wasStoredInMainPayload = true;
        try {
            object = storage.getObject(oid);
            // no exception thrown, object exists, determine if we should overwrite...
            if (shouldPark) {
            	wasStoredInMainPayload = false;
            	String parkedPayloadId = "parked-metadata.json";
            	storeJsonInPayload(dataJson, metaJson, object, parkedPayloadId, idPrefix);
            } else {
            	// merge it, overwriting similar fields...
            	storeJsonInPayload(dataJson, metaJson, object, payloadId, idPrefix);
            }
        } catch (StorageException ex) {
        	//@TODO: Programming by exception, find a better way, perhaps add storage.objectExists()?
            // This is going to be brand new
            try {
                object = StorageUtils.getDigitalObject(storage, oid);
                storeJsonInPayload(dataJson, metaJson, object, payloadId, idPrefix);
            } catch (StorageException ex2) {
            	throw new HarvesterException("Error creating new digital object: ", ex2);
            }
        }
        // Set the pending flag
        if (wasStoredInMainPayload && object != null) {
            try {
                object.getMetadata().setProperty("render-pending", "true");
                object.close();
            } catch (Exception ex) {
                log.error("Error setting 'render-pending' flag: ", ex);
            }
        }
        return wasStoredInMainPayload;
	}	
	/**
     * Store the processed data and metadata in a payload
     *
     * @param dataJson an instantiated JSON object containing data to store
     * @param metaJson an instantiated JSON object containing metadata to store
     * @param object the object to put our payload in
     * @throws HarvesterException if an error occurs
     */
    protected void storeJsonInPayload(JsonObject dataJson, JsonObject metaJson,
            DigitalObject object, String payloadId, String idPrefix) throws HarvesterException {
    	Payload payload = null;
        JsonSimple json = new JsonSimple();
        try {
            // New payloads
            payload = object.getPayload(payloadId);
            //log.debug("Updating existing payload: '{}' => '{}'",
            //        object.getId(), payloadId);

            // Get the old JSON to merge
            try {
                json = new JsonSimple(payload.open());
            } catch (IOException ex) {
                log.error("Error parsing existing JSON: '{}' => '{}'",
                    object.getId(), payloadId);
                throw new HarvesterException(
                        "Error parsing existing JSON: ", ex);
            } finally {
                payload.close();
            }

            // Update storage
            try {
                InputStream in = streamMergedJson(dataJson, metaJson, json, idPrefix);
                object.updatePayload(payloadId, in);

            } catch (IOException ex2) {
            	throw new HarvesterException("Error processing JSON data: ", ex2);
            } catch (StorageException ex2) {
                throw new HarvesterException("Error updating payload: ", ex2);
            }
        } catch (StorageException ex) {
            // Create a new Payload
            try {
                //log.debug("Creating new payload: '{}' => '{}'",
                //        object.getId(), payloadId);
                InputStream in = streamMergedJson(dataJson, metaJson, json, idPrefix);
                payload = object.createStoredPayload(payloadId, in);

            } catch (IOException ex2) {
                throw new HarvesterException("Error parsing JSON encoding: ", ex2);
            } catch (StorageException ex2) {
                throw new HarvesterException("Error creating new payload: ", ex2);
            }
        }
    }
    /**
     * Merge the newly processed data with an (possible) existing data already
     * present, also convert the completed JSON merge into a Stream for storage.
     *
     * @param dataJson an instantiated JSON object containing data to store
     * @param metaJson an instantiated JSON object containing metadata to store
     * @param existing an instantiated JsonSimple object with any existing data
     * @throws IOException if any character encoding issues effect the Stream
     */
    protected InputStream streamMergedJson(JsonObject dataJson,
            JsonObject metaJson, JsonSimple existing, String idPrefix) throws IOException {
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
     * @param file The harvest file to store
     * @return DigitalObject The storage object with the file
     * @throws StorageException If storage failed
     */
    protected DigitalObject updateHarvestFile(File file) throws StorageException {
        // Check the file in storage
    	log.debug("Checking file in storage:" + file.getAbsolutePath());
        DigitalObject object = StorageUtils.checkHarvestFile(storage, file);
        //log.info("=== Check harvest file: '{}'=> '{}'", file.getName(), object);
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
            //log.info("=== Try again: '{}'=> '{}'", file.getName(), object);
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
}
