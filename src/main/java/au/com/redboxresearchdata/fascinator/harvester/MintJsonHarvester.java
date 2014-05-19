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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.messaging.MessagingException;
import com.googlecode.fascinator.common.messaging.MessagingServices;

public class MintJsonHarvester extends BaseJsonHarvester {

	/** Logging */
	private static Logger log = LoggerFactory.getLogger(MintJsonHarvester.class);
	private static final String HARVESTER_ID = "MintJson";
	private static final String HARVESTER_NAME = "Mint Json harvester";
	private static final String RULES_CONFIG_EXTENSION = ".json";
    
    public MintJsonHarvester() {
    	super(HARVESTER_ID, HARVESTER_NAME);
    }
    
    /** Gets called by init(File) and init(String) after setting the config, annoyingly, config is private. */
	@Override
	public void init() throws HarvesterException {
		try {
			messaging = MessagingServices.getInstance();
		} catch (MessagingException ex) {
			errorMessage = "Failed to start connection:" + ex.getMessage();
			log.error(errorMessage);
			throw new HarvesterException(ex);
		}		
		try {
			harvestConfig = new JsonSimple(configFile);
		} catch (IOException e) {
			errorMessage = "IO Error loading main configuration file.";
			log.error(errorMessage);
			throw new HarvesterException(e);
		}
		log.info("Rules config not loaded - lazy loading expected.");
	}
    
	@Override
	protected boolean isValidJson(JsonSimple json) {
		String idVal = json.getString(null, idField);
		boolean isValid = idVal != null && idVal.trim().length() > 0;
		log.debug("isValidJson:" + isValid);
		if (!isValid) {
			log.error("Invalid JSON:" + json.toString(true));
		}
		return isValid;
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
		//get rulesConfig out of data
		String rulesConfig = extractRulesConfig(data);
		updateRulesConfig(rulesConfig);	
		setUpRules();
		return super.harvest(data, type, requestId);
	}
	
	protected String extractRulesConfig(JsonSimple data) throws HarvesterException {
		String rulesConfig = StringUtils.EMPTY;
		List<Object> rulesConfigList = data.search("rulesConfig");
		if (rulesConfigList.size() > 0 && rulesConfigList.get(0) instanceof String) {
			log.warn("Only using first config rule found for entire message set.");
			rulesConfig = (String)rulesConfigList.get(0);
			log.debug("extracted rules config: " + rulesConfig);
		} else {
			throw new HarvesterException("No rules config found in data.");
		}
		return rulesConfig;
	}
	
	protected void updateRulesConfig(String rulesConfig) throws HarvesterException {
		JsonObject harvestConfig = this.harvestConfig.getJsonObject();
		Map<String, JsonSimple> mapConfig = JsonSimple.toJavaMap(harvestConfig);
		JsonObject harvestObject = this.harvestConfig.getObject("harvester");
		Object currentRulesConfig = null;
		if (harvestObject instanceof JsonObject) {
			log.debug("Found harvester object. Extracting rulesConfig...");
			currentRulesConfig = harvestObject.get("rulesConfig");
		} else {
			throw new HarvesterException("Unable to find harvester config data. Cannot update rules config.");
		}
		if (currentRulesConfig instanceof String) {
			log.debug("Found current rules config: " + currentRulesConfig);
			// current rules config path may have been updated - ensure only the path is used to append incoming rules config name
			String updatedRulesConfig = FilenameUtils.getFullPath((String)currentRulesConfig) + rulesConfig + RULES_CONFIG_EXTENSION;		
			log.debug("Updating rules config path: " + updatedRulesConfig + " in harvest object ...");
			harvestObject.put("rulesConfig", updatedRulesConfig);
		} else {
			throw new HarvesterException("Unable to find current config rules path in order to update rules config.");
		}
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

}
