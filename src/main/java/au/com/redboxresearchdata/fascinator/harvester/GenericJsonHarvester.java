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
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;

/**
 * 
 * A Generic JSON Harvester.
 * 
 * This implementation ensures a non-null "ID" field, and drops the "data" property to the main payload. 
 * 
 * This class is provided simply to bootstrap a trivial JSON harvester set up,
 * and is not intended to replace a type-specific set up where harvesting of several types are supported. 
 * Also, validation should occur at type-specific harvesters and NOT at the rules file.  Among others, a type-specific harvester validation ensures proper monitoring and administration.
 *   
 * 
 * @author Shilo Banihit
 *
 */
public class GenericJsonHarvester extends BaseJsonHarvester {

	/** Logging */
    private static Logger log = LoggerFactory.getLogger(GenericJsonHarvester.class);
    
    public GenericJsonHarvester() {
    	super("GenericJson", "Generic JSON Harvester");
    }
    
    public GenericJsonHarvester(String name, String description) {
    	super(name, description);
    }
    
	/**
	 * Ensures the incoming JSON has an none-empty ID and owner field.
	 * 
	 * @see au.com.redboxresearchdata.fascinator.harvester.BaseJsonHarvester#isValidJson(com.googlecode.fascinator.common.JsonSimple)
	 */
	@Override
	protected boolean isValidJson(JsonSimple json) {
		String idVal = json.getString(null, idField);
		boolean isValid = idVal != null && idVal.trim().length() > 0;
		String owner = json.getString(harvestConfig.getString("guest",  "default-owner"), "owner");		
		isValid = isValid && owner != null && owner.trim().length() > 0;		
		log.debug("isValidJson:" + isValid);
		if (!isValid) {
			log.error("Invalid JSON:" + json.toString(true));
		}
		return isValid;
	}

	/**
	 * Merge the newly processed data with an (possible) existing data already
	 * present, also convert the completed JSON merge into a Stream for storage.
	 * 
	 * This implementation just saves the data (except the owner property) to the main payload, ignoring prefix and metadata that came along the originalPath harvest message.
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
		existing.getJsonObject().putAll(dataJson);
		// remove the owner from the main payload		
		existing.getJsonObject().remove("owner");
		
		// Turn into a stream to return
		String jsonString = existing.toString(true);				
		return IOUtils.toInputStream(jsonString, "UTF-8");
	}
		
	/**
	 * Sets render-pending, owner and saves custom object metadata properties. 
	 * 
	 */
	@Override
	protected void saveCustomObjectMetadata(String oid, DigitalObject object, Properties metadata, JsonSimple dataJson, String handledAs) throws HarvesterException {
		if (HANDLING_TYPE_OVERWRITE.equalsIgnoreCase(handledAs)) {
			metadata.setProperty("render-pending", "true");
			metadata.setProperty("owner", dataJson.getString(harvestConfig.getString("guest",  "default-owner"), "owner")); // permissive
			JSONArray customPropArray = dataJson.getArray(null, "customProperties");
			if (customPropArray != null) {
				for (Object customPropObj : customPropArray) {
					String customPropVal = getVar(dataJson, customPropObj.toString(), oid);
					if (customPropVal != null) {
						metadata.setProperty(customPropObj.toString(), customPropVal);
					}
				}
			}
		}
	}
}
