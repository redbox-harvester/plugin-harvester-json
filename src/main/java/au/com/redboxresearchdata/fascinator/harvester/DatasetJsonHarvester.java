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

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;

/**
 * 
 * 	Harvests incoming JSON documents into Dataset records.
 * 
 *   
 * 
 * @author Shilo Banihit
 *
 */
public class DatasetJsonHarvester extends GenericJsonHarvester {
	public DatasetJsonHarvester() {
		super("DatasetJson", "Dataset JSON Harvester");
	}
	
	protected void setCustomObjectMetadata(String oid, DigitalObject object, Properties metadata, JsonSimple dataJson, String handledAs) throws HarvesterException {
		super.setCustomObjectMetadata(oid, object, metadata, dataJson, handledAs);
		if (HANDLING_TYPE_OVERWRITE.equalsIgnoreCase(handledAs)) {
			// create the workflow.metadata file
			JsonSimple workflowmeta = new JsonSimple();
			workflowmeta.getJsonObject().put("id", "dataset");
			workflowmeta.getJsonObject().put("step", "investigation");
			workflowmeta.getJsonObject().put("pageTitle", "Metadata Record");
			workflowmeta.getJsonObject().put("label", "Investigation");
			JsonObject formData = workflowmeta.writeObject("formData");
			formData.put("title", dataJson.getString("", "title"));
			formData.put("description", dataJson.getString("", "description"));
			try {
				object.createStoredPayload("workflow.metadata", IOUtils.toInputStream(workflowmeta.toString(true), "UTF-8"));				
			} catch (Exception e) {
				throw new HarvesterException(e);
			}
		}
	}
}
