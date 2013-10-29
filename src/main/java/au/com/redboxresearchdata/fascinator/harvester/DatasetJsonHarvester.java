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
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.spring.ApplicationContextProvider;
import com.googlecode.fascinator.transformer.basicVersioning.ExtensionBasicVersioningTransformer;

/**
 * 
 * 	Harvests incoming JSON documents into Dataset records.
 * 
 * 
 * @author Shilo Banihit
 *
 */
public class DatasetJsonHarvester extends GenericJsonHarvester {
	private static Logger log = LoggerFactory
			.getLogger(DatasetJsonHarvester.class);
	
	public DatasetJsonHarvester() {
		super("DatasetJson", "Dataset JSON Harvester");
	}
	
	@Override
	protected void saveCustomObjectMetadata(String oid, DigitalObject object, Properties metadata, JsonSimple dataJson, String handledAs) throws HarvesterException {
		super.saveCustomObjectMetadata(oid, object, metadata, dataJson, handledAs);
		log.debug("Dataset harvester, handling as:" + handledAs);
		if (HANDLING_TYPE_PARK.equalsIgnoreCase(handledAs)) {
			log.debug("Creating a version for the parked data...");
			// since we don't want to put the object on the toolchain, we trigger the transformer to version to parked payload			
			try {
				ApplicationContext context = ApplicationContextProvider.getApplicationContext();
				if (context != null) {
					ExtensionBasicVersioningTransformer versioningTransformer = (ExtensionBasicVersioningTransformer) context.getBean("extensionBasicVersioningTransformer", ExtensionBasicVersioningTransformer.class);
					if (versioningTransformer != null) {
						versioningTransformer.transform(object);
					} else {
						log.error("Cannot get instance of versioning transformer");
					}
				} else {
					log.error("No context available.");
				}
			} catch (Exception e) {
				throw new HarvesterException(e);
			}
			log.debug("Done, creating a version for the parked data...");
		}
	}
}
