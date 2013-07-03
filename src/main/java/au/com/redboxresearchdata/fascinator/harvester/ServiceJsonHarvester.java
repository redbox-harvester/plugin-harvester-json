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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.messaging.MessagingServices;

/**
 * 
 * Exemplar implementation of a JSON Harvester for Service Type.
 * 
 * @author Shilo Banihit
 *
 */
public class ServiceJsonHarvester extends GenericJsonHarvester {
	
    
    /** Logging */
    private Logger log = LoggerFactory.getLogger(MessagingServices.class);
	
	public ServiceJsonHarvester() {
		super("ServiceJson", "Services JSON Harvester");
	}

	/**
	 * Checks if JSON has non-null ID field as configured in the harvest config file. 
	 * 
	 */
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
}
