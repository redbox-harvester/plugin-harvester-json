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
package au.com.redboxresearchdata.fascinator.jmx;

import java.util.List;
import java.util.Set;

public interface JsonHarvestQueueMXBean {

	/**
	 * Harvest an arbitrary text
	 */
	public void harvestJsonText(String text);
	
	/**
	 * Return a json array of failed harvest ids.
	 * 
	 */
	public String getFailedJsonList();
	
	/**
	 * Return the json text of the harvest id.
	 * 
	 */
	public String getFailedJsonText(String harvestId);
	
	/**
	 * Sets the json text of the harvest id.
	 * 
	 */
	public void setFailedJsonText(String harvestId, String json);

	/**
	 * Attempts a reharvest of the specified harvest id. 
	 */
	public void reharvestFailedJson(String harvestId);
	
	/**
	 * Removes the failed harvest id.
	 *  
	 * @param harvestId
	 */
	public void removeFailedJson(String harvestId);
	
	/**
	 * Returns json array of failed JSON harvest requests.
	 * @return
	 */
	public String getFailedRequests();
	
	/**
	 * Clears failed requests.
	 */
	public void clearFailedRequests();
	
}
