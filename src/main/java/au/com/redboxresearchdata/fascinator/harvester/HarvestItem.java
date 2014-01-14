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

import java.beans.ConstructorProperties;

/**
 * Represents a harvest item. 
 * 
 * This was created primarily for monitoring and administration purposes.
 * 
 * @author Shilo Banihit
 *
 */
public class HarvestItem {

	/** The harvest item id*/
	protected String hid;
	
	/** The harvest request id */
	protected String hrid;
	/** 
	 * OID of the item, empty if not processed by harvester.
	 */
	protected String oid;
	
	/**
	 * The data to be harvested.
	 */
	protected Object data;
	
	/**
	 * If should be placed on the transformer queue after harvesting.
	 */
	protected boolean shouldBeTransformed;
	
	/**
	 * True if this item has passed validation.
	 */
	protected boolean valid;
	
	/**
	 * If this has been successfully harvested.
	 */
	private boolean harvested;
	
	private String handledAs;
	
	@ConstructorProperties({"oid", "data", "shouldBeTransformed", "valid", "harvested"})
	public HarvestItem(String oid, Object data, boolean shouldBeTransformed, boolean valid, boolean harvested) {
		this.oid = oid;
		this.data = data;
		this.shouldBeTransformed = shouldBeTransformed;
		this.valid = valid;
		this.harvested = harvested;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public boolean isShouldBeTransformed() {
		return shouldBeTransformed;
	}

	public void setShouldBeTransformed(boolean shouldBeTransformed) {
		this.shouldBeTransformed = shouldBeTransformed;
	}

	public String getOid() {
		return oid;
	}

	public void setOid(String oid) {
		this.oid = oid;
	}

	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}

	public boolean isHarvested() {
		return harvested;
	}

	public void setHarvested(boolean harvested) {
		this.harvested = harvested;
	}

	public String getHid() {
		return hid;
	}

	public void setHid(String hid) {
		this.hid = hid;
	}

	public String getHrid() {
		return hrid;
	}

	public void setHrid(String hrid) {
		this.hrid = hrid;
	}

	public String getHandledAs() {
		return handledAs;
	}

	public void setHandledAs(String handledAs) {
		this.handledAs = handledAs;
	}
}
