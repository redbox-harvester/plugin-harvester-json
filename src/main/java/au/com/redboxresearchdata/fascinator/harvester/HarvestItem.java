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

	/** The harvest request id */
	protected String hid;
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
}
