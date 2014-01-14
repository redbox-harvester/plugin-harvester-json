/*******************************************************************************
 * Copyright (C) 2014 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
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

/**
 * A harvest request.
 * 
 * @author Shilo Banihit
 *
 */
public class HarvestRequest {

	private String requestId;
	private String harvesterId;
	private String hostName;
	private String hostIp;
	private String data;
	private long received;
	
	public HarvestRequest(String requestId, String harvesterId, String hostName, String hostIp, long received) {
		this.requestId = requestId;
		this.harvesterId = harvesterId;
		this.hostName = hostName;
		this.hostIp = hostIp;		
		this.received = received;
	}
	
	public String getRequestId() {
		return requestId;
	}
	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}
	public String getHarvesterId() {
		return harvesterId;
	}
	public void setHarvesterId(String harvesterId) {
		this.harvesterId = harvesterId;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getHostIp() {
		return hostIp;
	}
	public void setHostIp(String hostIp) {
		this.hostIp = hostIp;
	}
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
	public long getReceived() {
		return received;
	}
	public void setReceived(long received) {
		this.received = received;
	}
}
