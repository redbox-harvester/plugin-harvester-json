package au.com.redboxresearchdata.fascinator.mxbeans;

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
