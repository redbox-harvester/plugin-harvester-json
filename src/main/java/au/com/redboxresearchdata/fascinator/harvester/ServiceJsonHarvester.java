/**
 * 
 */
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
		boolean isValid = json.getString(null, idField) != null;
		log.debug("isValidJson:" + isValid);
		if (!isValid) {
			log.error("Invalid JSON:" + json.toString(true));
		}
		return isValid;
	}
}
