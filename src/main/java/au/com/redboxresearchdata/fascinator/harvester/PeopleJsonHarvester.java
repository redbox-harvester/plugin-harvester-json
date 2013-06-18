package au.com.redboxresearchdata.fascinator.harvester;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.messaging.MessagingServices;

public class PeopleJsonHarvester extends GenericJsonHarvester {

	/** Logging */
    private Logger log = LoggerFactory.getLogger(MessagingServices.class);
    
    public PeopleJsonHarvester() {
    	super("PeopleJson", "People JSON Harvester");
    }
    
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
