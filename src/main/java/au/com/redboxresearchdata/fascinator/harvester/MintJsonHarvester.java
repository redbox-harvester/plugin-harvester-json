/*
 * Copyright (C) 2013 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License along
 *   with this program; if not, write to the Free Software Foundation, Inc.,
 *   51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package au.com.redboxresearchdata.fascinator.harvester;

import au.com.redboxresearchdata.fascinator.harvester.service.MintHarvestEnum;
import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.messaging.MessagingException;
import com.googlecode.fascinator.common.messaging.MessagingServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Harvester for mint harvests. This is used to delegate to a rules config json file, e.g, services, languages, parties_people.
 * TODO : update javadocs and refactor methods into separate service for use by other harvesters.
 *
 * @author <a href="matt@redboxresearchdata.com.au">Matt Mulholland</a>
 * @version since 1.2-SNAPSHOT
 */
public class MintJsonHarvester extends BaseJsonHarvester {

    private static Logger log = LoggerFactory.getLogger(MintJsonHarvester.class);
    private static final String ID = "MintJson";
    private static final String NAME = "Mint Json harvester";

    public MintJsonHarvester() {
        super(ID, NAME);
    }

    @Override
    public void init() throws HarvesterException {
        try {
            messaging = MessagingServices.getInstance();
        } catch (MessagingException ex) {
            errorMessage = "Failed to start connection:" + ex.getMessage();
            log.error(errorMessage);
            throw new HarvesterException(ex);
        }
        try {
            harvestConfig = new JsonSimple(configFile);
        } catch (IOException e) {
            errorMessage = "IO Error loading main configuration file.";
            log.error(errorMessage);
            throw new HarvesterException(e);
        }
        log.info("Rules config not loaded - lazy loading expected.");
    }

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

    /**
     * Convenience method to start the harvest of objects.
     *
     * @param sourceJsonStr - JSON string to harvest - see the class comments for the
     *                      basic structure
     * @return List of Object Ids.
     * @throws HarvesterException
     */
    public List<HarvestItem> harvest(JsonSimple data, String type, String requestId)
            throws HarvesterException {
        JsonObject harvest = getHarvest();
        for (MintHarvestEnum mintHarvestEnum : MintHarvestEnum.values()) {
            mintHarvestEnum.updateHarvest(data, harvest);
        }
        setUpRules();
        return super.harvest(data, type, requestId);
    }

    private JsonObject getHarvest() throws HarvesterException {
        JsonObject harvest = this.harvestConfig.getObject("harvester");
        if (harvest instanceof JsonObject) {
            log.debug("Found harvester object...");
            return harvest;
        } else {
            throw new HarvesterException("Unable to find harvester config data. Cannot update config.");
        }

    }

}
