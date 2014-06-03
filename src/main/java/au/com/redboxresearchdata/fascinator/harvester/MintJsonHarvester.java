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

import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.messaging.MessagingException;
import com.googlecode.fascinator.common.messaging.MessagingServices;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class MintJsonHarvester extends BaseJsonHarvester {

    /**
     * Logging
     */
    private static Logger log = LoggerFactory.getLogger(MintJsonHarvester.class);
    private static final String ID = "MintJson";
    private static final String NAME = "Mint Json harvester";
    private static final String RULES_EXTENSION = ".json";
    private static final String RULES_KEY = "rulesConfig";
    private static final String ID_PREFIX_KEY = "recordIDPrefix";

    public MintJsonHarvester() {
        super(ID, NAME);
    }

    /**
     * Gets called by init(File) and init(String) after setting the config, annoyingly, config is private.
     */
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
        //get rulesConfig out of data
        String rulesConfig = extractStrictConfig(data, RULES_KEY);
        String recordIdInfix = extractLenientDefaultIfNullConfig(data, ID_PREFIX_KEY, RULES_KEY);
        JsonObject harvest = getHarvest();
        appendToFullPathOfHarvestKey(harvest, RULES_KEY, rulesConfig + RULES_EXTENSION);
        appendToPathPrefixOfHarvestKey(harvest, ID_PREFIX_KEY, recordIdInfix.toLowerCase());
        setUpRules();
        return super.harvest(data, type, requestId);
    }

    private String extractStrictConfig(JsonSimple data, String configKey) throws HarvesterException {
        String configValue = StringUtils.EMPTY;
        List<Object> configList = data.search(configKey);
        if (configList.size() > 0 && configList.get(0) instanceof String) {
            log.warn("Only using first config key: " + configKey + " found for entire message set.");
            configValue = (String) configList.get(0);
            log.debug("extracted config value: " + configValue);
        } else {
            throw new HarvesterException("No config key: " + configKey + " found in data.");
        }
        return configValue;
    }

    private String extractLenientDefaultIfNullConfig(JsonSimple data, String configKey, String configKeyDefault) {
        String configValue = extractLenientConfig(data, configKey);
        if (StringUtils.isBlank(configValue)) {
            configValue = extractLenientConfig(data, configKeyDefault);
        }
        return configValue;
    }

    private String extractLenientConfig(JsonSimple data, String configKey) {
        String configValue = StringUtils.EMPTY;
        try {
            configValue = extractStrictConfig(data, configKey);
        } catch (HarvesterException e) {
            log.warn("Returning empty string.");
        }
        return configValue;
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

    private void appendToFullPathOfHarvestKey(JsonObject harvest, String key, String appendage) throws HarvesterException {
        String currentValue = getHarvestKeyValue(harvest, key);
        // current rules config path may have been updated - ensure only the current path is used
        updateHarvestFileKey(harvest, key, FilenameUtils.getFullPath(currentValue), appendage);
    }

    private void appendToPathPrefixOfHarvestKey(JsonObject harvest, String key, String appendage) throws HarvesterException {
        String currentValue = getHarvestKeyValue(harvest, key);
        // current rules config path may have been updated - ensure only the first prefix of path is used
        updateHarvestFileKey(harvest, key, StringUtils.substringBefore(currentValue, "/"), appendage);
    }

    private String getHarvestKeyValue(JsonObject harvest, String key) throws HarvesterException {
        Object currentValue = harvest.get(key);
        if (currentValue instanceof String) {
            log.debug("Found current harvest config for:" + key + ", is :" + currentValue);
            return (String)currentValue;
        } else {
            throw new HarvesterException("Unable to find current config rules path in order to update rules config.");
        }
    }

    private void updateHarvestFileKey(JsonObject harvest, String key, String path, String base) {
        String value = FilenameUtils.concat(path, base + "/");
        log.debug("Updating: " + key + " to: " + value);
        harvest.put(key, value);
    }
}
