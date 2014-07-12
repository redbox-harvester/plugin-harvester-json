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

package au.com.redboxresearchdata.fascinator.harvester.service

import com.googlecode.fascinator.api.harvester.HarvesterException
import com.googlecode.fascinator.common.JsonObject
import groovy.util.logging.Log4j
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang.StringUtils

/**
 * @version
 * @author <a href="matt@redboxresearchdata.com.au">Matt Mulholland</a>
 */
@Log4j
class HarvestConfigUpdateService {

    def appendToFullPathOfHarvestKeyValue = {JsonObject harvest, String key, String appendage->
        String currentValue = getHarvestKeyValue(harvest, key);
        // current rules config path may have been updated - ensure only the current path is used
        String filePath = FilenameUtils.getFullPath(currentValue)
        String harvestKeyValue = FilenameUtils.concat(filePath, appendage);
        update(harvest, key, harvestKeyValue);
    }

    def appendToPathPrefixOfHarvestKeyValue = {JsonObject harvest, String key, String appendage->
        String currentValue = getHarvestKeyValue(harvest, key);
        // current rules config path may have been updated - ensure only the first prefix of path is used
        String filePrefix = StringUtils.substringBefore(currentValue, "/")
        String baseDirectory = appendage + "/"
        String harvestKeyValue = FilenameUtils.concat(filePrefix, baseDirectory);
        update(harvest, key, harvestKeyValue);
    }

    String getHarvestKeyValue(JsonObject harvest, String key) {
        Object currentValue = harvest.get(key);
        if (currentValue instanceof String) {
            log.info("Found current harvest config for:" + key + ", is :" + currentValue);
            return (String) currentValue;
        } else {
            throw new HarvesterException("Unable to find current config rules path in order to update rules config.");
        }
    }

    def update = {JsonObject harvest, String key, String value ->
        if (StringUtils.isNotBlank(value)) {
            log.info("Updating: " + key + " to: " + value);
            harvest.put(key, value);
        } else {
            log.warn("Value is blank. Key: " + key + " not updated.");
        }
    }
}
