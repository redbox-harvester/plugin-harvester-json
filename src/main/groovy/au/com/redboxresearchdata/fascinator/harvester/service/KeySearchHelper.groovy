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
import groovy.util.logging.Log4j
import org.apache.commons.lang.StringUtils

/**
 * @version
 * @author <a href="matt@redboxresearchdata.com.au">Matt Mulholland</a>
 */
@Log4j
class KeySearchHelper {

    String getKeyValueFromList(def data, def keyList) {
        def keyValue
        keyList.any {
            keyValue = getKeyValue(data, it)
            if (keyValue) {
                log.info("Returning: " + keyValue + ", for: " + keyList[0])
                return true
            } else {
                log.info("Continuing to search through key list alternatives...")
            }
        }
        return keyValue
    }

    String getKeyValue(def data, def key) {
        def keyValue
        log.info("Looking for " << key)
        def list = data.search(key);
        keyValue = getFirstMember(list)
        return keyValue
    }

    String getFirstMember(def list) {
        def keyValue = StringUtils.EMPTY
        if (list && list[0] in String) {
            log.warn("Only using first config key found...");
            keyValue = list[0];
            log.debug("got key value: " + keyValue);
        }
        return keyValue
    }

    def getStrictKeyValue = { def data, def keyList, def errorMessage ->
        def keyValue = getKeyValueFromList(data, keyList)
        if (!keyValue) throw new HarvesterException(errorMessage)
        return keyValue
    }

    def getLenientKeyValue = { def data, def keyList, def errorMessage ->
        def keyValue = getKeyValueFromList(data, keyList)
        if (!keyValue) {
            log.warn(errorMessage);
            log.warn("Returning empty string for: " + keyList[0])
        }
        return keyValue
    }
}
