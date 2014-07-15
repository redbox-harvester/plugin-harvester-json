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

import org.apache.commons.lang.StringUtils

/**
 * @version
 * @author <a href="matt@redboxresearchdata.com.au">Matt Mulholland</a>
 */
enum MintHarvestEnum {
    RULES_KEY("rulesConfig", "getStrictKeyValue", "appendToFullPathOfHarvestKeyValue",
            { it + ".json" }),
    ID_PREFIX_KEY("recordIDPrefix", "getLenientKeyValue", "appendToPathPrefixOfHarvestKeyValue",
            { it.toLowerCase() }, MintHarvestEnum.RULES_KEY.keyName),
    ID_FIELD_KEY("idField", "getLenientKeyValue", "update", { StringUtils.defaultIfEmpty(it.trim(), "ID") });

    String keyName
    Expando searcher
    Expando updater

    private MintHarvestEnum(String keyName, String searchFunction, String updateFunction, Closure formatter) {
        this.keyName = keyName
        this.searcher = new Expando()
        this.searcher.with {
            function = new KeySearchHelper()."${searchFunction}"
            keyList = [keyName]
            errorMessage = "No harvest config key for: " + keyName + " found in data."
        }
        this.updater = new Expando()
        this.updater.with {
            function = new HarvestConfigUpdateService()."${updateFunction}"
            formatFunction = formatter
        }
    }

    private MintHarvestEnum(String keyName, String searchFunction, String updateFunction, Closure configFormatter,
                            String defaultKeyName) {
        this(keyName, searchFunction, updateFunction, configFormatter)
        this.searcher.keyList << defaultKeyName
    }

    def updateHarvest(def data, def harvest) {
        String value = getHarvestedValue(data)
        updateHarvestConfigKey(harvest, value)
    }

    String getHarvestedValue(def data) {
        String value = this.searcher.function(data, this.searcher.keyList, this.searcher.errorMessage)
        return value
    }

    def updateHarvestConfigKey(def harvest, def value) {
        def formattedValue = this.updater.formatFunction(value)
        this.updater.function(harvest, this.keyName, formattedValue)
    }

}
