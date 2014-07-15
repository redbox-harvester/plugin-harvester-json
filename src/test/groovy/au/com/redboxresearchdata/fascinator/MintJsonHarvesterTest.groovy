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

package au.com.redboxresearchdata.fascinator

import au.com.redboxresearchdata.fascinator.harvester.HarvestItem
import au.com.redboxresearchdata.fascinator.harvester.MintJsonHarvester
import com.googlecode.fascinator.api.storage.DigitalObject
import com.googlecode.fascinator.api.storage.Payload
import com.googlecode.fascinator.api.storage.Storage
import com.googlecode.fascinator.common.JsonSimple
import com.googlecode.fascinator.common.messaging.MessagingServices
import com.googlecode.fascinator.common.storage.StorageUtils
import org.apache.log4j.PropertyConfigurator
import org.powermock.reflect.Whitebox
import org.springframework.core.io.ClassPathResource
import spock.lang.Specification

/**
 * @version
 * @author <a href="matt@redboxresearchdata.com.au">Matt Mulholland</a>
 */
class MintJsonHarvesterTest extends Specification {
    String type;
    String requestId
    JsonSimple data
    JsonSimple harvestConfig
    MintJsonHarvester mintJsonHarvester
    private static final String RULES_KEY = "rulesConfig"
    StorageUtils storageUtils
    Storage storage
    DigitalObject digitalObject
    Properties metadata
    MessagingServices messaging
    Payload payload
    private static String RULES_PATH = "src/test/resources/rules"

    def setup() {
        type = "MintJson"
        requestId = "0fce141a-0f05-46e9-b9ac-fbb2292e0304"
        this.mintJsonHarvester = new MintJsonHarvester()
        this.harvestConfig = getJson("HarvestMintConfig.json")
        this.storage = Mock(Storage)
        this.digitalObject = Mock(DigitalObject)
        this.metadata = Mock(Properties)
        this.messaging = Mock(MessagingServices)
        this.payload = Mock(Payload)
        _ * this.storage.getObject(_ as String) >> this.digitalObject
        _ * this.digitalObject.getMetadata() >> this.metadata
        _ * this.digitalObject.getPayload(_ as String) >> this.payload
        Whitebox.setInternalState(this.mintJsonHarvester, "harvestConfig", this.harvestConfig)
        Whitebox.setInternalState(this.mintJsonHarvester, "storage", this.storage)
        Whitebox.setInternalState(this.mintJsonHarvester, "messaging", this.messaging)
    }

    def TestSingleServiceNoRecodIdPrefixNoIDDefault() {
        given: "incoming data has: " +
                "rulesConfig: 'Services', " +
                "idField: (none), " +
                "recordIDPrefix: (none), " +
                "and only 1 record"
        this.data = getJson("MintJsonServicesSingle.json")

        when: "harvesting Mint service data using Mint JSON harvester"
        List<HarvestItem> harvestItemList = this.mintJsonHarvester.harvest(this.data, this.type, this.requestId)

        then: "A List of 'HarvestItem' is returned" +
                "list size is 1" +
                "the harvestitem data is equal to the test data" +
                "the harvest config has: " +
                "- idField: 'ID'" +
                "- recordIDPrefix: 'test-prefix/services/'" +
                "- rulesConfig: 'src/test/resources/Services.json'"
        harvestItemList.size() == 1
        harvestItemList[0].data.toString().equals(this.data.getArray("data")[0].toString())
        this.harvestConfig.getPath("harvester", "recordIDPrefix").equals("test-prefix/services/")
        this.harvestConfig.getPath("harvester", "rulesConfig").equals(RULES_PATH + "/Services.json")
    }

    def TestSinglePartiesPeopleNoIDDefault() {
        given: "incoming data has: " +
                "rulesConfig: 'Parties_People', " +
                "idField: (none), " +
                "recordIDPrefix: 'parties/people', " +
                "and only 1 record"
        this.data = getJson("MintJsonParties_PeopleSingle.json")

        when: "harvesting Mint service data using Mint JSON harvester"
        List<HarvestItem> harvestItemList = this.mintJsonHarvester.harvest(this.data, this.type, this.requestId)

        then: "A List of 'HarvestItem' is returned" +
                "list size is 1" +
                "the harvestitem data is equal to the test data" +
                "the harvest config has: " +
                "- idField: 'ID', " +
                "- recordIDPrefix: 'test-prefix/parties/people/', " +
                "- rulesConfig: 'src/test/resources/Parties_People.json'"
        harvestItemList.size() == 1
        harvestItemList[0].data.toString().equals(this.data.getArray("data")[0].toString())
        this.harvestConfig.getPath("harvester", "recordIDPrefix").equals("test-prefix/parties/people/")
        this.harvestConfig.getPath("harvester", "rulesConfig").equals(RULES_PATH + "/Parties_People.json")
    }

    def TestSingleLanguagesNoRecordIdPrefix() {
        given: "incoming data has: " +
                "rulesConfig: 'Languages', " +
                "idField: 'alpha3', " +
                "recordIDPrefix: (none), " +
                "and only 1 record"
        this.data = getJson("MintJsonLanguagesSingle.json")

        when: "harvesting Mint service data using Mint JSON harvester"
        List<HarvestItem> harvestItemList = this.mintJsonHarvester.harvest(this.data, this.type, this.requestId)

        then: "A List of 'HarvestItem' is returned" +
                "list size is 1" +
                "the harvestitem data is equal to the test data" +
                "the harvest config has: " +
                "- idField: 'alpha3'" +
                "- recordIDPrefix: 'test-prefix/languages/'" +
                "- rulesConfig set to 'src/test/resources/Languages.json'"
        harvestItemList.size() == 1
        harvestItemList[0].data.toString().equals(this.data.getArray("data")[0].toString())
        this.harvestConfig.getPath("harvester", "recordIDPrefix").equals("test-prefix/languages/")
        this.harvestConfig.getPath("harvester", "rulesConfig").equals(RULES_PATH + "/Languages.json")
    }

    JsonSimple getJson(String fileName) {
        return new JsonSimple(getFile(fileName))
    }

    File getFile(String fileName) {
        return new ClassPathResource(fileName).getFile()
    }

    def cleanup() {
        this.mintJsonHarvester = null
        this.harvestConfig = null
        this.storage = null
        this.digitalObject = null
        this.metadata = null
        this.messaging = null
        this.payload = null
    }
}