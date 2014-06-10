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

import au.com.redboxresearchdata.fascinator.harvester.MintJsonHarvester
import com.googlecode.fascinator.common.JsonSimple
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
    MintJsonHarvester mintJsonHarvester;
    private static final String RULES_KEY = "rulesConfig";

    def setup() {
        type = "MintJson"
        requestId = "0fce141a-0f05-46e9-b9ac-fbb2292e0304"
        data = new JsonSimple(new ClassPathResource("harvestMintConfigForServices.json").getFile())
        mintJsonHarvester = Mock(MintJsonHarvester);

    }

    def Test1() {
        expect : "testing setup"
        this.mintJsonHarvester.extractStrictConfig(this.data, RULES_KEY) == "Services"
    }

    def Test2() {
//        when: "harvesting service data"
//        this.mintJsonHarvester.harvest(this.data, this.type, this.requestId)
//
//        expect : "In the harvest config:" +
//                "- only first prefix of recordIDPrefix is appended to," +
//                "- the existing full path of rulesConfig is appended to" +
//                "- the id field is updated"
//        this.mintJsonHarvester.harvestConfig ==
    }


    String getHarvestConfigForServices() {
          return "{\n" +
                  "  \"harvester\": {\n" +
                  "    \"recordIDPrefix\": \"test-prefix/\",\n" +
                  "    \"mainPayloadId\": \"metadata.json\",\n" +
                  "    \"rulesConfig\": \"originalPath/harvest/\",\n" +
                  "    \"handlingType\": \"park\",\n" +
                  "    \"commit\": true,\n" +
                  "    \"idField\": \"foo\"\n" +
                  "  }\n" +
                  "}"
    }

    def cleanup() {
        type = "boo"
        print type
    }
}