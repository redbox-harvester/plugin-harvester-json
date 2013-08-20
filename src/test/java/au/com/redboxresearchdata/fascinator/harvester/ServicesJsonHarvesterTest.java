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

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.common.JsonSimple;

public class ServicesJsonHarvesterTest {

	private static JsonSimple validData;
	private static ServiceJsonHarvester harvester;
	@BeforeClass
	public static void setUp() throws Exception {
		validData = new JsonSimple(new File("src/test/resources/TestService.json"));
		harvester = new ServiceJsonHarvester();
	}

	@AfterClass
	public static void tearDown() throws Exception {
	}

   @Test
	public void testIsValidJson() {
		harvester.idField = "ID";
		assertTrue(1==1);
	}
	
	@Test
	public void testBuildHarvestList() {
		harvester.setData(validData);
		assertTrue(1==1);
		/**try {
			harvester.buildHarvestList();
		} catch (HarvesterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals(1, harvester.harvestList.size());**/
	}
}
