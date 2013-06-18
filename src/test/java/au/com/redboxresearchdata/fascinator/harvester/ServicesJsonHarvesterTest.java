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
		assertTrue(harvester.isValidJson(new JsonSimple(validData.getObject("data"))));
	}
	
	@Test
	public void testBuildHarvestList() {
		harvester.setData(validData);
		try {
			harvester.buildHarvestList();
		} catch (HarvesterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals(1, harvester.harvestList.size());
	}
}
