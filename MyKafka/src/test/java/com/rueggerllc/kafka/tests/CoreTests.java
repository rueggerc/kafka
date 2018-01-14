package com.rueggerllc.kafka.tests;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class CoreTests {

	private static Logger logger = Logger.getLogger(CoreTests.class);

	
	@BeforeClass
	public static void setupClass() throws Exception {
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setupTest() throws Exception {
	}

	@After
	public void tearDownTest() throws Exception {
	}
	
	@Test
	// @Ignore
	public void testDummy() {
		logger.info("Dummy Test Begin");
	}
	
	@Test
	public void testMod1() {
		int numberOfPartitions = 3;
        Integer value = new Integer(24);
        int hashCode = value.hashCode();
        logger.info("hashcode=" + hashCode);
        int result = value.hashCode() % numberOfPartitions + 45;
        logger.info("Result=" + result);
	}
	
	@Test
	@Ignore
	public void testPartitionLogic() {
		int numberOfPartitions = 3;
        Integer value = new Integer(24);
        int result = value.hashCode() % numberOfPartitions + 2;
        logger.info("Result=" + result);
	}
	
	
	
	
}
