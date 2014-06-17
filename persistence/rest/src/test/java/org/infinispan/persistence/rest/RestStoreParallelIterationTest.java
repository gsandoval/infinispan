package org.infinispan.persistence.rest;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.rest.configuration.RestStoreConfigurationBuilder;
import org.infinispan.rest.EmbeddedRestServer;
import org.infinispan.rest.RestTestingUtil;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "unstable", testName = "persistence.rest.RestStoreParallelIterationTest", description = "original group: functional")
public class RestStoreParallelIterationTest  extends ParallelIterationTest {

   private EmbeddedCacheManager localCacheManager;
   private EmbeddedRestServer restServer;

   protected void configurePersistence(ConfigurationBuilder cb) {
      localCacheManager = TestCacheManagerFactory.createCacheManager();
      restServer = RestTestingUtil.startRestServer(localCacheManager);
      cb.persistence().addStore(RestStoreConfigurationBuilder.class)
            .host("localhost")
            .port(restServer.getPort())
            .path("/rest/"+ BasicCacheContainer.DEFAULT_CACHE_NAME)
            .preload(false);
   }

   @Override
   protected void teardown() {
      super.teardown();
      RestTestingUtil.killServers(restServer);
      TestingUtil.killCacheManagers(localCacheManager);
   }

   @Override
   protected int numThreads() {
      return KnownComponentNames.getDefaultThreads(KnownComponentNames.PERSISTENCE_EXECUTOR) + 1 /** caller's thread */;
   }

   /*
    * Unfortunately we need to mark each test individual as unstable because the super class belong to a valid test
    * group. I think that it appends the unstable group to the super class group making it running the tests anyway.
    */

   @Test(groups = "unstable", description = "don't know why but it is still running this test even the class is marked as unstable")
   public void testParallelIterationWithValueAndMetadata() {
      super.testParallelIterationWithValueAndMetadata();
   }

   @Test(groups = "unstable", description = "don't know why but it is still running this test even the class is marked as unstable")
   public void testParallelIterationWithValueWithoutMetadata() {
      super.testParallelIterationWithValueWithoutMetadata();
   }

   @Test(groups = "unstable", description = "don't know why but it is still running this test even the class is marked as unstable")
   public void testSequentialIterationWithValueAndMetadata() {
      super.testSequentialIterationWithValueAndMetadata();
   }

   @Test(groups = "unstable", description = "don't know why but it is still running this test even the class is marked as unstable")
   public void testSequentialIterationWithValueWithoutMetadata() {
      super.testSequentialIterationWithValueWithoutMetadata();
   }

   @Test(groups = "unstable", description = "don't know why but it is still running this test even the class is marked as unstable")
   public void testParallelIterationWithoutValueWithMetadata() {
      super.testParallelIterationWithoutValueWithMetadata();
   }

   @Test(groups = "unstable", description = "don't know why but it is still running this test even the class is marked as unstable")
   public void testParallelIterationWithoutValueOrMetadata() {
      super.testParallelIterationWithoutValueOrMetadata();
   }

   @Test(groups = "unstable", description = "don't know why but it is still running this test even the class is marked as unstable")
   public void testSequentialIterationWithoutValueWithMetadata() {
      super.testSequentialIterationWithoutValueWithMetadata();
   }

   @Test(groups = "unstable", description = "don't know why but it is still running this test even the class is marked as unstable")
   public void testSequentialIterationWithoutValueOrMetadata() {
      super.testSequentialIterationWithoutValueOrMetadata();
   }

   @Test(groups = "unstable")
   @Override
   public void testCancelingTaskMultipleProcessors() {
      super.testCancelingTaskMultipleProcessors();
   }

   @Override
   protected void assertMetadataEmpty(InternalMetadata metadata) {
      // RestStore always creates metadata, even if we wrote the entry with null metadata
      if (metadata != null) {
         assertTrue(metadata.created() < 0);
         assertTrue(metadata.lastUsed() < 0);
         assertTrue(metadata.lifespan() < 0);
         assertTrue(metadata.maxIdle() < 0);
         assertNull(metadata.version());
      }
   }
}
