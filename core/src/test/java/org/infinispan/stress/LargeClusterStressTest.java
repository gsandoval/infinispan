package org.infinispan.stress;

import org.infinispan.Cache;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.topology.LocalTopologyManager;
import org.testng.annotations.Test;

/**
 * Test that we're able to start a large cluster in a single JVM.
 *
 * @author Dan Berindei
 * @since 5.3
 */
@Test(groups = "stress", testName = "stress.LargeClusterStressTest")
public class LargeClusterStressTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 50;
   private static final int NUM_CACHES = 100;

   @Override
   protected void createCacheManagers() throws Throwable {
      // start the cache managers in the test itself
   }

   public void testLargeCluster() throws Exception {
      ConfigurationBuilder distConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      distConfig.clustering().stateTransfer().awaitInitialTransfer(false);
      distConfig.jmxStatistics().disable();
      ConfigurationBuilder replConfig = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      replConfig.clustering().stateTransfer().awaitInitialTransfer(false);
      replConfig.jmxStatistics().disable();
      for (int i = 0; i < NUM_NODES; i++) {
         GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
         gcb.globalJmxStatistics().allowDuplicateDomains(true).disable();
         gcb.transport().defaultTransport().nodeName(TestResourceTracker.getNameForIndex(i));
         BlockingThreadPoolExecutorFactory remoteExecutorFactory = new BlockingThreadPoolExecutorFactory(
               10, 1, 0, 60000);
         gcb.transport().remoteCommandThreadPool().threadPoolFactory(remoteExecutorFactory);
         EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build());
         registerCacheManager(cm);
         for (int j = 0; j < NUM_CACHES; j++) {
            if (j % 2 == 0) {
               cm.defineConfiguration("replcache" + j, replConfig.build());
               Cache<Object, Object> cache = cm.getCache("replcache" + j);
               cache.put(cm.getAddress(), "bla");
            } else {
               cm.defineConfiguration("distcache" + j, distConfig.build());
               Cache<Object, Object> cache = cm.getCache("distcache" + j);
               cache.put(cm.getAddress(), "bla");
            }
         }
         log.infof("Started cache manager %s", cm.getAddress());
         // TODO Test is unstable without this wait, needs more investigation after JGRP-1899 is fixed
//         TestingUtil.blockForMemberToFail(30000, cacheManagers.toArray(new EmbeddedCacheManager[0]));
      }

      for (int j = 0; j < NUM_CACHES; j++) {
         waitForClusterToForm("replcache" + j);
         waitForClusterToForm("distcache" + j);
      }
   }

   @Test(dependsOnMethods = "testLargeCluster")
   public void testLargeClusterShutdown() throws Exception {
      TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class).setRebalancingEnabled(false);
      for (EmbeddedCacheManager cm : cacheManagers) {
         log.infof("Stopping cache manager %s", cm.getAddress());
         cm.stop();
      }
   }
}
