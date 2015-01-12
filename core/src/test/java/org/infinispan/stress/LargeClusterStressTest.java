package org.infinispan.stress;

import org.infinispan.Cache;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.topology.LocalTopologyManager;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Test that we're able to start a large cluster in a single JVM.
 *
 * @author Dan Berindei
 * @since 5.3
 */
@Test(groups = "stress", testName = "stress.LargeClusterStressTest")
public class LargeClusterStressTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 50;
   private static final int NUM_CACHES = 50;

   @Override
   protected void createCacheManagers() throws Throwable {
      // start the cache managers in the test itself
   }

   public void testLargeCluster() throws Exception {
      final Configuration distConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false).clustering().stateTransfer().awaitInitialTransfer(false).build();
      final Configuration replConfig = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false).clustering().stateTransfer().awaitInitialTransfer(false).build();

      // Start the caches (and the JGroups channels) in separate threads
      Future<Object>[] futures = new Future[NUM_NODES];
      for (int i = 0; i < NUM_NODES; i++) {
         final String nodeName = TestResourceTracker.getNameForIndex(i);
         futures[i] = fork(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
               GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
               gcb.globalJmxStatistics().allowDuplicateDomains(true);
               gcb.transport().defaultTransport().nodeName(nodeName);
               BlockingThreadPoolExecutorFactory remoteExecutorFactory = new BlockingThreadPoolExecutorFactory(
                     10, 1, 0, 60000);
               gcb.transport().remoteCommandThreadPool().threadPoolFactory(remoteExecutorFactory);
               EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build());
               registerCacheManager(cm);
               for (int j = 0; j < NUM_CACHES; j++) {
                  if (j % 2 == 0) {
                     cm.defineConfiguration("repl-cache-" + j, replConfig);
                  } else {
                     cm.defineConfiguration("dist-cache-" + j, distConfig);
                  }
               }
               for (int j = 0; j < NUM_CACHES; j++) {
                  if (j % 2 == 0) {
                     Cache<Object, Object> cache = cm.getCache("repl-cache-" + j);
                     cache.put(cm.getAddress(), "bla");
                  } else {
                     Cache<Object, Object> cache = cm.getCache("dist-cache-" + j);
                     cache.put(cm.getAddress(), "bla");
                  }
               }
               log.infof("Started cache manager %s", cm.getAddress());
               return null;
            }
         });
      }

      for (int i = 0; i < NUM_NODES; i++) {
         futures[i].get(30, TimeUnit.SECONDS);
      }

      for (int j = 0; j < NUM_CACHES; j++) {
         if (j % 2 == 0) {
            waitForClusterToForm("repl-cache-" + j);
         } else {
            waitForClusterToForm("dist-cache-" + j);
         }
      }
      TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class).setRebalancingEnabled(false);
   }
}
