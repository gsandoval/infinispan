package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertEquals;

/**
 * Test to verify distributed entry behavior when a loader is present
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.DistributedEntryRetrieverWithLoaderTest")
public class DistributedEntryRetrieverWithLoaderTest extends MultipleCacheManagersTest {
   protected final static String CACHE_NAME = "DistributedEntryRetrieverWithLoaderTest";
   protected ConfigurationBuilder builderUsed;
   protected final boolean tx = false;
   protected final CacheMode cacheMode = CacheMode.DIST_SYNC;

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      builderUsed.clustering().hash().numOwners(1);
      builderUsed.persistence().passivation(false).addStore(DummyInMemoryStoreConfigurationBuilder.class).storeName(CACHE_NAME);
      if (tx) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      createClusteredCaches(3, CACHE_NAME, builderUsed);
   }

   @Test
   public void testCacheLoader() throws InterruptedException, ExecutionException, TimeoutException {
      Cache<MagicKey, String> cache0 = cache(0, CACHE_NAME);
      Cache<MagicKey, String> cache1 = cache(1, CACHE_NAME);
      Cache<MagicKey, String> cache2 = cache(2, CACHE_NAME);

      Map<MagicKey, String> originalValues = new HashMap<MagicKey, String>();
      originalValues.put(new MagicKey(cache0), "cache0");
      originalValues.put(new MagicKey(cache1), "cache1");
      originalValues.put(new MagicKey(cache2), "cache2");

      cache0.putAll(originalValues);

      EntryRetriever<MagicKey, String> retriever = cache1.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      Iterator<Map.Entry<MagicKey, String>> iterator = retriever.retrieveEntries(null, null, null, null);

      // we need this count since the map will replace same key'd value
      int count = 0;
      Map<MagicKey, String> results = new HashMap<MagicKey, String>();
      while (iterator.hasNext()) {
         Map.Entry<MagicKey, String> entry = iterator.next();
         results.put(entry.getKey(), entry.getValue());
         count++;
      }
      assertEquals(count, 3);
      assertEquals(originalValues, results);
   }
}

