package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyValueFilterConverter;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Base class for entry retriever tests
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.BaseEntryRetrieverTest")
public abstract class BaseEntryRetrieverTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = getClass().getName();
   protected ConfigurationBuilder builderUsed;
   protected final boolean tx;
   protected final CacheMode cacheMode;

   public BaseEntryRetrieverTest(boolean tx, CacheMode mode) {
      this.tx = tx;
      cacheMode = mode;
   }

   protected abstract Object getKeyTiedToCache(Cache<?, ?> cache);

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      if (tx) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      if (cacheMode.isClustered()) {
         builderUsed.clustering().hash().numOwners(2);
         builderUsed.clustering().stateTransfer().chunkSize(50);
         createClusteredCaches(3, CACHE_NAME, builderUsed);
      } else {
         EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builderUsed);
         cacheManagers.add(cm);
      }
   }

   protected Map<Object, String> putValuesInCache() {
      // This is linked to keep insertion order
      Map<Object, String> valuesInserted = new LinkedHashMap<Object, String>();
      Cache<Object, String> cache = cache(0, CACHE_NAME);
      Object key = getKeyTiedToCache(cache);
      cache.put(key, key.toString());
      valuesInserted.put(key, key.toString());
      return valuesInserted;
   }

   @Test
   public void simpleTest() {
      Map<Object, String> values = putValuesInCache();

      EntryRetriever<MagicKey, String> retriever = cache(0, CACHE_NAME).getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      CloseableIterator<Map.Entry<MagicKey, String>> iterator = retriever.retrieveEntries(null, null, null, null);
      Map<MagicKey, String> results = mapFromIterator(iterator);
      assertEquals(values, results);
   }

   @Test
   public void simpleTestLocalFilter() {
      Map<Object, String> values = putValuesInCache();
      Iterator<Map.Entry<Object, String>> iter = values.entrySet().iterator();
      Map.Entry<Object, String> excludedEntry = iter.next();
      // Remove it so comparison below will be correct
      iter.remove();

      EntryRetriever<MagicKey, String> retriever = cache(0, CACHE_NAME).getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      CloseableIterator<Map.Entry<MagicKey, String>> iterator = retriever.retrieveEntries(
            new KeyFilterAsKeyValueFilter<MagicKey, String>(new CollectionKeyFilter<Object>(Collections.singleton(excludedEntry.getKey()))),
            null, null, null);
      Map<MagicKey, String> results = mapFromIterator(iterator);
      assertEquals(values, results);
   }

   @Test
   public void testPublicAPI() {
      Map<Object, String> values = putValuesInCache();
      Iterator<Map.Entry<Object, String>> iter = values.entrySet().iterator();
      Map.Entry<Object, String> excludedEntry = iter.next();
      // Remove it so comparison below will be correct
      iter.remove();


      Cache<MagicKey, String> cache = cache(0, CACHE_NAME);
      EntryIterable<MagicKey, String> iterable = cache.getAdvancedCache().filterEntries(
            new KeyFilterAsKeyValueFilter<MagicKey, String>(new CollectionKeyFilter<Object>(
                  Collections.singleton(excludedEntry.getKey()))));

      Map<MagicKey, String> results = mapFromIterable(iterable);
      assertEquals(values, results);
   }

   @Test
   public void testPublicAPIWithConverter() {
      Map<Object, String> values = putValuesInCache();
      Iterator<Map.Entry<Object, String>> iter = values.entrySet().iterator();
      Map.Entry<Object, String> excludedEntry = iter.next();
      // Remove it so comparison below will be correct
      iter.remove();


      Cache<MagicKey, String> cache = cache(0, CACHE_NAME);
      EntryIterable<MagicKey, String> iterable = cache.getAdvancedCache().filterEntries(
            new KeyFilterAsKeyValueFilter<MagicKey, String>(new CollectionKeyFilter<Object>(
                  Collections.singleton(excludedEntry.getKey()))));

      Map<MagicKey, String> results = mapFromIterable(iterable.converter(new StringTruncator(2, 5)));

      assertEquals(values.size(), results.size());
      for (Map.Entry<Object, String> entry : values.entrySet()) {
         assertEquals(entry.getValue().substring(2, 7), results.get(entry.getKey()));
      }
   }

   @Test
   public void testFilterAndConverterCombined() {
      Map<Object, String> values = putValuesInCache();
      Iterator<Map.Entry<Object, String>> iter = values.entrySet().iterator();
      Map.Entry<Object, String> excludedEntry = iter.next();
      // Remove it so comparison below will be correct
      iter.remove();


      Cache<MagicKey, String> cache = cache(0, CACHE_NAME);
      KeyValueFilterConverter<MagicKey, String, String> filterConverter = new CompositeKeyValueFilterConverter<MagicKey, String, String>(
            new KeyFilterAsKeyValueFilter<Object, String>(new CollectionKeyFilter<Object>(Collections.singleton(excludedEntry.getKey()))),
            new StringTruncator(2, 5));
      EntryIterable<MagicKey, String> iterable = cache.getAdvancedCache().filterEntries(filterConverter);
      Map<MagicKey, String> results = mapFromIterable(iterable);

      assertEquals(values.size(), results.size());
      for (Map.Entry<Object, String> entry : values.entrySet()) {
         assertEquals(entry.getValue().substring(2, 7), results.get(entry.getKey()));
      }
   }

   protected static <K, V> Map<K, V> mapFromIterator(Iterator<Map.Entry<K, V>> iterator) {
      Map<K, V> map = new HashMap<K, V>();
      while (iterator.hasNext()) {
         Map.Entry<K, V> entry = iterator.next();
         map.put(entry.getKey(), entry.getValue());
      }
      return map;
   }

   protected static <K, V> Map<K, V> mapFromIterator(CloseableIterator<Map.Entry<K, V>> iterator) {
      try {
         return mapFromIterator((Iterator<Map.Entry<K, V>>)iterator);
      } finally {
         try {
            iterator.close();
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
   }

   protected static <K, V> Map<K, V> mapFromIterable(Iterable<Map.Entry<K, V>> iterable) {
      Map<K, V> map = new HashMap<K, V>();
      for (Map.Entry<K, V> entry : iterable) {
         map.put(entry.getKey(), entry.getValue());
      }
      return map;
   }

   protected static <K, V> Map<K, V> mapFromIterable(CloseableIterable<Map.Entry<K, V>> iterable) {
      try {
         return mapFromIterable((Iterable<Map.Entry<K, V>>)iterable);
      } finally {
         try {
            iterable.close();
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
   }

   protected static class StringTruncator implements Converter<Object, String, String>, Serializable {
      private final int beginning;
      private final int length;

      public StringTruncator(int beginning, int length) {
         this.beginning = beginning;
         this.length = length;
      }

      @Override
      public String convert(Object key, String value, Metadata metadata) {
         if (value != null && value.length() > beginning + length) {
            return value.substring(beginning, beginning + length);
         } else {
            return value;
         }
      }
   }
}

