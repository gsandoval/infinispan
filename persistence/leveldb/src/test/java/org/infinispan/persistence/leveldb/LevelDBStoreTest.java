package org.infinispan.persistence.leveldb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.leveldb.LevelDBStoreTest")
public class LevelDBStoreTest extends BaseStoreTest {

   private LevelDBStore fcs;
   private String tmpDirectory;
   private EmbeddedCacheManager cacheManager;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   protected LevelDBStoreConfiguration createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      cacheManager = TestCacheManagerFactory.createCacheManager(CacheMode.LOCAL, false);
      LevelDBStoreConfigurationBuilder cfg = new LevelDBStoreConfigurationBuilder(lcb);
      cfg.location(tmpDirectory + "/data");
      cfg.expiredLocation(tmpDirectory + "/expiry");
      cfg.clearThreshold(2);
      return cfg.create();
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      return cacheManager.getCache().getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }

   @AfterMethod
   @Override
   public void tearDown() throws PersistenceException {
      super.tearDown();
      TestingUtil.killCacheManagers(cacheManager);
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      clearTempDir();
      fcs = new LevelDBStore();
      ConfigurationBuilder cb = new ConfigurationBuilder();
      LevelDBStoreConfiguration cfg = createCacheStoreConfig(cb.persistence());
      fcs.init(new DummyInitializationContext(cfg, getCache(), getMarshaller(), new ByteBufferFactoryImpl(),
                                              new MarshalledEntryFactoryImpl(getMarshaller())));
      fcs.start();
      return fcs;
   }

   @Override
   public void testPurgeExpired() throws Exception {
      long lifespan = 1000;
      InternalCacheEntry k1 = TestInternalCacheEntryFactory.create("k1", "v1", lifespan);
      InternalCacheEntry k2 = TestInternalCacheEntryFactory.create("k2", "v2", lifespan);
      InternalCacheEntry k3 = TestInternalCacheEntryFactory.create("k3", "v3", lifespan);
      cl.write(TestingUtil.marshalledEntry(k1, getMarshaller()));
      cl.write(TestingUtil.marshalledEntry(k2, getMarshaller()));
      cl.write(TestingUtil.marshalledEntry(k3, getMarshaller()));
      assert cl.contains("k1");
      assert cl.contains("k2");
      assert cl.contains("k3");
      Thread.sleep(lifespan + 100);
      cl.purge(new WithinThreadExecutor(), null);
      LevelDBStore fcs = (LevelDBStore) cl;
      assert fcs.load("k1") == null;
      assert fcs.load("k2") == null;
      assert fcs.load("k3") == null;
   }

   public void testStopStartDoesntNukeValues() throws InterruptedException {
      assert !cl.contains("k1");
      assert !cl.contains("k2");

      long lifespan = 1;
      long idle = 1;
      InternalCacheEntry se1 = TestInternalCacheEntryFactory.create("k1", "v1", lifespan);
      InternalCacheEntry se2 = TestInternalCacheEntryFactory.create("k2", "v2");
      InternalCacheEntry se3 = TestInternalCacheEntryFactory.create("k3", "v3", -1, idle);
      InternalCacheEntry se4 = TestInternalCacheEntryFactory.create("k4", "v4", lifespan, idle);

      cl.write(TestingUtil.marshalledEntry(se1, getMarshaller()));
      cl.write(TestingUtil.marshalledEntry(se2, getMarshaller()));
      cl.write(TestingUtil.marshalledEntry(se3, getMarshaller()));
      cl.write(TestingUtil.marshalledEntry(se4, getMarshaller()));
      Thread.sleep(100);
      // Force a purge expired so that expiry tree is updated
      cl.purge(new WithinThreadExecutor(), null);
      cl.stop();
      cl.start();
      assert se1.isExpired();
      assert cl.load("k1") == null;
      assert !cl.contains("k1");
      assert cl.load("k2") != null;
      assert cl.contains("k2");
      assert cl.load("k2").getValue().equals("v2");
      assert se3.isExpired();
      assert cl.load("k3") == null;
      assert !cl.contains("k3");
      assert se3.isExpired();
      assert cl.load("k3") == null;
      assert !cl.contains("k3");
   }

   public void testConcurrentWriteAndRestart() {
      concurrentWriteAndRestart(true);
   }

   public void testConcurrentWriteAndStop() {
      concurrentWriteAndRestart(true);
   }

   private void concurrentWriteAndRestart(boolean start) {
      final int THREADS = 4;
      final AtomicBoolean run = new AtomicBoolean(true);
      final AtomicInteger writtenPre = new AtomicInteger();
      final AtomicInteger writtenPost = new AtomicInteger();
      final AtomicBoolean post = new AtomicBoolean(false);
      final CountDownLatch started = new CountDownLatch(THREADS);
      final CountDownLatch finished = new CountDownLatch(THREADS);
      ExecutorService executor = Executors.newFixedThreadPool(THREADS);
      for (int i = 0; i < THREADS; ++i) {
         final int thread = i;
         executor.execute(new Runnable() {
            @Override
            public void run() {
               try {
                  started.countDown();
                  int i = 0;
                  while (run.get()) {
                     InternalCacheEntry entry = TestInternalCacheEntryFactory.create("k" + i, "v" + i);
                     MarshalledEntry me = TestingUtil.marshalledEntry(entry, getMarshaller());
                     try {
                        AtomicInteger record = post.get() ? writtenPost : writtenPre;
                        cl.write(me);
                        ++i;
                        int prev;
                        do {
                           prev = record.get();
                           if ((prev & (1 << thread)) != 0) break;
                        } while (record.compareAndSet(prev, prev | (1 << thread)));
                     } catch (PersistenceException e) {
                        // when the store is stopped, exceptions are thrown
                     }
                  }
               } catch (Exception e) {
                  log.error("Failed", e);
                  throw new RuntimeException(e);
               } finally {
                  finished.countDown();
               }
            }
         });
      }
      try {
         if (!started.await(30, TimeUnit.SECONDS)) {
            fail();
         }
         Thread.sleep(1000);
         cl.stop();
         post.set(true);
         Thread.sleep(1000);
         if (start) {
            cl.start();
            Thread.sleep(1000);
         }
      } catch (InterruptedException e) {
         fail();
      } finally {
         run.set(false);
         executor.shutdown();
      }
      try {
         if (!finished.await(30, TimeUnit.SECONDS)) {
            fail();
         }
      } catch (InterruptedException e) {
         fail();
      }
      assertEquals(writtenPre.get(), (1 << THREADS) - 1, "pre");
      if (start) {
         assertEquals(writtenPost.get(), (1 << THREADS) - 1, "post");
      } else {
         assertEquals(writtenPost.get(), 0, "post");
      }
   }
}
