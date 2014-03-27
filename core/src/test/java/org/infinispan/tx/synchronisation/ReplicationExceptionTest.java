package org.infinispan.tx.synchronisation;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertNull;

@Test(groups = "functional", testName = "replication.ReplicationExceptionTest")
public class ReplicationExceptionTest extends MultipleCacheManagersTest {

    protected void createCacheManagers() {
//        DummyTransactionManager.getInstance().setUseXaXid(true);
        ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
        configuration.locking()
                .lockAcquisitionTimeout(60000l)
                .transaction().transactionManagerLookup(new DummyTransactionManagerLookup())
                // uncomment this line and exception is not thrown for some reason
                .lockingMode(LockingMode.PESSIMISTIC);
        configuration.transaction().useSynchronization(false);
        createClusteredCaches(2, configuration);
        waitForClusterToForm();
    }

    @Test(groups = "functional", expectedExceptions = { CacheException.class })
    public void testSyncReplTimeout() {
        AdvancedCache cache1 = cache(0).getAdvancedCache();
        AdvancedCache cache2 = cache(1).getAdvancedCache();
        cache2.addInterceptor(new CommandInterceptor() {
            @Override
            protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd)
                    throws Throwable {
                // Add a delay
                Thread.sleep(100);
                return super.handleDefault(ctx, cmd);
            }
        }, 0);

        cache1.getCacheConfiguration().clustering().sync().replTimeout(10);
        cache2.getCacheConfiguration().clustering().sync().replTimeout(10);
        TestingUtil.blockUntilViewsReceived(10000, cache1, cache2);

        cache1.put("k", "v");
    }

   @Test(dependsOnMethods =  "testSyncReplTimeout")
   public void testTxCleanup() {
      assertNotLocked("k");
      assertNull(DummyTransactionManager.getInstance().getTransaction());
   }

}