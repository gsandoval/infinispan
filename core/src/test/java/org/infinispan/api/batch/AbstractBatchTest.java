package org.infinispan.api.batch;

import org.infinispan.Cache;
import org.infinispan.test.AbstractInfinispanTest;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractBatchTest extends AbstractInfinispanTest {
   protected String getOnDifferentThread(final Cache<String, String> cache, final String key) throws InterruptedException, ExecutionException {
      final AtomicReference<String> ref = new AtomicReference<String>();
      Future<String> future = fork(new Callable<String>() {
         public String call() {
            cache.startBatch();
            String result = cache.get(key);
            cache.endBatch(true);
            return result;
         }
      });

      return future.get();
   }
}
