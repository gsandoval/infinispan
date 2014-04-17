package org.infinispan.executors;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "executors.ExecutorAllCompletionServiceTest")
public class ExecutorAllCompletionServiceTest extends AbstractInfinispanTest {

   public void testWaitForAll() throws InterruptedException {
      ExecutorAllCompletionService service = createService(1);
      long before = System.currentTimeMillis();
      service.submit(new WaitRunnable(500), null);
      service.submit(new WaitRunnable(500), null);
      service.waitUntilAllCompleted();
      long after = System.currentTimeMillis();
      assert after - before >= 1000;
      assert service.isAllCompleted();
      assert !service.isExceptionThrown();
   }

   public void testExceptions() throws InterruptedException {
      ExecutorAllCompletionService service = createService(1);
      service.submit(new WaitRunnable(1), null);
      service.submit(new ExceptionRunnable("second"), null);
      service.submit(new WaitRunnable(1), null);
      service.submit(new ExceptionRunnable("third"), null);
      service.waitUntilAllCompleted();
      assert service.isAllCompleted();
      assert service.isExceptionThrown();
      assert "second".equals(findCause(service.getFirstException()).getMessage());
   }

   public void testParallelWait() throws InterruptedException, TimeoutException, ExecutionException {
      final ExecutorAllCompletionService service = createService(2);
      for (int i = 0; i < 300; ++i) {
         service.submit(new WaitRunnable(10), null);
      }
      List<Future> workers = new ArrayList<Future>(10);
      for (int i = 0; i < 10; ++i) {
         Future f = fork(new Callable<Void>() {
            @Override
            public Void call() {
               try {
                  service.waitUntilAllCompleted();
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
               assert service.isAllCompleted();
               assert !service.isExceptionThrown();
               return null;
            }
         });
         workers.add(f);
      }
      for (Future f : workers) {
         f.get(10, TimeUnit.SECONDS);
      }
      assert service.isAllCompleted();
      assert !service.isExceptionThrown();
   }

   public void testParallelException() throws InterruptedException, TimeoutException, ExecutionException {
      final ExecutorAllCompletionService service = createService(2);
      for (int i = 0; i < 150; ++i) {
         service.submit(new WaitRunnable(10), null);
      }
      service.submit(new ExceptionRunnable("foobar"), null);
      for (int i = 0; i < 150; ++i) {
         service.submit(new WaitRunnable(10), null);
      }
      List<Future> workers = new ArrayList<Future>(10);
      for (int i = 0; i < 10; ++i) {
         Future f = fork(new Callable<Void>() {
            @Override
            public Void call() {
               try {
                  service.waitUntilAllCompleted();
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
               assert service.isAllCompleted();
               assert service.isExceptionThrown();
               return null;
            }
         });
         workers.add(f);
      }
      for (Future f : workers) {
         f.get(10, TimeUnit.SECONDS);
      }
      assert service.isAllCompleted();
      assert service.isExceptionThrown();
   }

   private Throwable findCause(ExecutionException e) {
      Throwable t = e;
      while (t.getCause() != null) t = t.getCause();
      return t;
   }

   private ExecutorAllCompletionService createService(int maxThreads) {
      return new ExecutorAllCompletionService(new ThreadPoolExecutor(1, maxThreads, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1000)));
   }

   private class WaitRunnable implements Runnable {
      private long period;

      private WaitRunnable(long period) {
         this.period = period;
      }

      @Override
      public void run() {
         try {
            Thread.sleep(period);
         } catch (InterruptedException e) {
         }
      }
   }

   private class ExceptionRunnable implements Runnable {
      private final String message;

      public ExceptionRunnable(String message) {
         this.message = message;
      }

      @Override
      public void run() {
         throw new RuntimeException(message);
      }
   }
}
