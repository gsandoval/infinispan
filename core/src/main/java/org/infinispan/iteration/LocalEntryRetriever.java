package org.infinispan.iteration;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyFilter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyFilterBridge;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.KeyValueFilterAsKeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.filter.Converter;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * Entry retriever that only retrieves keys and values from the local stores.  This is useful for local, replicated
 * and invalidation caches.
 *
 * @author wburns
 * @since 7.0
 */
public class LocalEntryRetriever<K, V> implements EntryRetriever<K, V> {
   protected final Log log = LogFactory.getLog(this.getClass());
   protected final int batchSize;
   protected final long timeout;
   protected final TimeUnit unit;

   protected DataContainer dataContainer;
   protected PersistenceManager persistenceManager;
   protected ExecutorService executorService;
   protected Cache<K, V> cache;
   protected TimeService timeService;
   protected InternalEntryFactory entryFactory;

   protected final Executor withinThreadExecutor = new WithinThreadExecutor();

   boolean passivationEnabled;

   @Inject
   public void inject(DataContainer dataContainer, PersistenceManager persistenceManager,
                      @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService,
                      TimeService timeService, InternalEntryFactory entryFactory, Cache<K, V> cache,
                      Configuration config) {
      this.dataContainer = dataContainer;
      this.persistenceManager = persistenceManager;
      this.executorService = executorService;
      this.timeService = timeService;
      this.entryFactory = entryFactory;
      this.cache = cache;

      this.passivationEnabled = config.persistence().passivation();
   }

   public LocalEntryRetriever(int batchSize, long timeout, TimeUnit unit) {
      if (batchSize <= 0) {
         throw new IllegalArgumentException("batchSize must be greater than 0");
      }
      if (timeout <= 0) {
         throw new IllegalArgumentException("timeout must be greater than 0");
      }
      if (unit == null) {
         throw new NullPointerException("unit must not be null");
      }
      this.batchSize = batchSize;
      this.timeout = timeout;
      this.unit = unit;
   }

   @Override
   public <C> void startRetrievingValues(UUID identifier, Address origin, Set<Integer> segments,
                                            KeyValueFilter<? super K, ? super V> filter,
                                            Converter<? super K, ? super V, C> converter, Set<Flag> flags) {
      throw new UnsupportedOperationException();
   }

   protected <C> void wireFilterAndConverterDependencies(KeyValueFilter<? super K, ? super V> filter, Converter<? super K, ? super V, C> converter) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      if (filter != null) {
         componentRegistry.wireDependencies(filter);
      }
      if (converter != null && converter != filter) {
         componentRegistry.wireDependencies(converter);
      }
   }

   @Override
   public <C> void receiveResponse(UUID identifier, Address origin, Set<Integer> completedSegments, Set<Integer> inDoubtSegments,
                                   Collection<Map.Entry<K, C>> entries) {
      throw new UnsupportedOperationException();
   }

   @Listener
   protected static class PassivationListener<K, V> {
      Queue<K> activatedKeys = new ConcurrentLinkedQueue<K>();

      @CacheEntryActivated
      public void onEntryActivated(CacheEntryActivatedEvent<K, V> activatedEvent) {
         activatedKeys.add(activatedEvent.getKey());
      }
   }

   protected class KeyValueActionForCacheLoaderTask<K, V> implements AdvancedCacheLoader.CacheLoaderTask<K, V> {

      private final ParallelIterableMap.KeyValueAction<? super K, ? super InternalCacheEntry> action;

      public KeyValueActionForCacheLoaderTask(ParallelIterableMap.KeyValueAction<? super K, ? super InternalCacheEntry> action) {
         this.action = action;
      }

      @Override
      public void processEntry(MarshalledEntry<K, V> marshalledEntry,
                               AdvancedCacheLoader.TaskContext taskContext)
            throws InterruptedException {
         if (!taskContext.isStopped()) {
            action.apply(marshalledEntry.getKey(), PersistenceUtil.convert(marshalledEntry, entryFactory));
            if (Thread.interrupted()) {
               throw new InterruptedException();
            }
         }
      }
   }

   protected boolean shouldUseLoader(Set<Flag> flags) {
      return flags == null || !flags.contains(Flag.SKIP_CACHE_LOAD);
   }

   @Override
   public <C> CloseableIterator<Map.Entry<K, C>> retrieveEntries(final KeyValueFilter<? super K, ? super V> filter,
                                                                 final Converter<? super K, ? super V, ? extends C> converter,
                                                                 final Set<Flag> flags,
                                                                 final SegmentListener listener) {
      wireFilterAndConverterDependencies(filter, converter);
      final Itr<K, C> iterator = new Itr<K, C>(batchSize);
      final ItrQueuerHandler<C> handler = new ItrQueuerHandler<C>(iterator);
      executorService.submit(new Runnable() {

         @Override
         public void run() {
            try {
               final Set<K> processedKeys = new ConcurrentHashSet<K>();
               Queue<Map.Entry<K, C>> queue = new ArrayDeque<Map.Entry<K,C>>(batchSize) {
                  @Override
                  public boolean add(Map.Entry<K, C> kcEntry) {
                     processedKeys.add(kcEntry.getKey());
                     return super.add(kcEntry);
                  }
               };
               // Note we still use the batchSize here so that if we have a lot of values we return them as we see
               // them
               MapAction<C> action = new MapAction(batchSize, converter, queue, handler);

               PassivationListener<K, V> listener = null;

               try {
                  int interruptCheck = 0;
                  for (InternalCacheEntry entry : dataContainer) {
                     K key = (K) entry.getKey();
                     if (filter != null) {
                        if (!filter.accept(key, (V) entry.getValue(), entry.getMetadata())) {
                           continue;
                        }
                     }

                     action.apply(key, entry);
                     if (interruptCheck++ % batchSize == 0) {
                        if (Thread.interrupted()) {
                           throw new CacheException("Entry Iterator was interrupted!");
                        }
                     }
                  }
                  if (shouldUseLoader(flags) && persistenceManager.size() > 0) {
                     if (passivationEnabled) {
                        listener = new PassivationListener<K, V>();
                        cache.addListener(listener);
                     }
                     KeyFilter<K> loaderFilter;
                     if (filter != null) {
                        loaderFilter = new CompositeKeyFilter<K>(new CollectionKeyFilter<K>(processedKeys),
                                                                 new KeyValueFilterAsKeyFilter<K>(filter));
                     } else {
                        loaderFilter = new CollectionKeyFilter<K>(processedKeys);
                     }
                     persistenceManager.processOnAllStores(withinThreadExecutor, new KeyFilterBridge(loaderFilter),
                                                           new KeyValueActionForCacheLoaderTask(action), true, true);
                  }
               } finally {
                  if (listener != null) {
                     cache.removeListener(listener);
                     AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
                     // Now we have to check all the activated keys, as it is possible it got promoted to the
                     // in memory data container after we would have seen it
                     for (K key : listener.activatedKeys) {
                        // If we didn't process it already we have to look it up
                        if (!processedKeys.contains(key)) {
                           Map.Entry entry = advancedCache.getCacheEntry(key);
                           if (entry != null) {
                              queue.add(entry);
                           }
                        }
                     }
                  }
               }
               if (log.isTraceEnabled()) {
                  log.trace("Completed transfer of entries from cache");
               }

               handler.handleBatch(true, queue);
            } catch (Throwable e) {
               //todo [anistor] any exception happening during entry retrieval should stop the process and throw an exception to the requestor instead of timing out
               log.exceptionProcessingEntryRetrievalValues(e);
            }
         }
      });
      return iterator;
   }

   private class MapAction<C> implements ParallelIterableMap.KeyValueAction<K, InternalCacheEntry> {
      final Converter<? super K, ? super V, ? extends C> converter;
      final Queue<Map.Entry<K, C>> queue;
      final int batchSize;
      final BatchHandler<K, C> handler;

      final AtomicInteger insertionCount = new AtomicInteger();

      public MapAction(int batchSize, Converter<? super K, ? super V, ? extends C> converter, Queue<Map.Entry<K, C>> queue,
                       BatchHandler<K, C> handler)  {
         this.batchSize = batchSize;
         this.converter = converter;
         this.queue = queue;
         this.handler = handler;
      }

      @Override
      public void apply(K k, InternalCacheEntry kvInternalCacheEntry) {
         C value;
         if (converter != null) {
            value = converter.convert(k, (V) kvInternalCacheEntry.getValue(), kvInternalCacheEntry.getMetadata());
         } else {
            // If no converter, than C is undefined, so it doesn't matter
            value = (C) kvInternalCacheEntry.getValue();
         }
         // We use just an immortal cache entry since it has low serialization overhead
         queue.add((Map.Entry<K, C>)new ImmortalCacheEntry(k, value));
         if (insertionCount.incrementAndGet() % batchSize == 0) {
            try {
               handler.handleBatch(false, queue);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();;
            }
            queue.clear();
         }
      }
   }

   protected interface BatchHandler<K, C> {
      public void handleBatch(boolean complete, Collection<Map.Entry<K, C>> entries) throws InterruptedException;
   }

   protected class ItrQueuerHandler<C> implements BatchHandler<K, C> {
      final Itr<K, C> iterator;

      public ItrQueuerHandler(Itr<K, C> iterator) {
         this.iterator = iterator;
      }

      @Override
      public void handleBatch(boolean complete, Collection<Map.Entry<K, C>> entries) throws InterruptedException {
         iterator.addEntries(entries);
         if (complete) {
            iterator.close();
         }
      }
   }

   protected class Itr<K, C> implements CloseableIterator<Map.Entry<K, C>> {

      private final BlockingQueue<Map.Entry<K, C>> queue;
      private final Lock nextLock = new ReentrantLock();
      private final Condition nextCondition = nextLock.newCondition();
      private boolean completed;

      public Itr(int batchSize) {
         // This is a blocking queue so that addEntries blocks to prevent multiple batches from the same sender
         this.queue = new ArrayBlockingQueue<Map.Entry<K, C>>(batchSize);
      }

      @Override
      public boolean hasNext() {
         boolean hasNext = !queue.isEmpty();
         if (!hasNext && !completed) {
            boolean interrupted = false;
            long targetTime = timeService.expectedEndTime(timeout, unit);
            nextLock.lock();
            try {
               while (!(hasNext = !queue.isEmpty()) && !completed) {
                  try {
                     if (!nextCondition.await(timeService.remainingTime(targetTime, TimeUnit.NANOSECONDS),
                                              TimeUnit.NANOSECONDS)) {
                        if (log.isTraceEnabled()) {
                           log.tracef("Did not retrieve entries in allotted timeout: %s units: unit", timeout, unit);
                        }
                        throw new TimeoutException("Did not retrieve entries in allotted timeout: " + timeout +
                                                         " units: " + unit);
                     }
                  } catch (InterruptedException e) {
                     // If interrupted, we just loop back around
                     interrupted = true;
                  }
               }
            } finally {
               nextLock.unlock();
            }

            if (interrupted) {
               Thread.currentThread().interrupt();
            }
         }
         return hasNext;
      }

      @Override
      public Map.Entry<K, C> next() {
         Map.Entry<K, C> entry = queue.poll();
         if (entry == null) {
            if (completed) {
               throw new NoSuchElementException();
            }
            nextLock.lock();
            try {
               while ((entry = queue.poll()) == null && !completed) {
                  try {
                     nextCondition.await();
                  } catch (InterruptedException e) {
                     // If interrupted, we just loop back around
                  }
               }
               if (entry == null) {
                  throw new NoSuchElementException();
               }
            } finally {
               nextLock.unlock();
            }
         }
         return entry;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException("Remove is not supported!");
      }

      public void addEntries(Collection<Map.Entry<K, C>> entries) throws InterruptedException {
         boolean wasCompleted = completed;
         Iterator<Map.Entry<K, C>> itr = entries.iterator();
         while (!wasCompleted && itr.hasNext()) {
            // First we put as many as we can in the queue without wait blocking.  After it fills up or done we have to
            // signal others to wake up
            Map.Entry<K, C> entry = null;
            while (itr.hasNext()) {
               entry = itr.next();
               if (!queue.offer(entry)) {
                  break;
               }
               entry = null;
            }
            // Signal anyone waiting for values now
            nextLock.lock();
            try {
               wasCompleted = completed;
               nextCondition.signalAll();
            } finally {
               nextLock.unlock();
            }
            // If we broke out early from not fully iterating then we need to block until the queue has something
            // available to be inserted and then we restart again.
            if (!wasCompleted && entry != null) {
               queue.put(entry);
            }
         }
      }

      @Override
      public void close() {
         nextLock.lock();
         try {
            completed = true;
            nextCondition.signalAll();
         } finally {
            nextLock.unlock();
         }
      }
   }
}
