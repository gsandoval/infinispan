package org.infinispan.distribution.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.KeyValuePair;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link org.infinispan.distribution.ch.ConsistentHashFactory} implementation that guarantees caches with the same members
 * have the same consistent hash.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class NewSyncConsistentHashFactory implements ConsistentHashFactory<DefaultConsistentHash> {

   @Override
   public DefaultConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                                       Map<Address, Float> capacityFactors) {
      checkCapacityFactors(members, capacityFactors);

      Builder builder = new Builder(hashFunction, numOwners, numSegments, members, capacityFactors);
      List<List<KeyValuePair<Integer, Address>>> hashes = computeVirtualNodeHashes(builder);
      populateOwners(builder, hashes);

      DefaultConsistentHash consistentHash = new DefaultConsistentHash(hashFunction, numOwners, numSegments, members, capacityFactors, builder.getAllOwners());
      return consistentHash;
   }

   List<List<KeyValuePair<Integer, Address>>> computeVirtualNodeHashes(Builder builder) {
      int numVirtualNodes = Math.max(builder.getNumSegments() / builder.getSortedMembers().size(), 200);

      List<List<KeyValuePair<Integer, Address>>> hashes = new ArrayList<List<KeyValuePair<Integer, Address>>>(builder.getNumSegments());
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         hashes.add(new ArrayList<KeyValuePair<Integer, Address>>(builder.getActualNumOwners()));
      }
      for (Address node : builder.getSortedMembers()) {
         int hash0 = node.hashCode();
         for (int i = 0; i < numVirtualNodes; i++) {
            int hash = normalizedHash(builder.getHashFunction(), hash0 + i);
            int segment = hash / builder.getSegmentSize();
            KeyValuePair<Integer, Address> newHash = new KeyValuePair<Integer, Address>(hash, node);
            List<KeyValuePair<Integer, Address>> segmentHashes = hashes.get(segment);
            int position = Collections.binarySearch(segmentHashes, newHash, new Comparator<KeyValuePair<Integer, Address>>() {
               @Override
               public int compare(KeyValuePair<Integer, Address> o1, KeyValuePair<Integer, Address> o2) {
                  int keyOrder = o1.getKey() - o2.getKey();
                  return keyOrder != 0 ? keyOrder : o1.getValue().compareTo(o2.getValue());
               }
            });
            if (-builder.getActualNumOwners() <= position && position < 0) {
               if (segmentHashes.size() >= builder.getActualNumOwners()) {
                  segmentHashes.remove(segmentHashes.size() - 1);
               }
               segmentHashes.add(-position - 1, newHash);
            }
         }
      }

      return hashes;
   }

   void populateOwners(Builder builder, List<List<KeyValuePair<Integer, Address>>> hashes) {
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         List<Address> owners = builder.getOwners(segment);

         int offset = 0;
         while (owners.size() < builder.getActualNumOwners()) {
            addSegmentOwners(builder, owners, hashes.get((segment + offset) % builder.getNumSegments()));
            offset++;
         }
      }
   }

   private void addSegmentOwners(Builder builder, List<Address> owners, List<KeyValuePair<Integer, Address>> segmentHashes) {
      for (KeyValuePair<Integer, Address> hashEntry : segmentHashes) {
         if (owners.size() >= builder.getActualNumOwners())
            break;

         Address owner = hashEntry.getValue();
         if (!owners.contains(owner)) {
            owners.add(owner);
         }
      }
   }

   private void checkCapacityFactors(List<Address> members, Map<Address, Float> capacityFactors) {
      if (capacityFactors != null) {
         float totalCapacity = 0;
         for (Address node : members) {
            Float capacityFactor = capacityFactors.get(node);
            if (capacityFactor == null || capacityFactor < 0)
               throw new IllegalArgumentException("Invalid capacity factor for node " + node);
            totalCapacity += capacityFactor;
         }
         if (totalCapacity == 0)
            throw new IllegalArgumentException("There must be at least one node with a non-zero capacity factor");
      }
   }

   protected int normalizedHash(Hash hashFunction, int hashcode) {
      return hashFunction.hash(hashcode) & Integer.MAX_VALUE;
   }

   @Override
   public DefaultConsistentHash updateMembers(DefaultConsistentHash baseCH, List<Address> newMembers,
                                              Map<Address, Float> actualCapacityFactors) {
      checkCapacityFactors(newMembers, actualCapacityFactors);

      // The ConsistentHashFactory contract says we should return the same instance if we're not making changes
      boolean sameCapacityFactors = actualCapacityFactors == null ? baseCH.getCapacityFactors() == null :
            actualCapacityFactors.equals(baseCH.getCapacityFactors());
      if (newMembers.equals(baseCH.getMembers()) && sameCapacityFactors)
         return baseCH;

      int numSegments = baseCH.getNumSegments();
      int numOwners = baseCH.getNumOwners();

      // We assume leavers are far fewer than members, so it makes sense to check for leavers
      HashSet<Address> leavers = new HashSet<Address>(baseCH.getMembers());
      leavers.removeAll(newMembers);

      // Create a new "balanced" CH in case we need to allocate new owners for segments with 0 owners
      DefaultConsistentHash rebalancedCH = null;

      // Remove leavers
      List<Address>[] newSegmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = new ArrayList<Address>(baseCH.locateOwnersForSegment(i));
         owners.removeAll(leavers);
         if (!owners.isEmpty()) {
            newSegmentOwners[i] = owners;
         } else {
            // this segment has 0 owners, fix it
            if (rebalancedCH == null) {
               rebalancedCH = create(baseCH.getHashFunction(), numOwners, numSegments, newMembers, actualCapacityFactors);
            }
            newSegmentOwners[i] = rebalancedCH.locateOwnersForSegment(i);
         }
      }

      return new DefaultConsistentHash(baseCH.getHashFunction(), numOwners, numSegments, newMembers,
            actualCapacityFactors, newSegmentOwners);
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {
      DefaultConsistentHash rebalancedCH = create(baseCH.getHashFunction(), baseCH.getNumOwners(),
            baseCH.getNumSegments(), baseCH.getMembers(), baseCH.getCapacityFactors());

      // the ConsistentHashFactory contract says we should return the same instance if we're not making changes
      if (rebalancedCH.equals(baseCH))
         return baseCH;

      return rebalancedCH;
   }

   @Override
   public DefaultConsistentHash union(DefaultConsistentHash ch1, DefaultConsistentHash ch2) {
      return ch1.union(ch2);
   }

   protected static class Builder {
      private final Hash hashFunction;
      private final int numOwners;
      private final Map<Address, Float> capacityFactors;
      private final int actualNumOwners;
      private final int numSegments;
      private final List<Address> sortedMembers;
      private final int segmentSize;
      private final List<Address>[] segmentOwners;

      private Builder(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                      Map<Address, Float> capacityFactors) {
         this.hashFunction = hashFunction;
         this.numSegments = numSegments;
         this.numOwners = numOwners;
         this.capacityFactors = capacityFactors;
         this.actualNumOwners = Math.min(numOwners, members.size());
         this.sortedMembers = sort(members, capacityFactors);
         this.segmentSize = (int) Math.ceil((double) Integer.MAX_VALUE / numSegments);
         this.segmentOwners = new List[numSegments];
         for (int i = 0; i < numSegments; i++) {
            segmentOwners[i] = new ArrayList<Address>(actualNumOwners);
         }
      }

      public Hash getHashFunction() {
         return hashFunction;
      }

      public int getNumOwners() {
         return numOwners;
      }

      public int getActualNumOwners() {
         return actualNumOwners;
      }

      public int getNumSegments() {
         return numSegments;
      }

      public List<Address> getSortedMembers() {
         return sortedMembers;
      }

      public int getSegmentSize() {
         return segmentSize;
      }

      public List<Address>[] getAllOwners() {
         return segmentOwners;
      }

      public List<Address> getOwners(int i) {
         return segmentOwners[i];
      }

      public float getCapacityFactor(Address node) {
         return capacityFactors != null ? capacityFactors.get(node) : 1;
      }

      private List<Address> sort(List<Address> members, final Map<Address, Float> capacityFactors) {
         ArrayList<Address> result = new ArrayList<Address>(members);
         Collections.sort(result, new Comparator<Address>() {
            @Override
            public int compare(Address o1, Address o2) {
               // Sort descending by capacity factor and ascending by address (UUID)
               int capacityComparison = capacityFactors != null ? capacityFactors.get(o1).compareTo(capacityFactors.get(o2)) : 0;
               return capacityComparison != 0 ? -capacityComparison : o1.compareTo(o2);
            }
         });
         return result;
      }

      private int nodesWithLoad() {
         int nodesWithLoad = sortedMembers.size();
         if (capacityFactors != null) {
            nodesWithLoad = 0;
            for (Address node : sortedMembers) {
               if (capacityFactors.get(node) != 0) {
                  nodesWithLoad++;
               }
            }
         }
         return nodesWithLoad;
      }
   }

   public static class Externalizer extends AbstractExternalizer<NewSyncConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, NewSyncConsistentHashFactory chf) {
      }

      @Override
      @SuppressWarnings("unchecked")
      public NewSyncConsistentHashFactory readObject(ObjectInput unmarshaller) {
         return new NewSyncConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.SYNC_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends NewSyncConsistentHashFactory>> getTypeClasses() {
         return Collections.<Class<? extends NewSyncConsistentHashFactory>>singleton(NewSyncConsistentHashFactory.class);
      }
   }
}
