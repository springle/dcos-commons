package com.mesosphere.sdk.offer;

import com.google.protobuf.TextFormat;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.DiskInfo;
import org.apache.mesos.Protos.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A representation of the pool of resources available in a single {@link Offer}. Tracks the
 * consumption of the {@link Offer}'s resources, as they are matched with
 * {@link ResourceRequirement}s.
 */
public class MesosResourcePool {
    private static final Logger logger = LoggerFactory.getLogger(MesosResourcePool.class);

    private Offer offer;
    private Map<String, List<MesosResource>> unreservedAtomicPool;
    private Map<String, MesosResource> dynamicallyReservedPool;
    private Map<String, Map<String, Value>> reservableMergedPool;

    /**
     * Creates a new pool of resources based on what's available in the provided {@link Offer}.
     */
    public MesosResourcePool(Offer offer) {
        init(offer);
    }

    private void init(Offer offer) {
        this.offer = offer;
        final Collection<MesosResource> mesosResources = getMesosResources(offer);
        this.unreservedAtomicPool = getUnreservedAtomicPool(mesosResources);
        this.dynamicallyReservedPool = getDynamicallyReservedPool(mesosResources);
        this.reservableMergedPool = getReservableMergedPool(mesosResources);
    }

    /**
     * Returns the underlying offer which this resource pool represents.
     */
    public Offer getOffer() {
        return offer;
    }

    /**
     * Returns the unreserved resources which cannot be partially consumed from an Offer. For
     * example, a MOUNT volume cannot be partially consumed, it's all-or-nothing.
     */
    public Map<String, List<MesosResource>> getUnreservedAtomicPool() {
        return unreservedAtomicPool;
    }

    /**
     * Returns the resources which were dynamically reserved.
     */
    public Map<String, MesosResource> getDynamicallyReservedPool() {
        return dynamicallyReservedPool;
    }

    /**
     * Returns the resources which are reservable.  These may have been pre-reserved (dynamically or statically) or
     * never reserved.
     */
    public Map<String, Map<String, Value>> getReservableMergedPool() {
        return reservableMergedPool;
    }

    public Map<String, Value> getUnreservedMergedPool() {
        return reservableMergedPool.get(Constants.DEFAULT_MESOS_ROLE);
    }

    /**
     * Returns the reserved resource, if present.
     */
    public Optional<MesosResource> getReservedResourceById(String resourceId) {
        return Optional.ofNullable(dynamicallyReservedPool.get(resourceId));
    }

    /**
     * Consumes and returns a {@link MesosResource} which meets the provided
     * {@link ResourceRequirement}, or does nothing and returns an empty {@link Optional} if no
     * available resources meet the requirement.
     */
    public Optional<MesosResource> consume(ResourceRequirement resourceRequirement) {
        if (resourceRequirement.expectsResource()) {
            return consumeReserved(resourceRequirement);
        } else if (resourceRequirement.isAtomic()) {
            return consumeAtomic(resourceRequirement);
        } else if (resourceRequirement.reservesResource()) {
            return consumeReservableMerged(resourceRequirement);
        }

        logger.error("The following resource requirement did not meet any consumption criteria: {}",
                TextFormat.shortDebugString(resourceRequirement.getResource()));
        return Optional.empty();
    }

    /**
     * Update the offer this pool represents, re-calculating available unreserved, reserved and atomic resources.
     * @param offer the offer to encapsulate
     */
    public void update(Offer offer) {
        init(offer);
    }

    private void releaseAtomicResource(MesosResource mesosResource) {
        Resource.Builder resBuilder = Resource.newBuilder(mesosResource.getResource());
        resBuilder.clearReservation();
        resBuilder.setRole(Constants.DEFAULT_MESOS_ROLE);

        if (resBuilder.hasDisk()) {
            DiskInfo.Builder diskBuilder = DiskInfo.newBuilder(resBuilder.getDisk());
            diskBuilder.clearPersistence();
            diskBuilder.clearVolume();
            resBuilder.setDisk(diskBuilder.build());
        }

        Resource releasedResource = resBuilder.build();

        List<MesosResource> resList = unreservedAtomicPool.get(mesosResource.getName());
        if (resList == null) {
            resList = new ArrayList<MesosResource>();
        }

        resList.add(new MesosResource(releasedResource));
        unreservedAtomicPool.put(mesosResource.getName(), resList);
    }

    private Optional<MesosResource> consumeReserved(ResourceRequirement resourceRequirement) {
        MesosResource mesosResource = dynamicallyReservedPool.get(resourceRequirement.getResourceId().get());

        if (mesosResource != null) {
            if (mesosResource.isAtomic()) {
                if (sufficientValue(resourceRequirement.getValue(), mesosResource.getValue())) {
                    dynamicallyReservedPool.remove(resourceRequirement.getResourceId());
                } else {
                    logger.warn("Reserved atomic quantity of {} is insufficient: desired {}, reserved {}",
                            resourceRequirement.getName(),
                            TextFormat.shortDebugString(resourceRequirement.getValue()),
                            TextFormat.shortDebugString(mesosResource.getValue()));
                    return Optional.empty();
                }
            } else {
                Value desiredValue = resourceRequirement.getValue();
                Value availableValue = mesosResource.getValue();
                if (ValueUtils.compare(availableValue, desiredValue) > 0) {
                    // update the value in pool with the remaining unclaimed resource amount
                    Resource remaining = ResourceUtils.setValue(
                            mesosResource.getResource(), ValueUtils.subtract(availableValue, desiredValue));
                    dynamicallyReservedPool.put(
                            resourceRequirement.getResourceId().get(),
                            new MesosResource(remaining));
                    // return only the claimed resource amount from this reservation
                    mesosResource = new MesosResource(resourceRequirement.getResource());
                } else {
                    dynamicallyReservedPool.remove(resourceRequirement.getResourceId().get());
                }
            }
        } else {
           logger.warn("Failed to find reserved {} resource with ID: {}. Reserved resource IDs are: {}",
                   resourceRequirement.getName(),
                   resourceRequirement.getResourceId(),
                   dynamicallyReservedPool.keySet());
        }

        return Optional.ofNullable(mesosResource);
    }

    private Optional<MesosResource> consumeAtomic(ResourceRequirement resourceRequirement) {
        Value desiredValue = resourceRequirement.getValue();
        List<MesosResource> atomicResources = unreservedAtomicPool.get(resourceRequirement.getName());
        List<MesosResource> filteredResources = new ArrayList<>();
        Optional<MesosResource> sufficientResource = Optional.empty();

        if (atomicResources != null) {
            for (MesosResource atomicResource : atomicResources) {
                if (!sufficientResource.isPresent() && sufficientValue(desiredValue, atomicResource.getValue())) {
                    sufficientResource = Optional.of(atomicResource);
                    // do NOT break: ensure filteredResources is fully populated
                } else {
                    filteredResources.add(atomicResource);
                }
            }
        }

        if (filteredResources.isEmpty()) {
            unreservedAtomicPool.remove(resourceRequirement.getName());
        } else {
            unreservedAtomicPool.put(resourceRequirement.getName(), filteredResources);
        }

        if (!sufficientResource.isPresent()) {
            if (atomicResources == null) {
                logger.info("Offer lacks any atomic resources named {}", resourceRequirement.getName());
            } else {
                logger.info("Offered quantity in all {} instances of {} is insufficient: desired {}",
                        atomicResources.size(),
                        resourceRequirement.getName(),
                        TextFormat.shortDebugString(resourceRequirement.getResource()));
            }
        }

        return sufficientResource;
    }

    private Optional<MesosResource> consumeReservableMerged(ResourceRequirement resourceRequirement) {
        Value desiredValue = resourceRequirement.getValue();
        Map<String, Value> pool = reservableMergedPool.get(resourceRequirement.getRole());
        if (pool == null) {
            logger.info("No unreserved resources available in role: {}", resourceRequirement.getRole());
            return Optional.empty();
        }

        Value availableValue = pool.get(resourceRequirement.getName());

        if (sufficientValue(desiredValue, availableValue)) {
            pool.put(resourceRequirement.getName(), ValueUtils.subtract(availableValue, desiredValue));
            reservableMergedPool.put(resourceRequirement.getRole(), pool);
            Resource resource = ResourceUtils.getUnreservedResource(
                    resourceRequirement.getName(),
                    desiredValue,
                    resourceRequirement.getRole());
            return Optional.of(new MesosResource(resource));
        } else {
            if (availableValue == null) {
                logger.info("Offer lacks any resources named {}", resourceRequirement.getName());
            } else {
                logger.info("Offered quantity of {} is insufficient: desired {}, offered {}",
                        resourceRequirement.getName(),
                        TextFormat.shortDebugString(desiredValue),
                        TextFormat.shortDebugString(availableValue));
            }
            return Optional.empty();
        }
    }

    private static boolean sufficientValue(Value desired, Value available) {
        if (desired == null) {
            return true;
        } else if (available == null) {
            return false;
        }

        Value difference = ValueUtils.subtract(desired, available);
        return ValueUtils.compare(difference, ValueUtils.getZero(desired.getType())) <= 0;
    }

    private static Collection<MesosResource> getMesosResources(Offer offer) {
        Collection<MesosResource> mesosResources = new ArrayList<MesosResource>();

        for (Resource resource : offer.getResourcesList()) {
            mesosResources.add(new MesosResource(resource));
        }

        return mesosResources;
    }

    private static Map<String, MesosResource> getDynamicallyReservedPool(
            Collection<MesosResource> mesosResources) {
        Map<String, MesosResource> reservedPool = new HashMap<String, MesosResource>();

        for (MesosResource mesResource : mesosResources) {
            if (mesResource.hasResourceId()) {
                reservedPool.put(mesResource.getResourceId().get(), mesResource);
            }
        }

        return reservedPool;
    }

    private Map<String, Map<String, Value>> getReservableMergedPool(Collection<MesosResource> mesosResources) {
        Map<String, List<MesosResource>> rolePool = new HashMap<>();
        for (MesosResource mesosResource : getMergedResources(mesosResources)) {
            if (!mesosResource.hasResourceId()) {
                String role = mesosResource.getRole();
                List<MesosResource> resources = rolePool.get(role);
                if (resources == null) {
                    resources = new ArrayList<>();
                }

                resources.add(mesosResource);
                rolePool.put(role, resources);
            }
        }

        Map<String, Map<String, Value>> roleResourcePool = new HashMap<>();
        for (Map.Entry<String, List<MesosResource>> entry : rolePool.entrySet()) {
            roleResourcePool.put(entry.getKey(), getResourcePool(entry.getValue()));
        }

        return roleResourcePool;
    }

    private static Map<String, List<MesosResource>> getUnreservedAtomicPool(
            Collection<MesosResource> mesosResources) {
        Map<String, List<MesosResource>> pool = new HashMap<String, List<MesosResource>>();

        for (MesosResource mesosResource : getUnreservedAtomicResources(mesosResources)) {
            String name = mesosResource.getName();
            List<MesosResource> resList = pool.get(name);

            if (resList == null) {
                resList = new ArrayList<MesosResource>();
            }

            resList.add(mesosResource);
            pool.put(name, resList);
        }

        return pool;
    }

    private static Map<String, Value> getResourcePool(Collection<MesosResource> mesosResources) {
        Map<String, Value> pool = new HashMap<>();
        for (MesosResource mesosResource : mesosResources) {
            String name = mesosResource.getName();
            Value currValue = pool.get(name);

            if (currValue == null) {
                currValue = ValueUtils.getZero(mesosResource.getType());
            }

            pool.put(name, ValueUtils.add(currValue, mesosResource.getValue()));
        }

        return pool;
    }

    private static Collection<MesosResource> getUnreservedAtomicResources(
            Collection<MesosResource> mesosResources) {
        return getUnreservedResources(getAtomicResources(mesosResources));
    }

    private static Collection<MesosResource> getUnreservedMergedResources(
            Collection<MesosResource> mesosResources) {
        return getUnreservedResources(getMergedResources(mesosResources));
    }

    private static Collection<MesosResource> getUnreservedResources(
            Collection<MesosResource> mesosResources) {
        Collection<MesosResource> unreservedResources = new ArrayList<MesosResource>();

        for (MesosResource mesosResource : mesosResources) {
            if (mesosResource.isUnreserved()) {
                unreservedResources.add(mesosResource);
            }
        }

        return unreservedResources;
    }

    private static Collection<MesosResource> getAtomicResources(
            Collection<MesosResource> mesosResources) {
        Collection<MesosResource> atomicResources = new ArrayList<>();

        for (MesosResource mesosResource : mesosResources) {
            if (mesosResource.isAtomic()) {
                atomicResources.add(mesosResource);
            }
        }

        return atomicResources;
    }

    private static Collection<MesosResource> getMergedResources(Collection<MesosResource> mesosResources) {
        Collection<MesosResource> mergedResources = new ArrayList<>();

        for (MesosResource mesosResource : mesosResources) {
            if (!mesosResource.isAtomic()) {
                mergedResources.add(mesosResource);
            }
        }

        return mergedResources;
    }
}
