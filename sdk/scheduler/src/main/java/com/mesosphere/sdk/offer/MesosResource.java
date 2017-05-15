package com.mesosphere.sdk.offer;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.DiskInfo.Source;
import org.apache.mesos.Protos.Value;

/**
 * Wrapper around a Mesos {@link Resource}, combined with a resource ID string which should be present in the
 * {@link Resource} as a {@link Label}.
 **/
public class MesosResource {
    public static final String RESOURCE_ID_KEY = "resource_id";
    public static final String DYNAMIC_PORT_KEY = "dynamic_port";
    public static final String VIP_LABEL_NAME_KEY = "vip_key";
    public static final String VIP_LABEL_VALUE_KEY = "vip_value";

    private final Resource resource;
    private final String resourceId;

    public MesosResource(Resource resource) {
        this.resource = resource;
        this.resourceId = getResourceIdInternal(resource);
    }

    public Resource getResource() {
        return resource;
    }

    public boolean isAtomic() {
        return resource.hasDisk()
            && resource.getDisk().hasSource()
            && resource.getDisk().getSource().getType().equals(Source.Type.MOUNT);
    }

    public String getName() {
        return resource.getName();
    }

    public Value.Type getType() {
        return resource.getType();
    }

    public boolean hasResourceId() {
        return resourceId != null;
    }

    public String getResourceId() {
        return resourceId;
    }

    public boolean hasReservation() {
        return resource.getReservationsCount() > 0 || resource.hasReservation();
    }

    public boolean isUnreserved() {
        if (resource.hasRole() && resource.getRole().equals(Constants.DEFAULT_MESOS_ROLE)) {
            return true;
        }

        if (!resource.hasReservation() && resource.getReservationsCount() == 0) {
            return true;
        }

        return false;
    }

    public boolean isReserved() {
        return !isUnreserved();
    }

    public String getRole() {
        // Default to last element of reservations stack.
        if (resource.getReservationsCount() > 0) {
            int lastIndex = resource.getReservationsCount() - 1;
            return resource.getReservations(lastIndex).getRole();
        }

        // Fall back to deprecated single reservation specified in resource element.
        if (resource.hasReservation() && resource.getReservation().hasRole()) {
            return resource.getReservation().getRole();
        }

        // Fall back to role specified at resource level
        if (resource.hasRole()) {
            return resource.getRole();
        }

        return Constants.DEFAULT_MESOS_ROLE;
    }

    public Value getValue() {
        return ValueUtils.getValue(resource);
    }

    public String getPrincipal() {
        if (resource.hasReservation() && resource.getReservation().hasPrincipal()) {
            return resource.getReservation().getPrincipal();
        }

        return null;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    private static String getResourceIdInternal(Resource resource) {
        if (resource.hasReservation()) {
            for (Label label : resource.getReservation().getLabels().getLabelsList()) {
                if (label.getKey().equals(RESOURCE_ID_KEY)) {
                    return label.getValue();
                }
            }
        }
        return null;
    }
}
