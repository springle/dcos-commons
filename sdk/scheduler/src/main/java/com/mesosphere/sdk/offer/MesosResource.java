package com.mesosphere.sdk.offer;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.DiskInfo.Source;
import org.apache.mesos.Protos.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
    private final Optional<String> resourceId;

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
        return resourceId.isPresent();
    }

    public Optional<String> getResourceId() {
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

    private static Optional<String> getResourceIdInternal(Resource resource) {
        final List<Resource.ReservationInfo> reservationInfos = new ArrayList<>();
        if (resource.hasReservation() && resource.getReservation().hasLabels()) {
            reservationInfos.add(resource.getReservation());
        }

        for (Resource.ReservationInfo reservationInfo : resource.getReservationsList()) {
            if (reservationInfo.hasLabels()) {
                reservationInfos.add(reservationInfo);
            }
        }

        List<Resource.ReservationInfo> reversedReservations = Lists.reverse(reservationInfos);
        return reversedReservations.stream()
                .map(reservationInfo -> getResourceIdInternal(reservationInfo.getLabels()))
                .filter(resourceId -> resourceId.isPresent())
                .map(resourceId -> resourceId.get())
                .findFirst();
    }

    private static Optional<String> getResourceIdInternal(Protos.Labels labels) {
        return labels.getLabelsList().stream()
                .filter(label -> label.getKey().equals(RESOURCE_ID_KEY))
                .map(label -> label.getValue())
                .filter(id -> !id.isEmpty())
                .findFirst();
    }
}
