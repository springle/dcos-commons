package com.mesosphere.sdk.offer.evaluate;

import com.google.inject.Inject;
import com.mesosphere.sdk.offer.*;
import com.mesosphere.sdk.scheduler.SchedulerFlags;
import com.mesosphere.sdk.scheduler.plan.PodInstanceRequirement;
import com.mesosphere.sdk.scheduler.recovery.FailureUtils;
import com.mesosphere.sdk.scheduler.recovery.RecoveryType;
import com.mesosphere.sdk.specification.*;
import com.mesosphere.sdk.state.StateStore;
import com.mesosphere.sdk.state.StateStoreException;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The OfferEvaluator processes {@link Protos.Offer}s and produces {@link OfferRecommendation}s.
 * The determination of what {@link OfferRecommendation}s, if any should be made are made
 * in reference to {@link PodInstanceRequirement}s.
 */
public class OfferEvaluator {
    private static final Logger logger = LoggerFactory.getLogger(OfferEvaluator.class);

    private final StateStore stateStore;
    private final String serviceName;
    private final UUID targetConfigId;
    private final SchedulerFlags schedulerFlags;

    @Inject
    public OfferEvaluator(
            StateStore stateStore,
            String serviceName,
            UUID targetConfigId,
            SchedulerFlags schedulerFlags) {
        this.stateStore = stateStore;
        this.serviceName = serviceName;
        this.targetConfigId = targetConfigId;
        this.schedulerFlags = schedulerFlags;
    }

    public List<OfferRecommendation> evaluate(PodInstanceRequirement podInstanceRequirement, List<Protos.Offer> offers)
            throws StateStoreException, InvalidRequirementException {

        // All tasks in the service (used by some PlacementRules):
        Map<String, Protos.TaskInfo> allTasks = stateStore.fetchTasks().stream()
                .collect(Collectors.toMap(Protos.TaskInfo::getName, Function.identity()));
        // Preexisting tasks for this pod (if any):
        Map<String, Protos.TaskInfo> thisPodTasks =
                TaskUtils.getTaskNames(podInstanceRequirement.getPodInstance()).stream()
                .map(taskName -> allTasks.get(taskName))
                .filter(taskInfo -> taskInfo != null)
                .collect(Collectors.toMap(Protos.TaskInfo::getName, Function.identity()));

        boolean anyTaskIsRunning = thisPodTasks.values().stream()
                .map(taskInfo -> taskInfo.getName())
                .map(taskName -> stateStore.fetchStatus(taskName))
                .filter(Optional::isPresent)
                .map(taskStatus -> taskStatus.get())
                .filter(taskStatus -> taskStatus.getState().equals(Protos.TaskState.TASK_RUNNING))
                .count() > 0;

        Optional<Protos.ExecutorID> executorId = Optional.empty();
        if (anyTaskIsRunning) {
            executorId = Optional.of(thisPodTasks.values().stream().findFirst().get().getExecutor().getExecutorId());
        }

        for (int i = 0; i < offers.size(); ++i) {
            List<OfferEvaluationStage> evaluationStages =
                    getEvaluationPipeline(podInstanceRequirement, allTasks.values(), thisPodTasks, executorId);

            Protos.Offer offer = offers.get(i);
            MesosResourcePool resourcePool = new MesosResourcePool(offer);
            PodInfoBuilder podInfoBuilder = new PodInfoBuilder(
                    podInstanceRequirement,
                    serviceName,
                    targetConfigId,
                    schedulerFlags,
                    thisPodTasks.values(),
                    executorId);
            List<EvaluationOutcome> outcomes = new ArrayList<>();
            int failedOutcomeCount = 0;

            for (OfferEvaluationStage evaluationStage : evaluationStages) {
                EvaluationOutcome outcome = evaluationStage.evaluate(resourcePool, podInfoBuilder);
                outcomes.add(outcome);
                if (!outcome.isPassing()) {
                    failedOutcomeCount++;
                }
            }

            StringBuilder outcomeDetails = new StringBuilder();
            for (EvaluationOutcome outcome : outcomes) {
                logOutcome(outcomeDetails, outcome, "");
            }
            if (outcomeDetails.length() != 0) {
                // trim extra trailing newline:
                outcomeDetails.deleteCharAt(outcomeDetails.length() - 1);
            }

            if (failedOutcomeCount != 0) {
                logger.info("Offer {}: failed {} of {} evaluation stages:\n{}",
                        i + 1, failedOutcomeCount, evaluationStages.size(), outcomeDetails.toString());
            } else {
                List<OfferRecommendation> recommendations = outcomes.stream()
                        .map(outcome -> outcome.getOfferRecommendations())
                        .flatMap(xs -> xs.stream())
                        .collect(Collectors.toList());
                logger.info("Offer {}: passed all {} evaluation stages, returning {} recommendations:\n{}",
                        i + 1, evaluationStages.size(), recommendations.size(), outcomeDetails.toString());
                return recommendations;
            }
        }

        return Collections.emptyList();
    }

    public List<OfferEvaluationStage> getEvaluationPipeline(
            PodInstanceRequirement podInstanceRequirement,
            Collection<Protos.TaskInfo> allTasks,
            Map<String, Protos.TaskInfo> thisPodTasks,
            Optional<Protos.ExecutorID> executorID) {
        List<OfferEvaluationStage> evaluationPipeline = new ArrayList<>();
        if (podInstanceRequirement.getPodInstance().getPod().getPlacementRule().isPresent()) {
            evaluationPipeline.add(new PlacementRuleEvaluationStage(
                    allTasks, podInstanceRequirement.getPodInstance().getPod().getPlacementRule().get()));
        }

        // TODO: Replace Executor Evaluation
        PodInstance podInstance = podInstanceRequirement.getPodInstance();
        boolean noLaunchedTasksExist = thisPodTasks.values().stream()
                .flatMap(taskInfo -> taskInfo.getResourcesList().stream())
                .map(resource -> ResourceUtils.getResourceId(resource))
                .filter(resourceId -> resourceId != null && !resourceId.isEmpty())
                .count() == 0;

        boolean podHasFailed = podInstanceRequirement.getRecoveryType().equals(RecoveryType.PERMANENT)
                || FailureUtils.isLabeledAsFailed(podInstance, stateStore);

        final String description;
        final boolean shouldGetNewRequirement;
        if (podHasFailed) {
            description = "failed";
            shouldGetNewRequirement = true;
        } else if (noLaunchedTasksExist) {
            description = "new";
            shouldGetNewRequirement = true;
        } else {
            description = "existing";
            shouldGetNewRequirement = false;
        }
        logger.info("Generating requirement for {} pod '{}' containing tasks: {}",
                description, podInstance.getName(), podInstanceRequirement.getTasksToLaunch());

        if (shouldGetNewRequirement) {
            evaluationPipeline.addAll(getNewEvaluationPipeline(podInstanceRequirement));
        } else {
            evaluationPipeline.addAll(getExistingEvaluationPipeline(podInstanceRequirement, thisPodTasks, executorID));
        }

        return evaluationPipeline;
    }

    private static void logOutcome(StringBuilder stringBuilder, EvaluationOutcome outcome, String indent) {
        stringBuilder.append(String.format("  %s%s%n", indent, outcome.toString()));
        for (EvaluationOutcome child : outcome.getChildren()) {
            logOutcome(stringBuilder, child, indent + "  ");
        }
    }

    private static List<OfferEvaluationStage> getNewEvaluationPipeline(PodInstanceRequirement podInstanceRequirement) {
        Map<ResourceSet, String> resourceSets = podInstanceRequirement.getPodInstance().getPod().getTasks().stream()
                .collect(Collectors.toMap(TaskSpec::getResourceSet, TaskSpec::getName));

        List<OfferEvaluationStage> evaluationStages = new ArrayList<>();
        for (Map.Entry<ResourceSet, String> entry : resourceSets.entrySet()) {
            String taskName = entry.getValue();
            List<ResourceSpec> resourceSpecs = getOrderedResourceSpecs(entry.getKey());
            for (ResourceSpec resourceSpec : resourceSpecs) {
                if (resourceSpec instanceof NamedVIPSpec) {
                    evaluationStages.add(
                            new NamedVIPEvaluationStage((NamedVIPSpec) resourceSpec, taskName, Optional.empty()));
                } else if (resourceSpec instanceof PortSpec) {
                    evaluationStages.add(new PortEvaluationStage((PortSpec) resourceSpec, taskName, Optional.empty()));
                } else {
                    evaluationStages.add(new ResourceEvaluationStage(resourceSpec, Optional.empty(), taskName));
                }
            }

            for (VolumeSpec volumeSpec : entry.getKey().getVolumes()) {
                evaluationStages.add(
                        new VolumeEvaluationStage(volumeSpec, taskName, Optional.empty(), Optional.empty()));
            }

            boolean shouldBeLaunched = podInstanceRequirement.getTasksToLaunch().contains(taskName);
            evaluationStages.add(new LaunchEvaluationStage(taskName, shouldBeLaunched));
        }

        return evaluationStages;
    }

    private static List<ResourceSpec> getOrderedResourceSpecs(ResourceSet resourceSet) {
        // Statically defined ports, then dynamic ports, then everything else
        List<ResourceSpec> staticPorts = new ArrayList<>();
        List<ResourceSpec> dynamicPorts = new ArrayList<>();
        List<ResourceSpec> simpleResources = new ArrayList<>();

        for (ResourceSpec resourceSpec : resourceSet.getResources()) {
            if (resourceSpec instanceof PortSpec) {
                if (((PortSpec) resourceSpec).getPort() == 0) {
                    dynamicPorts.add(resourceSpec);
                } else {
                    staticPorts.add(resourceSpec);
                }
            } else {
                simpleResources.add(resourceSpec);
            }
        }

        List<ResourceSpec> resourceSpecs = new ArrayList<>();
        resourceSpecs.addAll(staticPorts);
        resourceSpecs.addAll(dynamicPorts);
        resourceSpecs.addAll(simpleResources);
        return resourceSpecs;
    }

    private static List<OfferEvaluationStage> getExistingEvaluationPipeline(
            PodInstanceRequirement podInstanceRequirement,
            Map<String, Protos.TaskInfo> podTasks,
            Optional<Protos.ExecutorID> executorID) {

        List<OfferEvaluationStage> evaluationStages = new ArrayList<>();
        List<TaskSpec> taskSpecs = podInstanceRequirement.getPodInstance().getPod().getTasks().stream()
                .filter(taskSpec -> podInstanceRequirement.getTasksToLaunch().contains(taskSpec.getName()))
                .collect(Collectors.toList());

        for (TaskSpec taskSpec : taskSpecs) {
            String taskInfoName = TaskSpec.getInstanceName(podInstanceRequirement.getPodInstance(), taskSpec.getName());
            Protos.TaskInfo taskInfo = podTasks.get(taskInfoName);
            if (taskInfo == null) {
                logger.error(String.format("Failed to fetch task %s.  Cannot generate resource map.", taskInfoName));
                return Collections.emptyList();
            }

            TaskResourceMapper taskResourceMapper = new TaskResourceMapper(taskSpec, taskInfo);
            taskResourceMapper.getOrphanedResources()
                    .forEach(resource -> evaluationStages.add(new UnreserveEvaluationStage(resource)));
            evaluationStages.addAll(taskResourceMapper.getEvaluationStages());

            boolean shouldLaunch = podInstanceRequirement.getTasksToLaunch().contains(taskSpec.getName());
            evaluationStages.add(new LaunchEvaluationStage(taskSpec.getName(), shouldLaunch));
        }

        return evaluationStages;
    }
}
