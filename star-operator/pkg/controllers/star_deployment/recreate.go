package star_deployment

import (
	"context"
	sr "github.com/shileifu/startrek/star-operator/pkg/apis/v1alpha1"
	deploymentutil "github.com/shileifu/startrek/star-operator/pkg/controllers/star_deployment/util/deployment_util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func (r *StarDeploymentReconciler) rolloutRecreate(ctx context.Context, d *sr.StarDeployment, rsList []*sr.StarReplicaSet, podMap map[types.UID][]*v1.Pod) error {
	// Don't create a new RS if not already existed, so that we avoid scaling up before scaling down.
	newRS, oldRSs, err := r.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, false)
	if err != nil {
		return err
	}
	allRSs := append(oldRSs, newRS)
	activeOldRSs := deploymentutil.FilterActiveReplicaSets(oldRSs)

	// scale down old replica sets.
	scaledDown, err := r.scaleDownOldReplicaSetsForRecreate(ctx, activeOldRSs, d)
	if err != nil {
		return err
	}
	if scaledDown {
		// Update DeploymentStatus.
		return r.syncRolloutStatus(ctx, allRSs, newRS, d)
	}

	// Do not process a deployment when it has old pods running.
	if oldPodsRunning(newRS, oldRSs, podMap) {
		return r.syncRolloutStatus(ctx, allRSs, newRS, d)
	}

	// If we need to create a new RS, create it now.
	if newRS == nil {
		newRS, oldRSs, err = r.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, true)
		if err != nil {
			return err
		}
		allRSs = append(oldRSs, newRS)
	}
	// scale up new replica set.
	if _, err := r.scaleUpNewReplicaSetForRecreate(ctx, newRS, d); err != nil {
		return err
	}

	if deploymentutil.DeploymentComplete(d, &d.Status) {
		if err := r.cleanupDeployment(ctx, oldRSs, d); err != nil {
			return err
		}
	}

	// Sync deployment status.
	return r.syncRolloutStatus(ctx, allRSs, newRS, d)
}

// oldPodsRunning returns whether there are old pods running or any of the old ReplicaSets thinks that it runs pods.
func oldPodsRunning(newRS *sr.StarReplicaSet, oldRSs []*sr.StarReplicaSet, podMap map[types.UID][]*v1.Pod) bool {
	if oldPods := deploymentutil.GetActualReplicaCountForReplicaSets(oldRSs); oldPods > 0 {
		return true
	}
	for rsUID, podList := range podMap {
		// If the pods belong to the new ReplicaSet, ignore.
		if newRS != nil && newRS.UID == rsUID {
			continue
		}
		for _, pod := range podList {
			switch pod.Status.Phase {
			case v1.PodFailed, v1.PodSucceeded:
				// Don't count pods in terminal state.
				continue
			default:
				// Pod is not in terminal phase.
				return true
			}
		}
	}
	return false
}

// scaleDownOldReplicaSetsForRecreate scales down old replica sets when deployment strategy is "Recreate".
func (dc *StarDeploymentReconciler) scaleDownOldReplicaSetsForRecreate(ctx context.Context, oldRSs []*sr.StarReplicaSet, deployment *sr.StarDeployment) (bool, error) {
	scaled := false
	for i := range oldRSs {
		rs := oldRSs[i]
		// Scaling not required.
		if *(rs.Spec.Replicas) == 0 {
			continue
		}
		scaledRS, updatedRS, err := dc.scaleReplicaSetAndRecordEvent(ctx, rs, 0, deployment)
		if err != nil {
			return false, err
		}
		if scaledRS {
			oldRSs[i] = updatedRS
			scaled = true
		}
	}
	return scaled, nil
}

// scaleUpNewReplicaSetForRecreate scales up new replica set when deployment strategy is "Recreate".
func (r *StarDeploymentReconciler) scaleUpNewReplicaSetForRecreate(ctx context.Context, newRS *sr.StarReplicaSet, deployment *sr.StarDeployment) (bool, error) {
	scaled, _, err := r.scaleReplicaSetAndRecordEvent(ctx, newRS, *(deployment.Spec.Replicas), deployment)
	return scaled, err
}
