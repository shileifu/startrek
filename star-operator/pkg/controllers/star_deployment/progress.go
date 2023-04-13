package star_deployment

import (
	"context"
	sr "github.com/shileifu/startrek/star-operator/pkg/apis/v1alpha1"
	"k8s.io/client-go/util/retry"
)

// syncRolloutStatus updates the status of a deployment during a rollout. There are
// cases this helper will run that cannot be prevented from the scaling detection,
// for example a resync of the deployment after it was scaled up. In those cases,
// we shouldn't try to estimate any progress.
func (r *StarDeploymentReconciler) syncRolloutStatus(ctx context.Context, allRSs []*sr.StarReplicaSet, newRS *sr.StarReplicaSet, d *sr.StarDeployment) error {
	newStatus := calculateStatus(allRSs, newRS, d)
	//TODO: condition temp deleted
	newDeployment := d
	newDeployment.Status = newStatus
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.Client.Status().Update(ctx, newDeployment)
	})
}
