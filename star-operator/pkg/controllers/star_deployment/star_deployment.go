/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package star_deployment

import (
	"context"
	"fmt"
	sr "github.com/shileifu/startrek/star-operator/pkg/apis/v1alpha1"
	deploymentutil "github.com/shileifu/startrek/star-operator/pkg/controllers/star_deployment/util/deployment_util"
	labelsutil "github.com/shileifu/startrek/star-operator/pkg/controllers/star_deployment/util/labels"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sort"
	"strconv"
)

var deploymentControllerKind = sr.GroupVersion.WithKind("StarDeployment")

// StarDeploymentReconciler reconciles a StarDeployment object
type StarDeploymentReconciler struct {
	client.Client
	Recorder record.EventRecorder
	Scheme   *runtime.Scheme
}

//+kubebuilder:rbac:groups=starrocks.com,resources=stardeployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=starrocks.com,resources=stardeployments/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=starrocks.com,resources=stardeployments/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the StarDeployment object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.12.1/pkg/reconcile
func (r *StarDeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	klog.FromContext(ctx)
	klog.V(4).Info("StarRocksClusterReconciler reconcile the update crd name ", req.Name, " namespace ", req.Namespace)
	var deployment sr.StarDeployment
	err := r.Client.Get(ctx, req.NamespacedName, &deployment)
	if errors.IsNotFound(err) {
		klog.V(2).Infof("StarDeployment %v have been deleted", req.NamespacedName)
		return ctrl.Result{}, nil
	}

	d := deployment.DeepCopy()
	// List ReplicaSets owned by this Deployment, while reconciling ControllerRef
	// through adoption/orphaning.
	rsListItems, err := r.getReplicaSetsForDeployment(ctx, d)
	if err != nil {
		return ctrl.Result{}, err
	}
	// List all Pods owned by this Deployment, grouped by their ReplicaSet.
	// Current uses of the podMap are:
	//
	// * check if a Pod is labeled correctly with the pod-template-hash label.
	// * check that no old Pods are running in the middle of Recreate Deployments.
	podMap, err := r.getPodMapForDeployment(ctx, d, rsListItems)
	if err != nil {
		return ctrl.Result{}, err
	}

	if d.DeletionTimestamp != nil {
		//TODO:增加删除处理
	}

	rsList := make([]*sr.StarReplicaSet, len(rsListItems))
	for i, _ := range rsListItems {
		rsList[i] = &rsListItems[i]
	}

	//TODO:判断是否扩缩容操作
	scalingEvent, err := r.isScalingEvent(ctx, d, rsList)
	//如果是扩容操作，只更新replicas
	if scalingEvent {
		return r.sync(ctx, d, rsList)
	}

	//TODO:更新操作：根据pod和relicaset，创建replicaset
	switch d.Spec.Strategy.Type {
	case sr.RecreateDeploymentStrategyType:
		return ctrl.Result{}, r.rolloutRecreate(ctx, d, rsList, podMap)
	case sr.RollingUpdateDeploymentStrategyType:
		return r.rolloutRolling(ctx, d, rsList)
	}
	return ctrl.Result{}, nil
}

// getPodMapForDeployment returns the Pods managed by a Deployment.
//
// It returns a map from ReplicaSet UID to a list of Pods controlled by that RS,
// according to the Pod's ControllerRef.
// NOTE: The pod pointers returned by this method point the pod objects in the cache and thus
// shouldn't be modified in any way.
func (r *StarDeploymentReconciler) getPodMapForDeployment(ctx context.Context, d *sr.StarDeployment, rsList []sr.StarReplicaSet) (map[types.UID][]*v1.Pod, error) {
	// Get all Pods that potentially belong to this Deployment.
	var podList v1.PodList
	var listOptions client.ListOptions
	listOptions.Namespace = d.Namespace
	client.MatchingLabels(d.Spec.Selector.MatchLabels).ApplyToList(&listOptions)
	err := r.Client.List(ctx, &podList, &listOptions)
	if err != nil {
		return nil, err
	}

	podMap := make(map[types.UID][]*v1.Pod, len(rsList))
	for _, rs := range rsList {
		podMap[rs.UID] = []*v1.Pod{}
	}

	for _, pod := range podList.Items {
		controllerRef := metav1.GetControllerOf(&pod)
		if controllerRef == nil {
			continue
		}

		// Only append if we care about this UID.
		if _, ok := podMap[controllerRef.UID]; ok {
			podMap[controllerRef.UID] = append(podMap[controllerRef.UID], &pod)
		}
	}

	return podMap, nil
}

func (r *StarDeploymentReconciler) getReplicaSetsForDeployment(ctx context.Context, d *sr.StarDeployment) ([]sr.StarReplicaSet, error) {
	// List all ReplicaSets to find those we own but that no longer match our
	// selector. They will be orphaned by ClaimReplicaSets().
	var rsList sr.StarReplicaSetList
	var listOptions client.ListOptions
	client.MatchingLabels(d.Spec.Selector.MatchLabels).ApplyToList(&listOptions)

	err := r.Client.List(ctx, &rsList, &listOptions)
	if err != nil {
		return nil, err
	}

	return rsList.Items, nil
}

func (r *StarDeploymentReconciler) isScalingEvent(ctx context.Context, d *sr.StarDeployment, rsList []*sr.StarReplicaSet) (bool, error) {
	//获取新旧的rs
	newRS, oldRSs, err := r.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, false)
	if err != nil {
		return false, err
	}
	allRSs := append(oldRSs, newRS)
	for _, rs := range deploymentutil.FilterActiveReplicaSets(allRSs) {
		desired, ok := deploymentutil.GetDesiredReplicasAnnotation(rs)
		if !ok {
			continue
		}
		if desired != *(d.Spec.Replicas) {
			return true, nil
		}
	}
	return false, nil
}

// cleanupDeployment is responsible for cleaning up a deployment ie. retains all but the latest N old replica sets
// where N=d.Spec.RevisionHistoryLimit. Old replica sets are older versions of the podtemplate of a deployment kept
// around by default 1) for historical reasons and 2) for the ability to rollback a deployment.
func (r *StarDeploymentReconciler) cleanupDeployment(ctx context.Context, oldRSs []*sr.StarReplicaSet, deployment *sr.StarDeployment) error {

	// Avoid deleting replica set with deletion timestamp set
	aliveFilter := func(rs *sr.StarReplicaSet) bool {
		return rs != nil && rs.ObjectMeta.DeletionTimestamp == nil
	}
	cleanableRSes := deploymentutil.FilterReplicaSets(oldRSs, aliveFilter)

	sort.Sort(deploymentutil.ReplicaSetsByCreationTimestamp(cleanableRSes))
	klog.V(4).Infof("Looking to cleanup old replica sets for deployment %q", deployment.Name)

	for i := 0; i < len(oldRSs); i++ {
		rs := cleanableRSes[i]
		// Avoid delete replica set with non-zero replica counts
		if rs.Status.Replicas != 0 || *(rs.Spec.Replicas) != 0 || rs.Generation > rs.Status.ObservedGeneration || rs.DeletionTimestamp != nil {
			continue
		}
		klog.V(4).Infof("Trying to cleanup replica set %q for deployment %q", rs.Name, deployment.Name)
		retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			return r.Client.Delete(ctx, rs)
		})
	}

	return nil
}

// getAllReplicaSetsAndSyncRevision returns all the replica sets for the provided deployment (new and all old), with new RS's and deployment's revision updated.
//
// rsList should come from getReplicaSetsForDeployment(d).
//
// 1. Get all old RSes this deployment targets, and calculate the max revision number among them (maxOldV).
// 2. Get new RS this deployment targets (whose pod template matches deployment's), and update new RS's revision number to (maxOldV + 1),
//    only if its revision number is smaller than (maxOldV + 1). If this step failed, we'll update it in the next deployment sync loop.
// 3. Copy new RS's revision number to deployment (update deployment's revision). If this step failed, we'll update it in the next deployment sync loop.
//
// Note that currently the deployment controller is using caches to avoid querying the server for reads.
// This may lead to stale reads of replica sets, thus incorrect deployment status.
func (r *StarDeploymentReconciler) getAllReplicaSetsAndSyncRevision(ctx context.Context, d *sr.StarDeployment, rsList []*sr.StarReplicaSet, createIfNotExisted bool) (*sr.StarReplicaSet, []*sr.StarReplicaSet, error) {
	_, allOldRs := deploymentutil.FindOldReplicaSets(d, rsList)

	//Get new replica set the updated revision number.
	newRS, err := r.getNewReplicaSet(ctx, d, rsList, allOldRs, createIfNotExisted)
	if err != nil {
		return nil, nil, err
	}
	return newRS, allOldRs, nil
}

// sync is responsible for reconciling deployments on scaling events or when they
// are paused.
func (r *StarDeploymentReconciler) sync(ctx context.Context, d *sr.StarDeployment, rsList []*sr.StarReplicaSet) (ctrl.Result, error) {
	newRS, oldRSs, err := r.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, false)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.scale(ctx, d, newRS, oldRSs); err != nil {
		// If we get an error while trying to scale, the deployment will be requeued
		// so we can abort this resync
		return ctrl.Result{}, err
	}

	allRSs := append(oldRSs, newRS)
	return ctrl.Result{}, r.syncDeploymentStatus(ctx, allRSs, newRS, d)
}

// syncDeploymentStatus checks if the status is up-to-date and sync it if necessary
func (r *StarDeploymentReconciler) syncDeploymentStatus(ctx context.Context, allRSs []*sr.StarReplicaSet, newRS *sr.StarReplicaSet, d *sr.StarDeployment) error {
	newStatus := calculateStatus(allRSs, newRS, d)

	if reflect.DeepEqual(d.Status, newStatus) {
		return nil
	}

	newDeployment := d
	newDeployment.Status = newStatus
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.Client.Status().Update(ctx, newDeployment)
	})
	return err
}

// calculateStatus calculates the latest status for the provided deployment by looking into the provided replica sets.
func calculateStatus(allRSs []*sr.StarReplicaSet, newRS *sr.StarReplicaSet, deployment *sr.StarDeployment) sr.StarDeploymentStatus {
	availableReplicas := deploymentutil.GetAvailableReplicaCountForReplicaSets(allRSs)
	totalReplicas := deploymentutil.GetReplicaCountForReplicaSets(allRSs)
	unavailableReplicas := totalReplicas - availableReplicas
	// If unavailableReplicas is negative, then that means the Deployment has more available replicas running than
	// desired, e.g. whenever it scales down. In such a case we should simply default unavailableReplicas to zero.
	if unavailableReplicas < 0 {
		unavailableReplicas = 0
	}

	status := sr.StarDeploymentStatus{
		// TODO: Ensure that if we start retrying status updates, we won't pick up a new Generation value.
		ObservedGeneration:  deployment.Generation,
		Replicas:            deploymentutil.GetActualReplicaCountForReplicaSets(allRSs),
		UpdatedReplicas:     deploymentutil.GetActualReplicaCountForReplicaSets([]*sr.StarReplicaSet{newRS}),
		ReadyReplicas:       deploymentutil.GetReadyReplicaCountForReplicaSets(allRSs),
		AvailableReplicas:   availableReplicas,
		UnavailableReplicas: unavailableReplicas,
		CollisionCount:      deployment.Status.CollisionCount,
	}

	/*// Copy conditions one by one so we won't mutate the original object.
	conditions := deployment.Status.Conditions
	for i := range conditions {
		status.Conditions = append(status.Conditions, conditions[i])
	}

	if availableReplicas >= *(deployment.Spec.Replicas)-deploymentutil.MaxUnavailable(*deployment) {
		minAvailability := deploymentutil.NewDeploymentCondition(sr.DeploymentAvailable, v1.ConditionTrue, deploymentutil.MinimumReplicasAvailable, "Deployment has minimum availability.")
		deploymentutil.SetDeploymentCondition(&status, *minAvailability)
	} else {
		noMinAvailability := deploymentutil.NewDeploymentCondition(sr.DeploymentAvailable, v1.ConditionFalse, deploymentutil.MinimumReplicasUnavailable, "Deployment does not have minimum availability.")
		deploymentutil.SetDeploymentCondition(&status, *noMinAvailability)
	}*/

	return status
}

// scale scales proportionally in order to mitigate risk. Otherwise, scaling up can increase the size
// of the new replica set and scaling down can decrease the sizes of the old ones, both of which would
// have the effect of hastening the rollout progress, which could produce a higher proportion of unavailable
// replicas in the event of a problem with the rolled out template. Should run only on scaling events or
// when a deployment is paused and not during the normal rollout process.
func (r *StarDeploymentReconciler) scale(ctx context.Context, deployment *sr.StarDeployment, newRS *sr.StarReplicaSet, oldRSs []*sr.StarReplicaSet) error {
	// If there is only one active replica set then we should scale that up to the full count of the
	// deployment. If there is no active replica set, then we should scale up the newest replica set.
	if activeOrLatest := deploymentutil.FindActiveOrLatest(newRS, oldRSs); activeOrLatest != nil {
		if *(activeOrLatest.Spec.Replicas) == (*deployment.Spec.Replicas) {
			return nil
		}

		_, _, err := r.scaleReplicaSetAndRecordEvent(ctx, activeOrLatest, *(deployment.Spec.Replicas), deployment)
		return err
	}

	// If the new replica set is saturated, old replica sets should be fully scaled down.
	// This case handles replica set adoption during a saturated new replica set.
	if deploymentutil.IsSaturated(deployment, newRS) {
		for _, old := range deploymentutil.FilterActiveReplicaSets(oldRSs) {
			if _, _, err := r.scaleReplicaSetAndRecordEvent(ctx, old, 0, deployment); err != nil {
				return err
			}
		}
		return nil
	}

	// There are old replica sets with pods and the new replica set is not saturated.
	// We need to proportionally scale all replica sets (new and old) in case of a
	// rolling deployment.
	if deploymentutil.IsRollingUpdate(deployment) {
		allRSs := deploymentutil.FilterActiveReplicaSets(append(oldRSs, newRS))
		allRSsReplicas := deploymentutil.GetReplicaCountForReplicaSets(allRSs)

		allowedSize := int32(0)
		if *(deployment.Spec.Replicas) > 0 {
			allowedSize = *(deployment.Spec.Replicas) + deploymentutil.MaxSurge(*deployment)
		}

		// Number of additional replicas that can be either added or removed from the total
		// replicas count. These replicas should be distributed proportionally to the active
		// replica sets.
		deploymentReplicasToAdd := allowedSize - allRSsReplicas

		// The additional replicas should be distributed proportionally amongst the active
		// replica sets from the larger to the smaller in size replica set. Scaling direction
		// drives what happens in case we are trying to scale replica sets of the same size.
		// In such a case when scaling up, we should scale up newer replica sets first, and
		// when scaling down, we should scale down older replica sets first.
		var scalingOperation string
		switch {
		case deploymentReplicasToAdd > 0:
			sort.Sort(deploymentutil.ReplicaSetsBySizeNewer(allRSs))
			scalingOperation = "up"

		case deploymentReplicasToAdd < 0:
			sort.Sort(deploymentutil.ReplicaSetsBySizeOlder(allRSs))
			scalingOperation = "down"
		}

		// Iterate over all active replica sets and estimate proportions for each of them.
		// The absolute value of deploymentReplicasAdded should never exceed the absolute
		// value of deploymentReplicasToAdd.
		deploymentReplicasAdded := int32(0)
		nameToSize := make(map[string]int32)
		for i := range allRSs {
			rs := allRSs[i]

			// Estimate proportions if we have replicas to add, otherwise simply populate
			// nameToSize with the current sizes for each replica set.
			if deploymentReplicasToAdd != 0 {
				proportion := deploymentutil.GetProportion(rs, *deployment, deploymentReplicasToAdd, deploymentReplicasAdded)

				nameToSize[rs.Name] = *(rs.Spec.Replicas) + proportion
				deploymentReplicasAdded += proportion
			} else {
				nameToSize[rs.Name] = *(rs.Spec.Replicas)
			}
		}

		// Update all replica sets
		for i := range allRSs {
			rs := allRSs[i]

			// Add/remove any leftovers to the largest replica set.
			if i == 0 && deploymentReplicasToAdd != 0 {
				leftover := deploymentReplicasToAdd - deploymentReplicasAdded
				nameToSize[rs.Name] = nameToSize[rs.Name] + leftover
				if nameToSize[rs.Name] < 0 {
					nameToSize[rs.Name] = 0
				}
			}

			// TODO: Use transactions when we have them.
			if _, _, err := r.scaleReplicaSet(ctx, rs, nameToSize[rs.Name], deployment, scalingOperation); err != nil {
				// Return as soon as we fail, the deployment is requeued
				return err
			}
		}
	}

	return nil
}

func (r *StarDeploymentReconciler) scaleReplicaSetAndRecordEvent(ctx context.Context, rs *sr.StarReplicaSet, newScale int32, deployment *sr.StarDeployment) (bool, *sr.StarReplicaSet, error) {
	// No need to scale
	if *(rs.Spec.Replicas) == newScale {
		return false, rs, nil
	}
	var scalingOperation string
	if *(rs.Spec.Replicas) < newScale {
		scalingOperation = "up"
	} else {
		scalingOperation = "down"
	}
	scaled, newRS, err := r.scaleReplicaSet(ctx, rs, newScale, deployment, scalingOperation)
	return scaled, newRS, err
}

func (r *StarDeploymentReconciler) scaleReplicaSet(ctx context.Context, rs *sr.StarReplicaSet, newScale int32, deployment *sr.StarDeployment, scalingOperation string) (bool, *sr.StarReplicaSet, error) {
	sizeNeedsUpdate := *(rs.Spec.Replicas) != newScale
	annotationsNeedUpdate := deploymentutil.ReplicasAnnotationsNeedUpdate(rs, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+deploymentutil.MaxSurge(*deployment))

	scaled := false
	var err error
	if sizeNeedsUpdate || annotationsNeedUpdate {
		rsCopy := rs.DeepCopy()
		*(rsCopy.Spec.Replicas) = newScale
		deploymentutil.SetReplicasAnnotations(rsCopy, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+deploymentutil.MaxSurge(*deployment))
		if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			return r.Client.Update(ctx, rsCopy)
		}); err == nil && sizeNeedsUpdate {
			scaled = true
			r.Recorder.Eventf(deployment, v1.EventTypeNormal, "ScalingReplicaSet", "Scaled %s replica set %s to %d", scalingOperation, rs.Name, newScale)
		}
	}

	return scaled, rs, err
}

// Returns a replica set that matches the intent of the given deployment. Returns nil if the new replica set doesn't exist yet.
// 1. Get existing new RS (the RS that the given deployment targets, whose pod template is the same as deployment's).
// 2. If there's existing new RS, update its revision number if it's smaller than (maxOldRevision + 1), where maxOldRevision is the max revision number among all old RSes.
// 3. If there's no existing new RS and createIfNotExisted is true, create one with appropriate revision number (maxOldRevision + 1) and replicas.
// Note that the pod-template-hash will be added to adopted RSes and pods.
func (r *StarDeploymentReconciler) getNewReplicaSet(ctx context.Context, d *sr.StarDeployment, rsList, oldRSs []*sr.StarReplicaSet, createIfNotExisted bool) (*sr.StarReplicaSet, error) {
	existingNewRS := deploymentutil.FindNewReplicaSet(d, rsList)
	// Calculate the max revision number among all old RSes

	// Calculate the max revision number among all old RSes
	maxOldRevision := deploymentutil.MaxRevision(oldRSs)
	// Calculate revision number for this new replica set
	newRevision := strconv.FormatInt(maxOldRevision+1, 10)

	if existingNewRS != nil {
		rsCopy := existingNewRS.DeepCopy()
		return rsCopy, nil
	}

	if !createIfNotExisted {
		return nil, nil
	}

	// new ReplicaSet does not exist, create one.
	newRSTemplate := *d.Spec.Template.DeepCopy()

	podTemplateSpecHash := deploymentutil.ComputeHash(&newRSTemplate, d.Status.CollisionCount)
	newRSTemplate.Labels = labelsutil.CloneAndAddLabel(d.Spec.Template.Labels, apps.DefaultDeploymentUniqueLabelKey, podTemplateSpecHash)
	// Add podTemplateHash label to selector.
	newRSSelector := labelsutil.CloneSelectorAndAddLabel(d.Spec.Selector, apps.DefaultDeploymentUniqueLabelKey, podTemplateSpecHash)

	// Create new ReplicaSet
	newRS := sr.StarReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			// Make the name deterministic, to ensure idempotence
			Name:            d.Name + "-" + podTemplateSpecHash,
			Namespace:       d.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(d, deploymentControllerKind)},
			Labels:          newRSTemplate.Labels,
		},
		Spec: sr.ReplicaSetSpec{
			Replicas: new(int32),
			Selector: newRSSelector,
			Template: newRSTemplate,
		},
	}
	allRSs := append(oldRSs, &newRS)
	newReplicasCount, err := deploymentutil.NewRSNewReplicas(d, allRSs, &newRS)
	if err != nil {
		return nil, err
	}

	*(newRS.Spec.Replicas) = newReplicasCount
	// Set new replica set's annotation
	deploymentutil.SetNewReplicaSetAnnotations(d, &newRS, newRevision, false)
	// Create the new ReplicaSet. If it already exists, then we need to check for possible
	// hash collisions. If there is any other error, we need to report it in the status of
	// the Deployment.

	err = r.Client.Create(ctx, &newRS)
	switch {
	case errors.IsAlreadyExists(err):
		return &newRS, nil
	case errors.HasStatusCause(err, v1.NamespaceTerminatingCause):
		// if the namespace is terminating, all subsequent creates will fail and we can safely do nothing
		return nil, err
	case err != nil:
		msg := fmt.Sprintf("Failed to create new replica set %q: %v", newRS.Name, err)
		r.Recorder.Eventf(d, v1.EventTypeWarning, deploymentutil.FailedRSCreateReason, msg)
		return nil, err
	}

	return &newRS, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StarDeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sr.StarDeployment{}).
		Complete(r)
}
