package star_replicaset

import (
	"context"
	sr "github.com/shileifu/startrek/star-operator/pkg/apis/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var replicasetControllerKind = sr.GroupVersion.WithKind("StarReplicaSet")

// StarReplicaSetReconciler reconciles a StarDeployment object
type StarReplicaSetReconciler struct {
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
func (rsc *StarReplicaSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	//TODO: 获取Replicaset
	klog.V(4).Infof("StarReplicaSetReconciler reconcile the update crd name")

	//TODO: 更新，缩减，扩容pod
	//notice
	// 1. the pod name  will change,

	//TODO: 更新Replicaset的状态

}

// SetupWithManager sets up the controller with the Manager.
func (rsc *StarReplicaSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sr.StarDeployment{}).
		Complete(rsc)
}
