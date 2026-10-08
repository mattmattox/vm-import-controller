package migration

import (
	"context"
	"fmt"
	"time"

	corecontrollers "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/harvester/vm-import-controller/pkg/apis/common"
	migration "github.com/harvester/vm-import-controller/pkg/apis/migration.harvesterhci.io/v1beta1"
	migrationController "github.com/harvester/vm-import-controller/pkg/generated/controllers/migration.harvesterhci.io/v1beta1"
	"github.com/harvester/vm-import-controller/pkg/source/vmware"
	"github.com/harvester/vm-import-controller/pkg/util"
)

type vmwareHandler struct {
	ctx    context.Context
	source migrationController.VmwareSourceController
	secret corecontrollers.SecretController
}

func RegisterVmwareController(ctx context.Context, source migrationController.VmwareSourceController, secret corecontrollers.SecretController) {
	vHandler := &vmwareHandler{
		ctx:    ctx,
		source: source,
		secret: secret,
	}
	source.OnChange(ctx, "vmware-source-change", vHandler.OnSourceChange)
}

func (h *vmwareHandler) OnSourceChange(_ string, v *migration.VmwareSource) (*migration.VmwareSource, error) {
	if v == nil || v.DeletionTimestamp != nil {
		return nil, nil
	}

	logrus.WithFields(logrus.Fields{
		"kind":      v.Kind,
		"name":      v.Name,
		"namespace": v.Namespace,
	}).Info("Reconciling source")

	if v.Status.Status != migration.ClusterReady {
		var client *vmware.Client
		var err error

		secretObj, err := h.secret.Get(v.SecretReference().Namespace, v.SecretReference().Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to lookup secret for %s migration %s: %w", v.Kind, v.NamespacedName(), err)
		}

		client, err = vmware.NewClient(h.ctx, v.Spec.EndpointAddress, v.Spec.Datacenter, secretObj)
		if err != nil {
			return nil, fmt.Errorf("failed to generate client for %s migration %s: %w", v.Kind, v.NamespacedName(), err)
		}

		err = client.Verify()
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"apiVersion": v.APIVersion,
				"kind":       v.Kind,
				"name":       v.Name,
				"namespace":  v.Namespace,
				"err":        err,
			}).Error("Failed to verify source for migration")

			// unable to find specific datacenter
			conds := []common.Condition{
				{
					Type:               migration.ClusterErrorCondition,
					Status:             corev1.ConditionTrue,
					LastUpdateTime:     metav1.Now().Format(time.RFC3339),
					LastTransitionTime: metav1.Now().Format(time.RFC3339),
				}, {
					Type:               migration.ClusterReadyCondition,
					Status:             corev1.ConditionFalse,
					LastUpdateTime:     metav1.Now().Format(time.RFC3339),
					LastTransitionTime: metav1.Now().Format(time.RFC3339),
				},
			}

			v.Status.Conditions = util.MergeConditions(v.Status.Conditions, conds)
			v.Status.Status = migration.ClusterNotReady
		} else {
			conds := []common.Condition{
				{
					Type:               migration.ClusterReadyCondition,
					Status:             corev1.ConditionTrue,
					LastUpdateTime:     metav1.Now().Format(time.RFC3339),
					LastTransitionTime: metav1.Now().Format(time.RFC3339),
				}, {
					Type:               migration.ClusterErrorCondition,
					Status:             corev1.ConditionFalse,
					LastUpdateTime:     metav1.Now().Format(time.RFC3339),
					LastTransitionTime: metav1.Now().Format(time.RFC3339),
				},
			}

			v.Status.Conditions = util.MergeConditions(v.Status.Conditions, conds)
			v.Status.Status = migration.ClusterReady
		}

		return h.source.UpdateStatus(v)
	}

	return nil, nil
}
