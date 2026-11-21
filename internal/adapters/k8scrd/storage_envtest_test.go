// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package k8scrd_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/go-tram/tram/internal/adapters/k8scrd"
	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
)

const storageEngine = "edge"

func storageSpec() *tramv1alpha1.StorageSpec {
	return &tramv1alpha1.StorageSpec{Size: resource.MustParse("1Gi")}
}

// startStorageCluster runs the manager on a cluster with a namespace.
// envtest runs no controllers besides the manager: no StatefulSet, Job or
// garbage collector, and no protection controller, so deleted claims keep
// their finalizer. The tests play these parts.
func startStorageCluster(t *testing.T) (*cluster, string) {
	t.Helper()
	c := startEngineCluster(t)
	ns := c.newNamespace(t)
	runManager(t, k8scrd.ManagerOptions{
		Kubeconfig: c.adminKubeconfig(t), Namespaces: []string{ns}, EngineImage: testImage,
	})
	return c, ns
}

// makeReady reports the workload of the Engine ready, as its controller
// would.
func (c *cluster) makeReady(t *testing.T, ns string, obj client.Object) {
	t.Helper()
	c.requireOwned(t, ns, storageEngine, obj)
	base := obj.DeepCopyObject().(client.Object)
	switch w := obj.(type) {
	case *appsv1.StatefulSet:
		w.Status = appsv1.StatefulSetStatus{
			ObservedGeneration: w.Generation, Replicas: *w.Spec.Replicas, ReadyReplicas: *w.Spec.Replicas,
			AvailableReplicas: *w.Spec.Replicas, CurrentReplicas: *w.Spec.Replicas, UpdatedReplicas: *w.Spec.Replicas,
		}
	case *appsv1.Deployment:
		w.Status = appsv1.DeploymentStatus{
			ObservedGeneration: w.Generation, Replicas: *w.Spec.Replicas, ReadyReplicas: *w.Spec.Replicas,
			Conditions: []appsv1.DeploymentCondition{{
				Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue, Reason: "MinimumReplicasAvailable",
			}},
		}
	}
	require.NoError(t, c.admin.Status().Patch(t.Context(), obj, client.MergeFrom(base)))
}

// createClaim creates the volume claim of an instance, as the StatefulSet
// controller would.
func (c *cluster) createClaim(t *testing.T, ns string, ordinal int) {
	t.Helper()
	requests := make(corev1.ResourceList)
	requests[corev1.ResourceStorage] = resource.MustParse("1Gi")
	claim := &corev1.PersistentVolumeClaim{
		Namespace: ns, Name: "data-tram-engine-" + storageEngine + "-" + string(rune('0'+ordinal)),
		Labels: map[string]string{
			"tram.io/engine": storageEngine, "tram.io/volume": "data",
			"app.kubernetes.io/managed-by": managerName,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: requests},
		},
	}
	require.NoError(t, c.admin.Create(t.Context(), claim))
}

// claimDeleted reports whether the claim of ordinal is deleted or marked for
// deletion; an error to get it counts as not deleted.
func (c *cluster) claimDeleted(t *testing.T, ns string, ordinal int) bool {
	var claim corev1.PersistentVolumeClaim
	key := types.NamespacedName{
		Namespace: ns,
		Name:      "data-tram-engine-" + storageEngine + "-" + string(rune('0'+ordinal)),
	}
	err := c.admin.Get(t.Context(), key, &claim)
	if apierrors.IsNotFound(err) {
		return true
	}
	return err == nil && !claim.DeletionTimestamp.IsZero()
}

// drainJobs returns the drain Jobs of the Engine by the ordinal of their
// volume.
func (c *cluster) drainJobs(t *testing.T, collect *assert.CollectT, ns string) map[string]batchv1.Job {
	var jobs batchv1.JobList
	assert.NoError(collect, c.admin.List(t.Context(), &jobs, client.InNamespace(ns),
		client.MatchingLabels{"tram.io/drain": storageEngine}))
	out := map[string]batchv1.Job{}
	for _, job := range jobs.Items {
		if job.DeletionTimestamp.IsZero() {
			out[job.Labels["tram.io/ordinal"]] = job
		}
	}
	return out
}

// completeJob reports a drain Job done, as the Job controller would.
func (c *cluster) completeJob(t *testing.T, job batchv1.Job) {
	t.Helper()
	base := job.DeepCopy()
	now := metav1.Now()
	job.Status = batchv1.JobStatus{
		StartTime: &now, CompletionTime: &now, Succeeded: 1,
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
		},
	}
	require.NoError(t, c.admin.Status().Patch(t.Context(), &job, client.MergeFrom(base)))
}

func TestEngineWithStorageRunsAsStatefulSet(t *testing.T) {
	t.Parallel()
	c, ns := startStorageCluster(t)
	c.createEngine(t, ns, storageEngine, tramv1alpha1.EngineSpec{Replicas: new(int32(2)), Storage: storageSpec()})

	var set appsv1.StatefulSet
	c.requireOwned(t, ns, storageEngine, &set)
	assert.Equal(t, appsv1.ParallelPodManagement, set.Spec.PodManagementPolicy)
	require.Len(t, set.Spec.VolumeClaimTemplates, 1)
	assert.Equal(t, "data", set.Spec.VolumeClaimTemplates[0].Name)
	assert.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		set.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
	pod := set.Spec.Template.Spec
	assert.Contains(t, pod.Containers[0].Args, "--data-dir=/var/lib/tram")
	assert.Equal(t, int64(65532), *pod.SecurityContext.FSGroup)
	assert.Equal(t, "/var/lib/tram", pod.Containers[0].VolumeMounts[0].MountPath)

	c.requireGone(t, ns, storageEngine, &appsv1.Deployment{})
	c.requireEngine(t, ns, storageEngine, func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		assert.Equal(collect, tramv1alpha1.WorkloadStatefulSet, e.Status.Workload)
		assert.Contains(collect, e.Finalizers, "tram.io/drain-buffers")
	})
}

func TestEngineStorageSizeCannotChange(t *testing.T) {
	t.Parallel()
	c, ns := startStorageCluster(t)
	c.createEngine(t, ns, storageEngine, tramv1alpha1.EngineSpec{Storage: storageSpec()})

	var engine tramv1alpha1.Engine
	require.NoError(t, c.admin.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: storageEngine}, &engine))
	base := engine.DeepCopy()
	engine.Spec.Storage.Size = resource.MustParse("2Gi")
	err := c.admin.Patch(t.Context(), &engine, client.MergeFrom(base))
	require.ErrorContains(t, err, "size cannot be changed")
}

// Adding storage starts a StatefulSet next to the Deployment, which keeps
// serving until the StatefulSet is ready.
func TestEngineMigratesToStatefulSet(t *testing.T) {
	t.Parallel()
	c, ns := startStorageCluster(t)
	c.createEngine(t, ns, storageEngine, tramv1alpha1.EngineSpec{Replicas: new(int32(2))})
	c.requireOwned(t, ns, storageEngine, &appsv1.Deployment{})

	c.updateEngine(t, ns, storageEngine, func(spec *tramv1alpha1.EngineSpec) { spec.Storage = storageSpec() })
	var set appsv1.StatefulSet
	c.requireOwned(t, ns, storageEngine, &set)
	c.requireEngine(t, ns, storageEngine, func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		assert.Equal(collect, metav1.ConditionTrue, condition(e, tramv1alpha1.ConditionMigrating).Status)
	})
	c.requireOwned(t, ns, storageEngine, &appsv1.Deployment{})

	c.makeReady(t, ns, &set)
	c.requireGone(t, ns, storageEngine, &appsv1.Deployment{})
	c.requireEngine(t, ns, storageEngine, func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		assert.Equal(collect, metav1.ConditionFalse, condition(e, tramv1alpha1.ConditionMigrating).Status)
	})
}

// Removing storage starts a Deployment; once it is ready, the StatefulSet
// goes, and its volumes are drained before they are deleted.
func TestEngineMigratesBackAndDrainsVolumes(t *testing.T) {
	t.Parallel()
	c, ns := startStorageCluster(t)
	c.createEngine(t, ns, storageEngine, tramv1alpha1.EngineSpec{Replicas: new(int32(2)), Storage: storageSpec()})
	c.requireOwned(t, ns, storageEngine, &appsv1.StatefulSet{})
	c.createClaim(t, ns, 0)
	c.createClaim(t, ns, 1)

	c.updateEngine(t, ns, storageEngine, func(spec *tramv1alpha1.EngineSpec) { spec.Storage = nil })
	var deployment appsv1.Deployment
	c.makeReady(t, ns, &deployment)
	c.requireGone(t, ns, storageEngine, &appsv1.StatefulSet{})

	var jobs map[string]batchv1.Job
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		jobs = c.drainJobs(t, collect, ns)
		assert.Len(collect, jobs, 2)
	}, eventuallyTimeout, pollInterval)
	job := jobs["0"]
	assert.Equal(t, []string{
		"drain", "--log-level=info", "--engine=" + storageEngine, "--watch-namespace=" + ns, "--data-dir=/data",
	}, job.Spec.Template.Spec.Containers[0].Args)
	assert.Equal(t, "data-tram-engine-edge-0",
		job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName)
	assert.NotContains(t, job.Spec.Template.Labels, "tram.io/engine", "the Service must not select drain pods")

	c.completeJob(t, job)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.True(collect, c.claimDeleted(t, ns, 0), "a drained volume is deleted")
		assert.False(collect, c.claimDeleted(t, ns, 1), "a volume is kept until it is drained")
	}, eventuallyTimeout, pollInterval)
	c.requireEngine(t, ns, storageEngine, func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		assert.Equal(collect, int32(1), e.Status.DrainingVolumes)
		assert.Equal(collect, metav1.ConditionTrue, condition(e, tramv1alpha1.ConditionDraining).Status)
	})

	// The finished Job stays for its logs, and the Engine tells what happened.
	var kept batchv1.Job
	require.NoError(t, c.admin.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: job.Name}, &kept))
	assert.True(t, kept.DeletionTimestamp.IsZero(), "the drain Job is kept for its logs")
	require.NotNil(t, kept.Spec.TTLSecondsAfterFinished)
	assert.Equal(t, int32(3600), *kept.Spec.TTLSecondsAfterFinished, "Kubernetes removes it after an hour")
	c.requireEvent(t, ns, "VolumeDraining")
	c.requireEvent(t, ns, "VolumeDrained")

	// A new volume of the same instance gets a Job of its own: the kept Job
	// belongs to the volume it drained.
	c.forgetClaim(t, ns, 0)
	c.createClaim(t, ns, 0)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var all batchv1.JobList
		assert.NoError(collect, c.admin.List(t.Context(), &all, client.InNamespace(ns),
			client.MatchingLabels{"tram.io/drain": storageEngine, "tram.io/ordinal": "0"}))
		assert.Len(collect, all.Items, 2, "the kept Job and one for the new volume")
	}, eventuallyTimeout, pollInterval)
	assert.False(t, c.claimDeleted(t, ns, 0), "the new volume is not mistaken for drained")
}

// Adding storage back within the time to live of a kept drain Job deletes
// the Job: its finished pod holds the deleted volume, whose name the new
// instance needs.
func TestStorageAddedBackReleasesKeptDrainJob(t *testing.T) {
	t.Parallel()
	c, ns := startStorageCluster(t)
	c.createEngine(t, ns, storageEngine, tramv1alpha1.EngineSpec{Storage: storageSpec()})
	c.requireOwned(t, ns, storageEngine, &appsv1.StatefulSet{})
	c.createClaim(t, ns, 0)

	c.updateEngine(t, ns, storageEngine, func(spec *tramv1alpha1.EngineSpec) { spec.Storage = nil })
	var deployment appsv1.Deployment
	c.makeReady(t, ns, &deployment)
	c.requireGone(t, ns, storageEngine, &appsv1.StatefulSet{})
	var job batchv1.Job
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		jobs := c.drainJobs(t, collect, ns)
		if assert.Contains(collect, jobs, "0") {
			job = jobs["0"]
		}
	}, eventuallyTimeout, pollInterval)
	c.completeJob(t, job)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.True(collect, c.claimDeleted(t, ns, 0), "a drained volume is deleted")
	}, eventuallyTimeout, pollInterval)
	key := types.NamespacedName{Namespace: ns, Name: job.Name}
	require.NoError(t, c.admin.Get(t.Context(), key, &job), "the drain Job is kept while no instance needs the volume")

	c.updateEngine(t, ns, storageEngine, func(spec *tramv1alpha1.EngineSpec) { spec.Storage = storageSpec() })
	c.requireOwned(t, ns, storageEngine, &appsv1.StatefulSet{})
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var kept batchv1.Job
		err := c.admin.Get(t.Context(), key, &kept)
		assert.True(collect, apierrors.IsNotFound(err) || !kept.DeletionTimestamp.IsZero(),
			"the kept Job goes, so the volume can")
	}, eventuallyTimeout, pollInterval)
	c.requireEvent(t, ns, "VolumeReleased")
}

// requireEvent waits for an event with reason on the Engine.
func (c *cluster) requireEvent(t *testing.T, ns, reason string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var list eventsv1.EventList
		if !assert.NoError(collect, c.admin.List(t.Context(), &list, client.InNamespace(ns))) {
			return
		}
		for _, e := range list.Items {
			if e.Reason == reason && e.Regarding.Kind == "Engine" && e.Regarding.Name == storageEngine {
				return
			}
		}
		collect.Errorf("no event %s on the Engine", reason)
	}, eventuallyTimeout, pollInterval)
}

// forgetClaim removes a deleted claim for good, as the protection controller
// envtest lacks would once no pod uses it.
func (c *cluster) forgetClaim(t *testing.T, ns string, ordinal int) {
	t.Helper()
	var claim corev1.PersistentVolumeClaim
	key := types.NamespacedName{
		Namespace: ns,
		Name:      "data-tram-engine-" + storageEngine + "-" + string(rune('0'+ordinal)),
	}
	require.NoError(t, c.admin.Get(t.Context(), key, &claim))
	base := claim.DeepCopy()
	claim.Finalizers = nil
	require.NoError(t, c.admin.Patch(t.Context(), &claim, client.MergeFrom(base)))
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.True(collect, apierrors.IsNotFound(c.admin.Get(t.Context(), key, &claim)))
	}, eventuallyTimeout, pollInterval)
}

// A volume left by scaling down is drained; scaling up again hands it back
// to its instance.
func TestScaleDownDrainsAndScaleUpReturnsVolume(t *testing.T) {
	t.Parallel()
	c, ns := startStorageCluster(t)
	c.createEngine(t, ns, storageEngine, tramv1alpha1.EngineSpec{Replicas: new(int32(2)), Storage: storageSpec()})
	c.requireOwned(t, ns, storageEngine, &appsv1.StatefulSet{})
	c.createClaim(t, ns, 0)
	c.createClaim(t, ns, 1)
	c.createClaim(t, ns, 2)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		jobs := c.drainJobs(t, collect, ns)
		assert.Len(collect, jobs, 1)
		assert.Contains(collect, jobs, "2", "only the volume without an instance is drained")
	}, eventuallyTimeout, pollInterval)

	c.updateEngine(t, ns, storageEngine, func(spec *tramv1alpha1.EngineSpec) { spec.Replicas = new(int32(3)) })
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Empty(collect, c.drainJobs(t, collect, ns), "the instance continues with its volume")
		assert.False(collect, c.claimDeleted(t, ns, 2))
	}, eventuallyTimeout, pollInterval)
}

// Deleting an Engine waits until its volumes are drained.
func TestDeletedEngineWaitsForDrains(t *testing.T) {
	t.Parallel()
	c, ns := startStorageCluster(t)
	c.createEngine(t, ns, storageEngine, tramv1alpha1.EngineSpec{Storage: storageSpec()})
	c.requireEngine(t, ns, storageEngine, func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		assert.Contains(collect, e.Finalizers, "tram.io/drain-buffers")
	})
	c.createClaim(t, ns, 0)

	engine := &tramv1alpha1.Engine{}
	engine.Namespace, engine.Name = ns, storageEngine
	require.NoError(t, c.admin.Delete(t.Context(), engine))

	var jobs map[string]batchv1.Job
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		jobs = c.drainJobs(t, collect, ns)
		assert.Contains(collect, jobs, "0")
	}, eventuallyTimeout, pollInterval)
	c.requireEngine(t, ns, storageEngine, func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		assert.False(collect, e.DeletionTimestamp.IsZero(), "the Engine waits for the drain")
	})

	c.completeJob(t, jobs["0"])
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		err := c.admin.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: storageEngine}, engine)
		assert.True(collect, apierrors.IsNotFound(err), "the Engine goes once its volumes are drained")
	}, eventuallyTimeout, pollInterval)
}

func TestDeletedEngineCanDiscardBuffers(t *testing.T) {
	t.Parallel()
	c, ns := startStorageCluster(t)
	c.createEngine(t, ns, storageEngine, tramv1alpha1.EngineSpec{Storage: storageSpec()})
	c.requireEngine(t, ns, storageEngine, func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		assert.Contains(collect, e.Finalizers, "tram.io/drain-buffers")
	})
	c.createClaim(t, ns, 0)

	var engine tramv1alpha1.Engine
	key := types.NamespacedName{Namespace: ns, Name: storageEngine}
	require.NoError(t, c.admin.Get(t.Context(), key, &engine))
	base := engine.DeepCopy()
	engine.Annotations = map[string]string{tramv1alpha1.DiscardBuffersAnnotation: "true"}
	require.NoError(t, c.admin.Patch(t.Context(), &engine, client.MergeFrom(base)))
	require.NoError(t, c.admin.Delete(t.Context(), &engine))

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.True(collect, apierrors.IsNotFound(c.admin.Get(t.Context(), key, &engine)))
		assert.True(collect, c.claimDeleted(t, ns, 0))
	}, eventuallyTimeout, pollInterval)
}

// createBufferStream creates a Stream "name" that writes OTLP data into the
// buffer queue "s".
func (c *cluster) createBufferStream(t *testing.T, ns, name string, engines ...string) {
	t.Helper()
	stream := &tramv1.Stream{
		Namespace: ns, Name: name,
		Spec: tramv1.StreamSpec{
			EngineRefs: engineRefs(engines...),
			Source: tramv1.Spec{
				Name: "in",
				Type: "otlp",
				Spec: &runtime.RawExtension{Raw: []byte(`{"http_port":4318}`)},
			},
			Sinks: []tramv1.ReceiverSpec{{
				Name: "outbox", Type: "buffer", From: "in",
				Spec: &runtime.RawExtension{Raw: []byte(`{"driver":"sqlite","sqlite":{},"queue":"s","max_age":"1h"}`)},
			}},
		},
	}
	require.NoError(t, c.admin.Create(t.Context(), stream))
}

// requireStream waits until the Stream "s" satisfies check.
func (c *cluster) requireStream(t *testing.T, ns string, check func(*assert.CollectT, *tramv1.Stream)) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var stream tramv1.Stream
		if !assert.NoError(
			collect,
			c.admin.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: "s"}, &stream),
		) {
			return
		}
		check(collect, &stream)
	}, eventuallyTimeout, pollInterval)
}

func TestBufferStreamReportsWhetherItsEngineHasStorage(t *testing.T) {
	t.Parallel()
	c, ns := startStorageCluster(t)
	c.createEngine(t, ns, storageEngine, tramv1alpha1.EngineSpec{})
	c.createBufferStream(t, ns, "s", storageEngine)

	available := func(want metav1.ConditionStatus, reason string) {
		c.requireStream(t, ns, func(collect *assert.CollectT, s *tramv1.Stream) {
			if !assert.Len(collect, s.Status.Engines, 1) {
				return
			}
			got := meta.FindStatusCondition(s.Status.Engines[0].Conditions, tramv1.ConditionStorageAvailable)
			if assert.NotNil(collect, got) {
				assert.Equal(collect, want, got.Status)
				assert.Equal(collect, reason, got.Reason)
			}
		})
	}
	available(metav1.ConditionFalse, "EngineHasNoStorage")

	c.updateEngine(t, ns, storageEngine, func(spec *tramv1alpha1.EngineSpec) { spec.Storage = storageSpec() })
	available(metav1.ConditionFalse, "StorageMigrating")

	var set appsv1.StatefulSet
	c.makeReady(t, ns, &set)
	available(metav1.ConditionTrue, "StorageAvailable")
}

// While an Engine migrates to storage, its Service selects the old and the
// new instances; once the StatefulSet is ready, only its instances, before
// the Deployment goes, so no request reaches an instance that cannot run a
// Stream with a buffer.
func TestServiceMovesToInstancesWithStorage(t *testing.T) {
	t.Parallel()
	c, ns := startStorageCluster(t)
	c.createEngine(t, ns, storageEngine, tramv1alpha1.EngineSpec{Replicas: new(int32(2))})
	c.requireOwned(t, ns, storageEngine, &appsv1.Deployment{})

	selector := func(want map[string]string) {
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			var service corev1.Service
			key := types.NamespacedName{Namespace: ns, Name: "tram-engine-" + storageEngine}
			if assert.NoError(collect, c.admin.Get(t.Context(), key, &service)) {
				assert.Equal(collect, want, service.Spec.Selector)
			}
		}, eventuallyTimeout, pollInterval)
	}
	selector(map[string]string{"tram.io/engine": storageEngine})

	c.updateEngine(t, ns, storageEngine, func(spec *tramv1alpha1.EngineSpec) { spec.Storage = storageSpec() })
	var set appsv1.StatefulSet
	c.requireOwned(t, ns, storageEngine, &set)
	assert.Equal(t, "true", set.Spec.Template.Labels["tram.io/storage"])
	selector(map[string]string{"tram.io/engine": storageEngine})

	c.makeReady(t, ns, &set)
	selector(map[string]string{"tram.io/engine": storageEngine, "tram.io/storage": "true"})
	c.requireGone(t, ns, storageEngine, &appsv1.Deployment{})
}

// createReaderStream creates a Stream "reader" that delivers the buffer queue
// "s" to the debug sink.
func (c *cluster) createReaderStream(t *testing.T, ns string, engines ...string) {
	t.Helper()
	stream := &tramv1.Stream{
		Namespace: ns, Name: "reader",
		Spec: tramv1.StreamSpec{
			EngineRefs: engineRefs(engines...),
			Source: tramv1.Spec{
				Name: "outbox", Type: "buffer",
				Spec: &runtime.RawExtension{Raw: []byte(`{"driver":"sqlite","sqlite":{},"queue":"s"}`)},
			},
			Sinks: []tramv1.ReceiverSpec{{Name: "out", Type: "debug", From: "outbox"}},
		},
	}
	require.NoError(t, c.admin.Create(t.Context(), stream))
}

// Removing storage waits while Streams write to buffers: their writes would
// land on volumes that go away. Once the writers are suspended, the Engine
// moves to a Deployment while the reading Stream stays, so the drain Jobs
// deliver the volumes through it.
func TestRemovingStorageWaitsForActiveBufferWriters(t *testing.T) {
	t.Parallel()
	c, ns := startStorageCluster(t)
	c.createEngine(t, ns, storageEngine, tramv1alpha1.EngineSpec{Storage: storageSpec()})
	var set appsv1.StatefulSet
	c.makeReady(t, ns, &set)
	c.createClaim(t, ns, 0)
	c.createBufferStream(t, ns, "s", storageEngine)
	c.createReaderStream(t, ns, storageEngine)
	c.requireEngine(t, ns, storageEngine, func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		assert.Equal(collect, int32(2), e.Status.Streams)
	})

	c.updateEngine(t, ns, storageEngine, func(spec *tramv1alpha1.EngineSpec) { spec.Storage = nil })
	c.requireEngine(t, ns, storageEngine, func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		reconciled := condition(e, tramv1alpha1.ConditionReconciled)
		assert.Equal(collect, metav1.ConditionFalse, reconciled.Status)
		assert.Equal(collect, "StorageInUse", reconciled.Reason)
		assert.Contains(collect, reconciled.Message, "s", "names the writing Stream")
		assert.NotContains(collect, reconciled.Message, "reader", "the reading Stream may stay")
	})
	assert.True(t, apierrors.IsNotFound(c.admin.Get(t.Context(),
		types.NamespacedName{Namespace: ns, Name: "tram-engine-" + storageEngine}, &appsv1.Deployment{})),
		"no Deployment is started while a Stream writes to a buffer")

	// The author stops writing; the reader stays.
	var writer tramv1.Stream
	require.NoError(t, c.admin.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: "s"}, &writer))
	base := writer.DeepCopy()
	writer.Spec.Suspended = new(true)
	require.NoError(t, c.admin.Patch(t.Context(), &writer, client.MergeFrom(base)))

	var deployment appsv1.Deployment
	c.makeReady(t, ns, &deployment)
	c.requireGone(t, ns, storageEngine, &appsv1.StatefulSet{})
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Contains(collect, c.drainJobs(t, collect, ns), "0", "the volume is drained through the reader")
	}, eventuallyTimeout, pollInterval)
}
