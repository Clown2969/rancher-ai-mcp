// Package stevetest provides a fake http.RoundTripper that answers Steve-shaped
// requests (as issued by client.Client's SteveGet/SteveList) by delegating to a
// k8s.io/client-go dynamic.Interface — typically the same
// k8s.io/client-go/dynamic/fake client existing tests already build fixtures
// for. This lets a single fixture set exercise both the typed/dynamic client
// path (DynClientCreator) and the Steve-routed path (SteveTransport).
package stevetest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/rancher/rancher-ai-mcp/pkg/converter"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

var (
	steveTypeToGVROnce sync.Once
	steveTypeToGVR     map[string]schema.GroupVersionResource
)

// gvrForSteveType resolves a steve type to a concrete GVR. local (built from
// a test's own customListKinds map, if any) is tried first because it knows
// the actual served version the fake dynamic client was registered with;
// the static production map is a fallback and deliberately leaves Version
// empty for multi-version resources (see converter.K8sKindsToGVRs), which
// the fake dynamic client can't resolve on its own.
func gvrForSteveType(steveType string, local map[string]schema.GroupVersionResource) (schema.GroupVersionResource, bool) {
	if gvr, ok := local[steveType]; ok {
		return gvr, true
	}

	steveTypeToGVROnce.Do(func() {
		steveTypeToGVR = make(map[string]schema.GroupVersionResource, len(converter.K8sKindsToGVRs))
		for _, gvr := range converter.K8sKindsToGVRs {
			steveTypeToGVR[converter.SteveTypeForGVR(gvr)] = gvr
		}
	})
	gvr, ok := steveTypeToGVR[steveType]
	return gvr, ok
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// NewRoundTripper returns an http.RoundTripper suitable for client.Client's
// SteveTransport field. It parses the Steve request path/query the same way
// client.go's steve.go builds it and serves the answer from dyn.
func NewRoundTripper(dyn dynamic.Interface) http.RoundTripper {
	return NewRoundTripperWithListKinds(dyn, nil)
}

// NewRoundTripperWithListKinds is like NewRoundTripper, but resolves steve
// types against listKinds first (typically the same
// map[schema.GroupVersionResource]string passed to
// dynamicfake.NewSimpleDynamicClientWithCustomListKinds to build dyn). Use
// this for resources registered with an explicit API version that the
// static production GVR map leaves blank (e.g. Cluster API types, see
// converter.CAPIClusterResourceKind and friends).
func NewRoundTripperWithListKinds(dyn dynamic.Interface, listKinds map[schema.GroupVersionResource]string) http.RoundTripper {
	local := make(map[string]schema.GroupVersionResource, len(listKinds))
	for gvr := range listKinds {
		local[converter.SteveTypeForGVR(gvr)] = gvr
	}
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return handle(req, dyn, local)
	})
}

func handle(req *http.Request, dyn dynamic.Interface, localListKinds map[string]schema.GroupVersionResource) (*http.Response, error) {
	// path: /k8s/clusters/{clusterID}/v1/{steveType}[/{namespace}]/{name}
	parts := strings.Split(strings.TrimPrefix(req.URL.Path, "/"), "/")
	if len(parts) < 5 || parts[0] != "k8s" || parts[1] != "clusters" || parts[3] != "v1" {
		return errorResponse(req, http.StatusBadRequest, fmt.Sprintf("stevetest: unrecognized steve path %q", req.URL.Path))
	}

	gvr, ok := gvrForSteveType(parts[4], localListKinds)
	if !ok {
		return errorResponse(req, http.StatusNotFound, fmt.Sprintf("stevetest: unknown steve type %q", parts[4]))
	}

	var namespace, name string
	switch len(parts) {
	case 5:
		// list, cluster-scoped or across all namespaces
	case 6:
		name = parts[5]
	case 7:
		namespace, name = parts[5], parts[6]
	default:
		return errorResponse(req, http.StatusBadRequest, fmt.Sprintf("stevetest: unrecognized steve path %q", req.URL.Path))
	}

	if name != "" {
		return handleGet(req, dyn, gvr, namespace, name)
	}
	return handleList(req, dyn, gvr)
}

func handleGet(req *http.Request, dyn dynamic.Interface, gvr schema.GroupVersionResource, namespace, name string) (*http.Response, error) {
	var ri dynamic.ResourceInterface = dyn.Resource(gvr)
	if namespace != "" {
		ri = dyn.Resource(gvr).Namespace(namespace)
	}

	obj, err := ri.Get(req.Context(), name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return errorResponse(req, http.StatusNotFound, err.Error())
		}
		return errorResponse(req, http.StatusInternalServerError, err.Error())
	}
	return jsonResponse(req, obj.Object)
}

func handleList(req *http.Request, dyn dynamic.Interface, gvr schema.GroupVersionResource) (*http.Response, error) {
	query := req.URL.Query()

	listOpts := metav1.ListOptions{}
	if ls := query.Get("labelSelector"); ls != "" {
		listOpts.LabelSelector = ls
	}
	if l := query.Get("limit"); l != "" {
		if n, err := strconv.ParseInt(l, 10, 64); err == nil {
			listOpts.Limit = n
		}
	}

	namespace := ""
	for _, f := range query["filter"] {
		if v, ok := strings.CutPrefix(f, "metadata.namespace="); ok {
			namespace = v
		}
	}

	var ri dynamic.ResourceInterface = dyn.Resource(gvr)
	if namespace != "" {
		ri = dyn.Resource(gvr).Namespace(namespace)
	}

	list, err := ri.List(req.Context(), listOpts)
	if err != nil {
		return errorResponse(req, http.StatusInternalServerError, err.Error())
	}

	items := make([]any, len(list.Items))
	for i := range list.Items {
		items[i] = list.Items[i].Object
	}
	return jsonResponse(req, map[string]any{"data": items})
}

func jsonResponse(req *http.Request, body any) (*http.Response, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(raw)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func errorResponse(req *http.Request, code int, msg string) (*http.Response, error) {
	raw, _ := json.Marshal(map[string]string{"message": msg})
	return &http.Response{
		StatusCode: code,
		Body:       io.NopCloser(bytes.NewReader(raw)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}
