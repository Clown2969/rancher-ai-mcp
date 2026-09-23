package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/rancher/rancher-ai-mcp/pkg/converter"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// steveTypeURL returns the Steve collection URL for a resource type within
// the given cluster (use "local" for Rancher management-plane resources
// such as management.cattle.io Clusters/Projects).
func (c *Client) steveTypeURL(clusterID, steveType string) string {
	return strings.TrimRight(c.rancherURL, "/") + "/k8s/clusters/" + clusterID + "/v1/" + steveType
}

// steveHTTPClient builds an *http.Client honoring the same TLS settings
// (insecure / caBundle) used for the typed/dynamic clients.
func (c *Client) steveHTTPClient() *http.Client {
	if c.SteveTransport != nil {
		return &http.Client{Transport: c.SteveTransport}
	}
	transport := &http.Transport{}
	switch {
	case c.insecure:
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit operator opt-in
	case len(c.caBundle) > 0:
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(c.caBundle)
		transport.TLSClientConfig = &tls.Config{RootCAs: pool}
	}
	return &http.Client{Transport: transport}
}

// steveDo issues an authenticated GET against a Steve URL and returns the
// parsed JSON body, translating Steve's HTTP status codes into the same
// k8s.io/apimachinery errors callers already handle (errors.IsNotFound, etc).
func (c *Client) steveDo(ctx context.Context, token, rawURL string, query url.Values) (map[string]any, error) {
	if len(query) > 0 {
		rawURL += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.steveHTTPClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("steve request to %s: %w", rawURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading steve response from %s: %w", rawURL, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.NewGenericServerResponse(resp.StatusCode, "GET", schema.GroupResource{}, rawURL, steveMessage(body), 0, false)
	}

	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("decoding steve response from %s: %w", rawURL, err)
	}
	return parsed, nil
}

// steveMessage extracts Steve's "message" field from an error body, falling
// back to the raw (truncated) body when the response isn't a Steve error envelope.
func steveMessage(body []byte) string {
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Message != "" {
		return envelope.Message
	}
	const maxLen = 200
	s := strings.TrimSpace(string(body))
	if len(s) > maxLen {
		s = s[:maxLen] + "…"
	}
	return s
}

// SteveGet retrieves a single resource through Steve's own REST convention
// ({rancherURL}/k8s/clusters/{clusterID}/v1/{steveType}[/{namespace}]/{name})
// instead of the typed/dynamic Kubernetes API. Steve filters collections by
// per-item access instead of hard-403ing when the token lacks cluster-wide
// RBAC, which is the whole reason to go through it.
func (c *Client) SteveGet(ctx context.Context, token, clusterID string, gvr schema.GroupVersionResource, namespace, name string) (*unstructured.Unstructured, error) {
	if name == "" {
		// An empty name would build a URL with a trailing slash (".../{type}/{namespace}/"),
		// which reads as a collection request rather than a single-resource one. Standard
		// k8s Get semantics treat an empty name as not-found, so fail the same way here
		// instead of accidentally listing the collection.
		return nil, errors.NewNotFound(gvr.GroupResource(), name)
	}

	u := c.steveTypeURL(clusterID, converter.SteveTypeForGVR(gvr))
	if namespace != "" {
		u += "/" + namespace
	}
	u += "/" + name

	obj, err := c.steveDo(ctx, token, u, nil)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(gvr.GroupResource(), name)
		}
		return nil, err
	}

	return &unstructured.Unstructured{Object: obj}, nil
}

// SteveList lists resources of the given type through Steve, optionally
// scoped to a namespace and/or filtered by label selector, mirroring
// metav1.ListOptions semantics via Steve's own query parameters.
func (c *Client) SteveList(ctx context.Context, token, clusterID string, gvr schema.GroupVersionResource, namespace, labelSelector string, limit int64) ([]*unstructured.Unstructured, error) {
	u := c.steveTypeURL(clusterID, converter.SteveTypeForGVR(gvr))

	query := url.Values{}
	if namespace != "" {
		query.Set("filter", "metadata.namespace="+namespace)
	}
	if labelSelector != "" {
		query.Set("labelSelector", labelSelector)
	}
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}

	parsed, err := c.steveDo(ctx, token, u, query)
	if err != nil {
		return nil, err
	}

	rawItems, _ := parsed["data"].([]any)
	items := make([]*unstructured.Unstructured, 0, len(rawItems))
	for _, raw := range rawItems {
		obj, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		items = append(items, &unstructured.Unstructured{Object: obj})
	}
	return items, nil
}
