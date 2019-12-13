package metrics

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	routev1 "github.com/openshift/client-go/route/clientset/versioned/typed/route/v1"
	"github.com/openshift/library-go/test/library"

	"github.com/Jeffail/gabs"
)

const monitoringNamespace = "openshift-monitoring"
const queryRoute = "thanos-querier"

func CheckMetricsCollection(t *testing.T, config *rest.Config, query string) {
	client, err := NewPrometheusClient(config)
	if err != nil {
		t.Fatalf("error creating prometheus client: %v", err)
	}
	client.WaitForQueryReturnOne(t, 30*time.Second, query)
}

func NewPrometheusClient(config *rest.Config) (*RouteClient, error) {
	routeClient, err := routev1.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error creating the route route client: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error creating the kube client: %w", err)
	}
	return NewRouteClient(routeClient, kubeClient, monitoringNamespace, queryRoute)
}

// RouteClient provides access to the prometheus-k8s statefulset via its
// public facing route.
type RouteClient struct {
	// Host address of the public route.
	host string
	// ServiceAccount bearer token to pass through Openshift oauth proxy.
	token string
}

// NewRouteClient creates and returns a new RouteClient.
func NewRouteClient(
	routeClient routev1.RouteV1Interface,
	kubeClient kubernetes.Interface,
	namespace, name string,
) (*RouteClient, error) {
	route, err := routeClient.Routes(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	host := route.Spec.Host

	secrets, err := kubeClient.CoreV1().Secrets(monitoringNamespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var token string

	for _, secret := range secrets.Items {
		_, dockerToken := secret.Annotations["openshift.io/create-dockercfg-secrets"]
		e2eToken := strings.Contains(secret.Name, "cluster-monitoring-operator-e2e-token-")

		// we have to skip the token secret that contains the openshift.io/create-dockercfg-secrets annotation
		// as this is the token to talk to the internal registry.
		if !dockerToken && e2eToken {
			token = string(secret.Data["token"])
		}
	}

	return &RouteClient{
		host:  host,
		token: token,
	}, nil
}

// PrometheusQuery runs an http get request against the Prometheus query api and returns
// the response body.
func (c *RouteClient) PrometheusQuery(query string) ([]byte, error) {
	// #nosec
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}

	req, err := http.NewRequest("GET", "https://"+c.host+"/api/v1/query", nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("query", query)
	req.URL.RawQuery = q.Encode()

	req.Header.Add("Authorization", "Bearer "+c.token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code response, want %d, got %d", http.StatusOK, resp.StatusCode)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// AlertmanagerQuery runs an http get request against the Prometheus query api and returns
// the response body.
func (c *RouteClient) AlertmanagerQuery(kvs ...string) ([]byte, error) {
	// #nosec
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}

	req, err := http.NewRequest("GET", "https://"+c.host+"/api/v2/alerts", nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	for i := 0; i < len(kvs)/2; i++ {
		q.Add(kvs[i*2], kvs[i*2+1])
	}
	req.URL.RawQuery = q.Encode()

	req.Header.Add("Authorization", "Bearer "+c.token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code response, want %d, got %d", http.StatusOK, resp.StatusCode)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// GetFirstValueFromPromQuery takes a query api response body and returns the
// value of the first timeseries. If body contains multiple timeseries
// GetFirstValueFromPromQuery errors.
func GetFirstValueFromPromQuery(body []byte) (int, error) {
	res, err := gabs.ParseJSON(body)
	if err != nil {
		return 0, err
	}

	count, err := res.ArrayCountP("data.result")
	if err != nil {
		return 0, err
	}

	if count != 1 {
		return 0, fmt.Errorf("expected body to contain single timeseries but got %v", count)
	}

	timeseries, err := res.ArrayElementP(0, "data.result")
	if err != nil {
		return 0, err
	}

	value, err := timeseries.ArrayElementP(1, "value")
	if err != nil {
		return 0, err
	}

	v, err := strconv.Atoi(value.Data().(string))
	if err != nil {
		return 0, fmt.Errorf("failed to parse query value: %v", err)
	}

	return v, nil
}

// WaitForQueryReturnGreaterEqualOne see WaitForQueryReturn.
func (c *RouteClient) WaitForQueryReturnGreaterEqualOne(t *testing.T, timeout time.Duration, query string) {
	c.WaitForQueryReturn(t, timeout, query, func(v int) error {
		if v >= 1 {
			return nil
		}

		return fmt.Errorf("expected value to equal or greater than 1 but got %v", v)
	})
}

// WaitForQueryReturnOne see WaitForQueryReturn.
func (c *RouteClient) WaitForQueryReturnOne(t *testing.T, timeout time.Duration, query string) {
	c.WaitForQueryReturn(t, timeout, query, func(v int) error {
		if v == 1 {
			return nil
		}

		return fmt.Errorf("expected value to equal 1 but got %v", v)
	})
}

// WaitForQueryReturn waits for a given PromQL query for a given time interval
// and validates the **first and only** result with the given validate function.
func (c *RouteClient) WaitForQueryReturn(t *testing.T, timeout time.Duration, query string, validate func(int) error) {
	err := library.Poll(5*time.Second, timeout, func() error {
		body, err := c.PrometheusQuery(query)
		if err != nil {
			t.Fatal(err)
		}

		v, err := GetFirstValueFromPromQuery(body)
		if err != nil {
			return fmt.Errorf("error getting first value from response body %q for query %q: %w", string(body), query, err)
		}

		if err := validate(v); err != nil {
			return fmt.Errorf("error validating response body %q for query %q: %w", string(body), query, err)
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}
