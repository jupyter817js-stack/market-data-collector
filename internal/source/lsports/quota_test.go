package lsports

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGetPackageQuotaParsesEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/Package/GetPackageQuota" {
			t.Fatalf("expected /Package/GetPackageQuota, got %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{
			"Header":{"HttpStatusCode":200,"Errors":null},
			"Body":{"CreditLimit":100,"UsedCredit":7,"CreditRemaining":93}
		}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, Credentials{PackageID: 4044, Username: "u", Password: "p"}, 5*time.Second)
	q, _, err := c.GetPackageQuota(context.Background())
	if err != nil {
		t.Fatalf("GetPackageQuota: %v", err)
	}
	if q.CreditLimit != 100 || q.UsedCredit != 7 || q.CreditRemaining != 93 {
		t.Fatalf("unexpected quota: %+v", q)
	}
}

func TestGetPackageQuotaSurfacesEnvelopeErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"Header":{"HttpStatusCode":401,"Errors":[{"Message":"invalid credentials"}]},
			"Body":null
		}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, Credentials{PackageID: 4044, Username: "u", Password: "p"}, 5*time.Second)
	_, _, err := c.GetPackageQuota(context.Background())
	if err == nil {
		t.Fatalf("expected envelope-level error, got nil")
	}
}
