package cluster

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"path/filepath"

	"github.com/rancher/dynamiclistener"
	"github.com/rancher/dynamiclistener/factory"
	"github.com/rancher/dynamiclistener/storage/file"
	"github.com/rancher/dynamiclistener/storage/kubernetes"
	"github.com/rancher/dynamiclistener/storage/memory"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/version"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/core"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// newListener returns a new TCP listener and HTTP reqest handler using dynamiclistener.
// dynamiclistener will use the cluster's Server CA to sign the dynamically generate certificate,
// and will sync the certs into the Kubernetes datastore, with a local disk cache.
func (c *Cluster) newListener(ctx context.Context) (net.Listener, http.Handler, error) {
	tcp, err := dynamiclistener.NewTCPListener(c.config.BindAddress, c.config.SupervisorPort)
	if err != nil {
		return nil, nil, err
	}

	cert, key, err := factory.LoadCerts(c.runtime.ServerCA, c.runtime.ServerCAKey)
	if err != nil {
		return nil, nil, err
	}

	storage := tlsStorage(ctx, c.config.DataDir, c.runtime)
	return dynamiclistener.NewListener(tcp, storage, cert, key, dynamiclistener.Config{
		ExpirationDaysCheck: config.CertificateRenewDays,
		Organization:        []string{version.Program},
		SANs:                append(c.config.SANs, "localhost", "kubernetes", "kubernetes.default", "kubernetes.default.svc."+c.config.ClusterDomain),
		CN:                  version.Program,
		TLSConfig: &tls.Config{
			ClientAuth:   tls.RequestClientCert,
			MinVersion:   c.config.TLSMinVersion,
			CipherSuites: c.config.TLSCipherSuites,
		},
	})
}

// initClusterAndHTTPS sets up the dynamic tls listener, request router,
// and cluster database. Once the database is up, it starts the supervisor http server.
func (c *Cluster) initClusterAndHTTPS(ctx context.Context) error {
	l, handler, err := c.newListener(ctx)
	if err != nil {
		return err
	}

	handler, err = c.getHandler(handler)
	if err != nil {
		return err
	}

	l, handler, err = c.initClusterDB(ctx, l, handler)
	if err != nil {
		return err
	}

	server := http.Server{
		Handler:  handler,
		ErrorLog: log.New(logrus.StandardLogger().Writer(), "Cluster-Http-Server ", log.LstdFlags),
	}

	go func() {
		err := server.Serve(l)
		logrus.Fatalf("server stopped: %v", err)
	}()

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	return nil
}

// tlsStorage creates an in-memory cache for dynamiclistener's certificate, backed by a file on disk
// and the Kubernetes datastore.
func tlsStorage(ctx context.Context, dataDir string, runtime *config.ControlRuntime) dynamiclistener.TLSStorage {
	fileStorage := file.New(filepath.Join(dataDir, "tls/dynamic-cert.json"))
	cache := memory.NewBacked(fileStorage)
	return kubernetes.New(ctx, func() *core.Factory {
		return runtime.Core
	}, metav1.NamespaceSystem, version.Program+"-serving", cache)
}
