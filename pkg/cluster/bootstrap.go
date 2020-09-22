package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/rancher/k3s/pkg/bootstrap"
	"github.com/rancher/k3s/pkg/clientaccess"
	"github.com/rancher/k3s/pkg/version"
	"github.com/sirupsen/logrus"
)

// Bootstrap attempts to load a managed database driver, if one has been initialized or should be created/joined.
// It then checks to see if the cluster needs to load boostrap data, and if so, loads data into the
// ControlRuntimeBoostrap struct, either via HTTP or from the datastore.
func (c *Cluster) Bootstrap(ctx context.Context) error {
	if err := c.assignManagedDriver(ctx); err != nil {
		return err
	}

	shouldBootstrap, err := c.shouldBootstrapLoad(ctx)
	if err != nil {
		return err
	}

	c.shouldBootstrap = shouldBootstrap

	if shouldBootstrap {
		if err := c.bootstrap(ctx); err != nil {
			return err
		}
	}

	return nil
}

// shouldBootstrapLoad returns true if we need to load ControlRuntimeBootstrap data again.
// This is controlled by a stamp file on disk that records successful bootstrap using a hash of the join token.
func (c *Cluster) shouldBootstrapLoad(ctx context.Context) (bool, error) {
	// If using a managed database, see if we need to bootstrap
	if c.managedDB != nil {
		c.runtime.HTTPBootstrap = true
		// No URL to join, so don't need to bootstrap
		if c.config.JoinURL == "" {
			return false, nil
		}

		// Validate the join token
		info, err := clientaccess.ParseAndValidateTokenForUser(c.config.JoinURL, c.config.Token, "server")
		if err != nil {
			// If we got a Service Unavailable error from the join URL, and the managed database is already
			// initialized, bypass additional bootstrap checks.
			// Normally this would be fatal, but in the case of quorum loss the other server cannot serve the
			// CA bundle until the datastore is up, and the datastore can't come up until we come up to restore quorum.
			// Break this deadlock by ignoring the failure if the managed database has already been initialized.
			// If the user has changed the JoinURL to target a different cluster and we SHOULD re-bootstrap,
			// etcd will fail to join the new cluster due to invalid certificates, and startup will fail until
			// another node can come online to restore quorum, or the cluster is reset by the administrator.
			if errors.Is(err, clientaccess.ErrServiceUnavailable) {
				if isInitialized, err2 := c.managedDB.IsInitialized(ctx, c.config); err2 != nil {
					logrus.Warnf("Failed to check managed database initialization: %v", err2)
				} else if isInitialized {
					logrus.Warnf("Ignoring bootstrap token validation error on initialized member due to cluster quorum loss: %v", err)
					return false, nil
				}
			}
			return false, err
		}

		c.clientAccessInfo = info
	}

	stamp := c.bootstrapStamp()
	if _, err := os.Stat(stamp); err == nil {
		logrus.Info("Cluster bootstrap already complete")
		return false, nil
	}

	if c.managedDB != nil && c.config.Token == "" {
		return false, fmt.Errorf(version.ProgramUpper + "_TOKEN is required to join a cluster")
	}

	return true, nil
}

// bootstrapped touches a file to indicate that bootstrap has been completed.
func (c *Cluster) bootstrapped() error {
	stamp := c.bootstrapStamp()
	if err := os.MkdirAll(filepath.Dir(stamp), 0700); err != nil {
		return err
	}

	// return if file already exists
	if _, err := os.Stat(stamp); err == nil {
		return nil
	}

	// otherwise try to create it
	f, err := os.Create(stamp)
	if err != nil {
		return err
	}

	return f.Close()
}

// httpBootstrap retrieves bootstrap data (certs and keys, etc) from the remote server via HTTP
// and loads it into the ControlRuntimeBootstrap struct. Unlike the storage bootstrap path,
// this data does not need to be decrypted since it is generated on-demand by an existing server.
func (c *Cluster) httpBootstrap() error {
	content, err := clientaccess.Get("/v1-"+version.Program+"/server-bootstrap", c.clientAccessInfo)
	if err != nil {
		return err
	}

	return bootstrap.Read(bytes.NewBuffer(content), &c.runtime.ControlRuntimeBootstrap)
}

// bootstrap performs cluster bootstrapping, either via HTTP (for managed databases) or direct load from datastore.
func (c *Cluster) bootstrap(ctx context.Context) error {
	c.joining = true

	// bootstrap managed database via HTTP
	if c.runtime.HTTPBootstrap {
		return c.httpBootstrap()
	}

	if err := c.storageBootstrap(ctx); err != nil {
		return err
	}

	return nil
}

// bootstrapStamp returns the path to a file in datadir/db that is used to record
// that a cluster has been joined. The filename is based on a portion of the sha256 hash of the token.
// We hash the token value exactly as it is provided by the user, NOT the normalized version.
func (c *Cluster) bootstrapStamp() string {
	return filepath.Join(c.config.DataDir, "db/joined-"+keyHash(c.config.Token))
}
