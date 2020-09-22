package clientaccess

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

var (
	ErrServiceUnavailable = &http.ProtocolError{ErrorString: "service unavailable"}
	insecureClient        = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
)

const (
	tokenPrefix = "K10"
	tokenFormat = "%s%s::%s:%s"
)

type OverrideURLCallback func(config []byte) (*url.URL, error)

// WriteClientKubeConfig generates a kubeconfig at destFile that can be used to connect to a server at url with the given certs and keys
func WriteClientKubeConfig(destFile string, url string, serverCAFile string, clientCertFile string, clientKeyFile string) error {
	serverCA, err := ioutil.ReadFile(serverCAFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read %s", serverCAFile)
	}

	clientCert, err := ioutil.ReadFile(clientCertFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read %s", clientCertFile)
	}

	clientKey, err := ioutil.ReadFile(clientKeyFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read %s", clientKeyFile)
	}

	config := clientcmdapi.NewConfig()

	cluster := clientcmdapi.NewCluster()
	cluster.CertificateAuthorityData = serverCA
	cluster.Server = url

	authInfo := clientcmdapi.NewAuthInfo()
	authInfo.ClientCertificateData = clientCert
	authInfo.ClientKeyData = clientKey

	context := clientcmdapi.NewContext()
	context.AuthInfo = "default"
	context.Cluster = "default"

	config.Clusters["default"] = cluster
	config.AuthInfos["default"] = authInfo
	config.Contexts["default"] = context
	config.CurrentContext = "default"

	return clientcmd.WriteToFile(*config, destFile)
}

type Info struct {
	CACerts  []byte `json:"cacerts,omitempty"`
	BaseURL  string `json:"baseurl,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	caHash   string
}

// String returns the token data, templated according to the token format
func (info *Info) String() string {
	return fmt.Sprintf(tokenFormat, tokenPrefix, hashCA(info.CACerts), info.Username, info.Password)
}

// ParseAndValidateToken parses a token, downloads and validates the server's CA bundle,
// and validates it according to the caHash from the token if set.
func ParseAndValidateToken(server string, token string) (*Info, error) {
	info, err := parseToken(token)
	if err != nil {
		return nil, err
	}

	if err := info.setServer(server); err != nil {
		return nil, err
	}

	if info.caHash != "" {
		if err := info.validateCAHash(); err != nil {
			return nil, err
		}
	}

	return info, nil
}

// ParseAndValidateToken parses a token with user override, downloads and
// validates the server's CA bundle, and validates it according to the caHash from the token if set.
func ParseAndValidateTokenForUser(server string, token string, username string) (*Info, error) {
	info, err := parseToken(token)
	if err != nil {
		return nil, err
	}

	info.Username = username

	if err := info.setServer(server); err != nil {
		return nil, err
	}

	if info.caHash != "" {
		if err := info.validateCAHash(); err != nil {
			return nil, err
		}
	}

	return info, nil
}

// validateCACerts returns a boolean indicating whether or not a CA bundle matches the provided hash,
// and a string containing the hash of the CA bundle.
func validateCACerts(cacerts []byte, hash string) (bool, string) {
	if len(cacerts) == 0 && hash == "" {
		return true, ""
	}

	newHash := hashCA(cacerts)
	return hash == newHash, newHash
}

// hashCA returns the hex-encoded SHA256 digest of a byte array.
func hashCA(cacerts []byte) string {
	digest := sha256.Sum256(cacerts)
	return hex.EncodeToString(digest[:])
}

// ParseUsernamePassword returns the username and password portion of a token string,
// along with a bool indicating if the token was successfully parsed.
func ParseUsernamePassword(token string) (string, string, bool) {
	info, err := parseToken(token)
	if err != nil {
		return "", "", false
	}
	return info.Username, info.Password, true
}

// parseToken parses a token into an Info struct
func parseToken(token string) (*Info, error) {
	var info = &Info{}

	if !strings.HasPrefix(token, tokenPrefix) {
		token = fmt.Sprintf(tokenFormat, tokenPrefix, "", "", token)
	}

	// Strip off the prefix
	token = token[len(tokenPrefix):]

	parts := strings.SplitN(token, "::", 2)
	token = parts[0]
	if len(parts) > 1 {
		info.caHash = parts[0]
		token = parts[1]
	}

	parts = strings.SplitN(token, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid token format")
	}

	info.Username = parts[0]
	info.Password = parts[1]

	return info, nil
}

// GetHTTPClient returns a http client that validates TLS server certificates using the provided CA bundle.
// If the CA bundle is empty, it validates using the default http client using the OS CA bundle.
// If the CA bundle is not empty but does not contain any valid certs, it validates using
// an empty CA bundle (which will always fail).
func GetHTTPClient(cacerts []byte) *http.Client {
	if len(cacerts) == 0 {
		return http.DefaultClient
	}

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(cacerts)

	return &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			TLSClientConfig: &tls.Config{
				RootCAs: pool,
			},
		},
	}
}

// Get makes a request to a subpath of info's BaseURL
func Get(path string, info *Info) ([]byte, error) {
	u, err := url.Parse(info.BaseURL)
	if err != nil {
		return nil, err
	}
	u.Path = path
	return get(u.String(), GetHTTPClient(info.CACerts), info.Username, info.Password)
}

// setServer sets the BaseURL and CACerts fields of the Info by connecting to the server
// and storing the CA bundle.
func (info *Info) setServer(server string) error {
	url, err := url.Parse(server)
	if err != nil {
		return errors.Wrapf(err, "Invalid server url, failed to parse: %s", server)
	}

	if url.Scheme != "https" {
		return fmt.Errorf("only https:// URLs are supported, invalid scheme: %s", server)
	}

	for strings.HasSuffix(url.Path, "/") {
		url.Path = url.Path[:len(url.Path)-1]
	}

	cacerts, err := getCACerts(*url)
	if err != nil {
		return err
	}

	info.BaseURL = url.String()
	info.CACerts = cacerts
	return nil
}

// ValidateCAHash validates that info's caHash matches the CACerts hash.
func (info *Info) validateCAHash() error {
	if ok, serverHash := validateCACerts(info.CACerts, info.caHash); !ok {
		return fmt.Errorf("token CA hash does not match the server CA hash: %s != %s", info.caHash, serverHash)
	}

	return nil
}

// getCACerts retrieves the CA bundle from a server.
// An error is raised if the CA bundle cannot be retrieved,
// or if the server's cert is not signed by the returned bundle.
func getCACerts(u url.URL) ([]byte, error) {
	u.Path = "/cacerts"
	url := u.String()

	// This first request is expected to fail. If the server has
	// a cert that can be validated using the default CA bundle, return
	// success with no CA certs.
	_, err := get(url, http.DefaultClient, "", "")
	if err == nil {
		return nil, nil
	}

	// Download the CA bundle using a client that does not validate certs.
	cacerts, err := get(url, insecureClient, "", "")
	if err != nil {
		return nil, errors.Wrap(err, "failed to get CA certs")
	}

	// Request the CA bundle again, validating that the CA bundle can be loaded
	// and used to validate the server certificate. This should only fail if we somehow
	// get an empty CA bundle. or if the dynamiclistener cert is incorrectly signed.
	_, err = get(url, GetHTTPClient(cacerts), "", "")
	if err != nil {
		return nil, errors.Wrap(err, "CA cert validation failed")
	}

	return cacerts, nil
}

// get makes a request to a url using a provided client, username, and password,
// returning the response body.
func get(u string, client *http.Client, username, password string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	if username != "" {
		req.SetBasicAuth(username, password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Return a unique error for 503 responses so that we can handle them when bootstrapping
	if resp.StatusCode == http.StatusServiceUnavailable {
		return nil, ErrServiceUnavailable
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", u, resp.Status)
	}

	return ioutil.ReadAll(resp.Body)
}
