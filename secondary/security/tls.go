//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/logging"
)

/////////////////////////////////////////////
// TLS Connection
/////////////////////////////////////////////

var userAgentPrefix = "Go-http-client/1.1-indexer-"

//
// Setup client TLSConfig
//
func setupClientTLSConfig(host string) (*tls.Config, error) {

	setting := GetSecuritySetting()
	if setting == nil {
		return nil, fmt.Errorf("Security setting is nil")
	}

	if !setting.encryptionEnabled {
		return nil, nil
	}

	// Get certificate and cbauth TLS setting
	certInBytes := setting.certInBytes
	if len(certInBytes) == 0 {
		return nil, fmt.Errorf("No certificate has been provided. Can't establish ssl connection to %v", host)
	}

	pref := setting.tlsPreference

	// Setup  TLSConfig
	tlsConfig := &tls.Config{}

	//  Set up cert pool for rootCAs
	tlsConfig.RootCAs = x509.NewCertPool()
	tlsConfig.RootCAs.AppendCertsFromPEM(certInBytes)

	if IsLocal(host) {
		// skip server verify if it is localhost
		tlsConfig.InsecureSkipVerify = true
	} else {
		// setup server host name
		tlsConfig.ServerName = host
	}

	// setup prefer ciphers
	if pref != nil {
		tlsConfig.MinVersion = pref.MinVersion
		tlsConfig.CipherSuites = pref.CipherSuites
		tlsConfig.PreferServerCipherSuites = pref.PreferServerCipherSuites
	}

	return tlsConfig, nil
}

//
// Set up a TLS client connection.  This function does not close conn upon error.
//
func makeTLSConn(conn net.Conn, hostname, port string) (net.Conn, error) {

	// Setup TLS Config
	tlsConfig, err := setupClientTLSConfig(hostname)
	if err != nil {
		return nil, err
	}

	// Setup TLS connection
	if tlsConfig != nil {
		tlsConn := tls.Client(conn, tlsConfig)

		// Initiate TlS handshake.  This is optional since first Read() or Write() will
		// initiate handshake implicitly.  By performing handshake now, we can detect
		// setup issue early on.

		// Spawn new routine to enforce timeout
		errChannel := make(chan error, 2)

		go func() {
			timer := time.NewTimer(time.Duration(2 * time.Minute))
			defer timer.Stop()

			select {
			case errChannel <- tlsConn.Handshake():
			case <-timer.C:
				errChannel <- errors.New("Unable to finish TLS handshake with 2 minutes")
			}
		}()

		err = <-errChannel
		if err != nil {
			return nil, fmt.Errorf("TLS handshake failed when connecting to %v, err=%v\n", hostname, err)
		}

		logging.Infof("TLS connection created for %v", net.JoinHostPort(hostname, port))
		return tlsConn, nil
	}

	return conn, nil
}

//
// Setup a TCP client connection
//
func makeTCPConn(addr string) (net.Conn, error) {

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// Secure a TCP connection.   This function will not convert conn to a SSL port.
// So if encryption is required, conn.RemoteAddr must already be using a SSL port.
func SecureConn(conn net.Conn, hostname, port string) (net.Conn, error) {

	if EncryptionRequired(hostname, port) {
		return makeTLSConn(conn, hostname, port)
	}

	return conn, nil
}

//
// Setup a TCP or TLS client connection depending whether encryption is used.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func MakeConn(addr string) (net.Conn, error) {

	addr, hostname, port, err := EncryptPortFromAddr(addr)
	if err != nil {
		return nil, err
	}

	conn, err := makeTCPConn(addr)
	if err != nil {
		return nil, err
	}

	conn2, err2 := SecureConn(conn, hostname, port)
	if err2 != nil {
		conn.Close()
		return nil, err2
	}

	return conn2, nil
}

//
// Setup a TCP client connection depending whether encryption is used.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func MakeTCPConn(addr string) (*net.TCPConn, error) {

	addr, _, _, err := EncryptPortFromAddr(addr)
	if err != nil {
		return nil, err
	}

	conn, err := makeTCPConn(addr)
	if err != nil {
		return nil, err
	}

	return conn.(*net.TCPConn), nil
}

/////////////////////////////////////////////
// TLS Listener
/////////////////////////////////////////////

//
// Setup server TLSConfig
//
func setupServerTLSConfig() (*tls.Config, error) {

	setting := GetSecuritySetting()
	if setting == nil {
		return nil, fmt.Errorf("Security setting is nil")
	}

	if !setting.encryptionEnabled {
		return nil, nil
	}

	return getTLSConfigFromSetting(setting)
}

func getTLSConfigFromSetting(setting *SecuritySetting) (*tls.Config, error) {

	// Get certifiicate and cbauth config
	cert := setting.certificate
	if cert == nil {
		return nil, fmt.Errorf("No certificate has been provided. Can't establish ssl connectionv")
	}

	pref := setting.tlsPreference

	// set up TLS server config
	config := &tls.Config{}

	// set up certificate
	config.Certificates = []tls.Certificate{*cert}

	if pref != nil {
		// setup ciphers
		config.CipherSuites = pref.CipherSuites
		config.PreferServerCipherSuites = pref.PreferServerCipherSuites

		// set up other attributes
		config.MinVersion = pref.MinVersion
		config.ClientAuth = pref.ClientAuthType

		// set up client cert
		if pref.ClientAuthType != tls.NoClientCert {
			certInBytes := setting.certInBytes
			if len(certInBytes) == 0 {
				return nil, fmt.Errorf("No certificate has been provided. Can't establish ssl connectionv")
			}

			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(certInBytes)
			config.ClientCAs = caCertPool
		}
	}

	return config, nil
}

//
// Set up a TLS listener
//
func MakeTLSListener(tcpListener net.Listener) (net.Listener, error) {

	config, err := setupServerTLSConfig()
	if err != nil {
		return nil, err
	}

	if config != nil {
		listener := tls.NewListener(tcpListener, config)
		logging.Infof("TLS listener created for %v", listener.Addr().String())
		return listener, nil
	}

	return tcpListener, nil
}

//
// Make a new tcp listener for given address.
// Always make it secure, even if the security is not enabled.
//
func MakeAndSecureTCPListener(addr string) (net.Listener, error) {

	addr, _, _, err := EncryptPortFromAddr(addr)
	if err != nil {
		return nil, err
	}

	tcpListener, err := makeTCPListener(addr)
	if err != nil {
		return nil, err
	}

	setting := GetSecuritySetting()
	if setting == nil {
		return nil, fmt.Errorf("Security setting required for TLS listener")
	}

	config, err := getTLSConfigFromSetting(setting)
	if err != nil {
		return nil, err
	}

	tlsListener := tls.NewListener(tcpListener, config)
	return tlsListener, nil
}

//
// Set up a TCP listener
//
func makeTCPListener(addr string) (net.Listener, error) {

	tcpListener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return tcpListener, nil
}

//
// Secure a TCP listener.  If encryption is requird, listener must already
// setup with SSL port.
//
func SecureListener(listener net.Listener) (net.Listener, error) {
	if EncryptionEnabled() {
		return MakeTLSListener(listener)
	}

	return listener, nil
}

//
// Set up a TLS or TCP listener, depending on whether encryption is used.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func MakeListener(addr string) (net.Listener, error) {

	addr, _, _, err := EncryptPortFromAddr(addr)
	if err != nil {
		return nil, err
	}

	listener, err := makeTCPListener(addr)
	if err != nil {
		return nil, err
	}

	listener2, err2 := SecureListener(listener)
	if err2 != nil {
		listener.Close()
		return nil, err
	}

	return listener2, nil
}

/////////////////////////////////////////////
// HTTP / HTTPS Client
/////////////////////////////////////////////

//
// Get URL.  This function will convert non-SSL port to SSL port when necessary.
//
func GetURL(u string) (*url.URL, error) {

	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		u = "http://" + u
	}

	parsedUrl, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	parsedUrl.Host, _, _, err = EncryptPortFromAddr(parsedUrl.Host)
	if err != nil {
		return nil, err
	}

	host, port, err := net.SplitHostPort(parsedUrl.Host)
	if err != nil {
		return nil, err
	}

	if EncryptionRequired(host, port) {
		parsedUrl.Scheme = "https"
	} else {
		parsedUrl.Scheme = "http"
	}

	return parsedUrl, nil
}

//
// Setup TLSTransport
//
func getTLSTransport(host string) (*http.Transport, error) {

	tlsConfig, err := setupClientTLSConfig(host)
	if err != nil {
		return nil, err
	}

	// There is no clone method for http.DefaultTransport
	// (transport := *(http.DefaultTransport.(*http.Transport))) causes runtime issue
	// see https://github.com/golang/go/issues/26013
	// The following code is a simple solution, but can cause upgrade issue.
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	transport.TLSClientConfig = tlsConfig

	return transport, nil
}

//
// Secure HTTP Client if necessary
//
func SecureClient(client *http.Client, u string) error {

	parsedUrl, err := GetURL(u)
	if err != nil {
		return err
	}

	host, port, err := net.SplitHostPort(parsedUrl.Host)
	if err != nil {
		return err
	}

	if EncryptionRequired(host, port) {
		t, err := getTLSTransport(host)
		if err != nil {
			return err
		}

		client.Transport = t
	}

	return nil
}

//
// Get HTTP client.  If encryption is enabled, client will be setup with TLS Transport.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func MakeClient(u string) (*http.Client, error) {

	// create a new Client.  Do not use http.DefaultClient.
	client := &http.Client{}
	if err := SecureClient(client, u); err != nil {
		return nil, err
	}

	return client, nil
}

/////////////////////////////////////////////
// HTTP / HTTPS Request
/////////////////////////////////////////////

type RequestParams struct {
	Timeout   time.Duration
	UserAgent string
}

//
// HTTP Get with Basic Auth.  If encryption is enabled, the request is made over HTTPS.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func GetWithAuth(u string, params *RequestParams) (*http.Response, error) {

	url, err := GetURL(u)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		logging.Verbosef("GetWithAuth: url %v elapsed %v", url.String(), time.Now().Sub(start))
	}()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	if params != nil && params.UserAgent != "" {
		req.Header.Add("User-agent", userAgentPrefix+params.UserAgent)
	}

	err = cbauth.SetRequestAuthVia(req, nil)
	if err != nil {
		return nil, err
	}

	client, err := MakeClient(url.String())
	if err != nil {
		return nil, err
	}

	if params != nil && params.Timeout >= time.Duration(0) {
		client.Timeout = params.Timeout
	}

	return client.Do(req)
}

//
// HTTP Post with Basic Auth.  If encryption is enabled, the request is made over HTTPS.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func PostWithAuth(u string, bodyType string, body io.Reader, params *RequestParams) (*http.Response, error) {

	url, err := GetURL(u)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		logging.Verbosef("PostWithAuth: url %v elapsed %v", url.String(), time.Now().Sub(start))
	}()

	req, err := http.NewRequest("POST", url.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)

	err = cbauth.SetRequestAuthVia(req, nil)
	if err != nil {
		return nil, err
	}

	client, err := MakeClient(url.String())
	if err != nil {
		return nil, err
	}

	if params != nil && params.Timeout >= time.Duration(0) {
		client.Timeout = params.Timeout
	}

	return client.Do(req)
}

//
// HTTP Get.  If encryption is enabled, the request is made over HTTPS.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func Get(u string, params *RequestParams) (*http.Response, error) {

	url, err := GetURL(u)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		logging.Verbosef("Get: url %v elapsed %v", url.String(), time.Now().Sub(start))
	}()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	client, err := MakeClient(url.String())
	if err != nil {
		return nil, err
	}

	if params != nil && params.Timeout >= time.Duration(0) {
		client.Timeout = params.Timeout
	}

	return client.Do(req)
}

//
// HTTP Post.  If encryption is enabled, the request is made over HTTPS.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func Post(u string, bodyType string, body io.Reader, params *RequestParams) (*http.Response, error) {

	url, err := GetURL(u)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		logging.Verbosef("Post: url %v elapsed %v", url.String(), time.Now().Sub(start))
	}()

	req, err := http.NewRequest("POST", url.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)

	client, err := MakeClient(url.String())
	if err != nil {
		return nil, err
	}

	if params != nil && params.Timeout >= time.Duration(0) {
		client.Timeout = params.Timeout
	}

	return client.Do(req)
}

/////////////////////////////////////////////
// HTTP / HTTPS Server
/////////////////////////////////////////////

//
// Make HTTPS Server
//
func MakeHTTPSServer(server *http.Server) error {

	// get server TLSConfig
	config, err := setupServerTLSConfig()
	if err != nil {
		return err
	}

	if config != nil {
		server.TLSConfig = config
		server.TLSNextProto = make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0)

		logging.Infof("HTTPS server created for %v", server.Addr)
	}

	return nil
}

//
// Secure the HTTP Server by setting TLS config
// Always secure the given HTTP server (even if the security is not enabled).
//
func SecureHTTPServer(server *http.Server) error {

	setting := GetSecuritySetting()
	if setting == nil {
		return fmt.Errorf("Security setting required for https server")
	}

	config, err := getTLSConfigFromSetting(setting)
	if err != nil {
		return err
	}

	if config != nil {
		server.TLSConfig = config
		server.TLSNextProto = make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0)

		logging.Infof("HTTPS server created for %v", server.Addr)
	}

	return nil
}

//
// Make HTTP Server
//
func makeHTTPServer(addr string) (*http.Server, error) {

	srv := &http.Server{
		Addr: addr,
	}

	return srv, nil
}

//
// Secure HTTP server.
// It expects that server must already be setup with HTTPS port.
//
func SecureServer(server *http.Server) error {

	if EncryptionEnabled() {
		return MakeHTTPSServer(server)
	}

	return nil
}

//
// Make HTTP/HTTPS server
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func MakeHTTPServer(addr string) (*http.Server, error) {

	addr, _, _, err := EncryptPortFromAddr(addr)
	if err != nil {
		return nil, err
	}

	server, err := makeHTTPServer(addr)
	if err != nil {
		return nil, err
	}

	if err := SecureServer(server); err != nil {
		return nil, err
	}

	return server, nil
}
