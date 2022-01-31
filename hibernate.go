package cbft

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"

	jp "github.com/dustin/go-jsonpointer"
	"github.com/pkg/errors"
)

const DefaultNsStatsSampleIntervalSecs = 30
const LastAccessedTime = "last_access_time"

type NsStatsSample struct {
	Url      string        // Ex: "http://10.0.0.1:8095".
	Start    time.Time     // When we started to get this sample.
	Duration time.Duration // How long it took to get this sample.
	Error    error
	Data     []byte // response from the endpoint.
}

type NsStatsOptions struct {
	NsStatsSampleInterval time.Duration
}

type MonitorNsStats struct {
	url      string // REST URL to monitor
	sampleCh chan NsStatsSample
	options  NsStatsOptions
	stopCh   chan struct{}
}

// Ref: https://stackoverflow.com/questions/31480710/validate-url-with-standard-package-in-go
func isValidURL(URL string) bool {
	u, err := url.Parse(URL)
	return err == nil && u.Scheme != "" && u.Host != ""
}

// NsStats for each index.
type NsIndexStats struct {
	LastAccessedTime string
	// temporarily set the type as string since until the first
	// access, the value is "", which results in an error while
	// converting to date-time
}

//  Unmarshals /nsstats and returns the relevant stats in a struct
// Currently, only returns the last_access_time for each index.
func unmarshalNsStats(data []byte, mgr *cbgt.Manager) (map[string]NsIndexStats, error) {
	result := make(map[string]NsIndexStats)
	cfg := mgr.Cfg()
	indexDefs, _, err := cbgt.CfgGetIndexDefs(cfg)
	if err != nil {
		log.Printf("error getting index defs")
		return result, errors.Wrapf(err, "error getting index defs")
	}
	for _, index := range indexDefs.IndexDefs {
		bucketName := index.SourceName
		indexName := index.Name
		path := "/" + bucketName + ":" + indexName + ":" + LastAccessedTime
		resByte, err := jp.Find(data, path)
		if err != nil {
			log.Printf("err: %e", err)
			return result, errors.Wrapf(err, "error extracting data from response")
		}
		result[indexName] = NsIndexStats{
			LastAccessedTime: string(resByte),
		}
	}
	return result, nil
}

// Returns a FQDN URL to the /nsstats endpoint.
func getNsStatsURL(mgr *cbgt.Manager, bindHTTP string) (string, error) {
	cfg := mgr.Cfg()

	nodeDefs, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		log.Printf("Error getting nodedefs: %s", err.Error())
		return "", errors.Wrapf(err, "Error getting nodedefs")
	}
	if nodeDefs == nil {
		return "", fmt.Errorf("empty nodedefs")
	}

	prefix := mgr.Options()["urlPrefix"]
	// polling one node endpoint is enough - avoids duplicate statistics.
	for _, node := range nodeDefs.NodeDefs {
		fqdnURL := "http://" + bindHTTP + prefix + "/api/nsstats"
		// no auth required for /nsstats
		secSettings := cbgt.GetSecuritySetting()
		if secSettings.EncryptionEnabled {
			if httpsURL, err := node.HttpsURL(); err == nil {
				fqdnURL = httpsURL
			}
		}
		// request error: parse 192.168.1.6:9200/api/nsstats: first path segment in URL cannot contain colon
		// Ref: https://stackoverflow.com/questions/54392948/first-path-segment-in-url-cannot-contain-colon
		fqdnURL = strings.Trim(fqdnURL, "\n")
		log.Printf("FQDN URL: %s", fqdnURL)
		if isValidURL(fqdnURL) {
			return fqdnURL, nil
		}
	}
	return "", nil
}

func HibernateEndpoint(mgr *cbgt.Manager, bindHTTP string) error {
	url, err := getNsStatsURL(mgr, bindHTTP)
	if err != nil {
		return errors.Wrapf(err, "error getting /nsstats endpoint URL")
	}

	nsStatsSample := make(chan NsStatsSample)
	options := &NsStatsOptions{
		NsStatsSampleInterval: 10 * time.Second,
	}

	resCh, err := startNsMonitor(url, nsStatsSample, *options, mgr)
	if err != nil {
		log.Printf("start ns monitor error")
		return errors.Wrapf(err, "error starting NSMonitor")
	}
	go func() {
		for r := range resCh.sampleCh {
			result, err := unmarshalNsStats(r.Data, mgr)
			if err != nil {
				log.Printf("error unmarshalling stats: %e", err)
				return err
			}
			log.Printf("Result: %+v", result)
		}
	}()

	return err
}

func startNsMonitor(url string, sampleCh chan NsStatsSample,
	options NsStatsOptions, mgr *cbgt.Manager) (*MonitorNsStats, error) {
	n := &MonitorNsStats{
		url:      url,
		sampleCh: sampleCh,
		options:  options,
		stopCh:   make(chan struct{}),
	}

	go n.runNode(url, mgr)

	return n, nil
}

func (n *MonitorNsStats) runNode(url string, mgr *cbgt.Manager) {
	NsStatsSampleInterval := n.options.NsStatsSampleInterval
	if NsStatsSampleInterval <= 0 {
		NsStatsSampleInterval =
			DefaultNsStatsSampleIntervalSecs * time.Second
	}

	NsStatsTicker := time.NewTicker(NsStatsSampleInterval)
	// want this to be continuously polled, hence not stopped.

	for {
		select {
		case <-n.stopCh:
			return

		case t, ok := <-NsStatsTicker.C:
			if !ok {
				return
			}
			n.sample(url, t, mgr)
		}
	}
}

func (n *MonitorNsStats) Stop() {
	close(n.stopCh)
}

func (n *MonitorNsStats) sample(url string, start time.Time,
	mgr *cbgt.Manager) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("request error: %s", err.Error())
		return
	}

	// res, err := http.Get(url) //don't use http.Get  - no default timeout
	client := cbgt.HttpClient()
	res, err := client.Do(req)
	if err != nil {
		log.Printf("response error: %s", err.Error())
		res.Body.Close()
		// closing here since defer will not apply on a premature return
		return
	}
	defer res.Body.Close()
	duration := time.Since(start)

	data := []byte{}
	if err == nil && res != nil {
		if res.StatusCode == 200 {
			var dataErr error

			data, dataErr = ioutil.ReadAll(res.Body)
			if err == nil && dataErr != nil {
				err = dataErr
			}
		} else {
			err = fmt.Errorf("nodes: sample res.StatusCode not 200,"+
				" res: %#v, url: %#v, err: %v",
				res, url, err)
		}
	} else {
		err = fmt.Errorf("nodes: sample,"+
			" res: %#v, url: %#v, err: %v",
			res, url, err)
	}

	finalNsStatsSample := NsStatsSample{
		Url:      url,
		Duration: duration,
		Error:    err,
		Data:     data,
		Start:    start,
	}

	select {
	case <-n.stopCh:
	case n.sampleCh <- finalNsStatsSample:
	}
}
