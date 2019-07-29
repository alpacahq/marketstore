package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/alpacahq/slait/rest"
)

type SlaitClient struct {
	Endpoint string
}

// used for setting up the structure
func (sc *SlaitClient) PostTopic(tr rest.TopicsRequest) (err error) {
	data, err := json.Marshal(tr)
	if err != nil {
		return err
	}
	_, err = sc.request("POST", sc.Endpoint+"/topics", data)
	return err
}

// used for delivering the data
func (sc *SlaitClient) PutPartition(topic, partition string, data []byte) error {
	_, err := sc.request(
		"PUT",
		fmt.Sprintf("%v/topics/%v/%v", sc.Endpoint, topic, partition),
		data,
	)
	return err
}

// delete a partition
func (sc *SlaitClient) DeletePartition(topic, partition string) error {
	_, err := sc.request(
		"DELETE",
		fmt.Sprintf("%v/topics/%v/%v", sc.Endpoint, topic, partition),
		nil,
	)
	return err
}

func (sc *SlaitClient) GetPartition(topic, partition string, from, to *time.Time, last int) (*rest.PartitionRequestResponse, error) {
	q := "&"
	if from != nil && !from.IsZero() {
		q = fmt.Sprintf("%v&%v=%v", q, "since", from.Format(time.RFC3339))
	}
	if to != nil && !to.IsZero() {
		q = fmt.Sprintf("%v&%v=%v", q, "to", to.Format(time.RFC3339))
	}
	if last > 0 {
		q = fmt.Sprintf("%v&%v=%v", q, "last", last)
	}
	data, err := sc.request(
		"GET",
		fmt.Sprintf("%v/topics/%v/%v?%v", sc.Endpoint, topic, partition, q),
		nil)
	if err != nil {
		return nil, err
	}
	resp := rest.PartitionRequestResponse{}
	if err = json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, err
}

func (sc *SlaitClient) request(method, url string, data []byte) ([]byte, error) {
	req, err := http.NewRequest(method, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf(
			"Slait request failed - URL: %v - Code: %v - Response: %v",
			url,
			resp.StatusCode,
			body,
		)
	}
	return body, err
}
