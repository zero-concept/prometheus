// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package generic

import (
	"log"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/prometheus/common/model"
)

// Client allows sending batches of Prometheus samples to an HTTP endpoint.
type Client struct {
	client  GenericWriteClient
	timeout time.Duration
}

// NewClient creates a new Client.
func NewClient(address string, timeout time.Duration) *Client {
	// TODO: Do something with this error.
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithTimeout(timeout))
	if err != nil {
		// grpc.Dial() returns immediately and doesn't error when the server is
		// unreachable when not passing in the WithBlock() option. The client then
		// will continuously try to (re)establish the connection in the background.
		// So it seems ok to die here on startup if there is a different kind of error
		// returned.
		log.Fatalln("Error creating gRPC client connection:", err)
	}
	return &Client{
		timeout: timeout,
		client:  NewGenericWriteClient(conn),
	}
}

// Store sends a batch of samples to the HTTP endpoint.
func (c *Client) Store(samples model.Samples) error {
	req := &GenericWriteRequest{
		Timeseries: make([]*TimeSeries, 0, len(samples)),
	}
	for _, s := range samples {
		ts := &TimeSeries{}
		for k, v := range s.Metric {
			ts.Labels = append(ts.Labels,
				&LabelPair{
					Name:  string(k),
					Value: string(v),
				})
		}
		ts.Samples = []*Sample{
			&Sample{
				Value:       float64(s.Value),
				TimestampMs: int64(s.Timestamp),
			},
		}
		req.Timeseries = append(req.Timeseries, ts)
	}
	ctxt, _ := context.WithTimeout(context.Background(), c.timeout)
	_, err := c.client.Write(ctxt, req)
	if err != nil {
		return err
	}
	return nil
}

// Name identifies the client as a generic client.
func (c Client) Name() string {
	return "generic"
}
