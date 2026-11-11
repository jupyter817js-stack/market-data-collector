package lsports

import (
	"context"
	"encoding/json"
	"fmt"
)

// Distribution start/stop endpoints toggle whether LSPORTS publishes
// messages to our package's RabbitMQ queue. Without an active distributor:
//   - AMQP connect returns 403 ("username or password not allowed")
//   - REST Snapshot endpoints return successful HTTP 200 but Body:[]
//   - /Fixtures/Subscribe accepts the call but the fixture does not show
//     up in the package's in-play data feed
//
// This was the root cause of the 2026-05-28 first-match silent failure
// (see memory/first_measurement_target.md). Per LSPORTS Support:
// "make sure that the distributor is activated before you are trying to
// connect the consumer".
//
// docs: https://docs.lsports.eu/u/trade/integration/rmq-connection/connection-and-consumption-setup

// DistributionResponse is the raw body LSPORTS returns. Success returns
// a string like "Value was already set" when the distributor was already
// on, or similar phrases when newly activated.
type DistributionResponse struct {
	Raw  []byte
	Body string
}

// StartDistribution calls POST /Distribution/Start. Must be invoked before
// connecting any AMQP consumer. Idempotent — calling Start when already
// on returns "Value was already set" rather than an error.
func (c *Client) StartDistribution(ctx context.Context) (DistributionResponse, error) {
	return c.distributionCall(ctx, "/Distribution/Start")
}

// StopDistribution calls POST /Distribution/Stop. Operator-only — once a
// fixture is subscribed and the distributor is on, leave it running for
// the duration of the trial.
func (c *Client) StopDistribution(ctx context.Context) (DistributionResponse, error) {
	return c.distributionCall(ctx, "/Distribution/Stop")
}

func (c *Client) distributionCall(ctx context.Context, path string) (DistributionResponse, error) {
	raw, _, err := c.Post(ctx, path, nil)
	if err != nil {
		return DistributionResponse{Raw: raw}, err
	}
	resp := DistributionResponse{Raw: raw}
	// Observed shapes:
	//   {"Header":{"HttpStatusCode":200}, "Body":{"Message":"Distribution is: Active"}}
	//   {"Header":{...}, "Body":"Value was already set"}
	//   bare quoted "Value was already set"
	var envMsg struct {
		Body struct {
			Message string `json:"Message"`
		} `json:"Body"`
	}
	if jerr := json.Unmarshal(raw, &envMsg); jerr == nil && envMsg.Body.Message != "" {
		resp.Body = envMsg.Body.Message
		return resp, nil
	}
	var envStr struct {
		Body string `json:"Body"`
	}
	if jerr := json.Unmarshal(raw, &envStr); jerr == nil && envStr.Body != "" {
		resp.Body = envStr.Body
		return resp, nil
	}
	var bare string
	if jerr := json.Unmarshal(raw, &bare); jerr == nil {
		resp.Body = bare
		return resp, nil
	}
	resp.Body = string(raw)
	return resp, fmt.Errorf("lsports: %s: could not parse response body (treat raw text as result)", path)
}
