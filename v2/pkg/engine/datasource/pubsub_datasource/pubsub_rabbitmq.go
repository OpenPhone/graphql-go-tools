package pubsub_datasource

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/buger/jsonparser"
	"github.com/cespare/xxhash/v2"

	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/datasource/httpclient"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
)

type RabbitMQEventConfiguration struct {
	Queues     []string `json:"queues"`
	Exchange   string   `json:"exchange"`
	RoutingKey string   `json:"routingKey"`
}

type RabbitMQConnector interface {
	New(ctx context.Context) RabbitMQPubSub
}

// RabbitMQPubSub is the interface for RabbitMQ pubsub operations
type RabbitMQPubSub interface {
	// Subscribe subscribes to the given queues and updates the subscription updater
	Subscribe(ctx context.Context, config RabbitMQSubscriptionEventConfiguration, updater resolve.SubscriptionUpdater) error
	// Publish publishes the given event to the RabbitMQ queue
	Publish(ctx context.Context, config RabbitMQPublishEventConfiguration) error
}

type RabbitMQSubscriptionSource struct {
	pubSub RabbitMQPubSub
}

func (s *RabbitMQSubscriptionSource) UniqueRequestID(ctx *resolve.Context, input []byte, xxh *xxhash.Digest) error {

	val, _, _, err := jsonparser.Get(input, "queues")
	if err != nil {
		return err
	}

	_, err = xxh.Write(val)
	if err != nil {
		return err
	}

	val, _, _, err = jsonparser.Get(input, "providerId")
	if err != nil {
		return err
	}

	_, err = xxh.Write(val)
	return err
}

func (s *RabbitMQSubscriptionSource) Start(ctx *resolve.Context, input []byte, updater resolve.SubscriptionUpdater) error {
	var subscriptionConfiguration RabbitMQSubscriptionEventConfiguration
	err := json.Unmarshal(input, &subscriptionConfiguration)
	if err != nil {
		return err
	}

	return s.pubSub.Subscribe(ctx.Context(), subscriptionConfiguration, updater)
}

type RabbitMQPublishDataSource struct {
	pubSub RabbitMQPubSub
}

func (s *RabbitMQPublishDataSource) Load(ctx context.Context, input []byte, out *bytes.Buffer) error {
	var publishConfiguration RabbitMQPublishEventConfiguration
	err := json.Unmarshal(input, &publishConfiguration)
	if err != nil {
		return err
	}

	if err := s.pubSub.Publish(ctx, publishConfiguration); err != nil {
		_, err = io.WriteString(out, `{"success": false}`)
		return err
	}
	_, err = io.WriteString(out, `{"success": true}`)
	return err
}

func (s *RabbitMQPublishDataSource) LoadWithFiles(ctx context.Context, input []byte, files []*httpclient.FileUpload, out *bytes.Buffer) (err error) {
	panic("not implemented")
}
