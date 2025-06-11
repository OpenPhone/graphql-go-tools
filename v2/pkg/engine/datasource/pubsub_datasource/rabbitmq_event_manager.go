package pubsub_datasource

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
)

type RabbitMQSubscriptionEventConfiguration struct {
	ProviderID string   `json:"providerId"`
	Queues     []string `json:"queues"`
	Exchange   string   `json:"exchange,omitempty"`
	RoutingKey string   `json:"routingKey,omitempty"`
}

type RabbitMQPublishEventConfiguration struct {
	ProviderID string          `json:"providerId"`
	Queue      string          `json:"queue"`
	Data       json.RawMessage `json:"data"`
	Exchange   string          `json:"exchange,omitempty"`
	RoutingKey string          `json:"routingKey,omitempty"`
}

func (s *RabbitMQPublishEventConfiguration) MarshalJSONTemplate() string {
	if s.Exchange != "" && s.RoutingKey != "" {
		return fmt.Sprintf(`{"queue":"%s", "data": %s, "providerId":"%s", "exchange":"%s", "routingKey":"%s"}`, s.Queue, s.Data, s.ProviderID, s.Exchange, s.RoutingKey)
	}
	return fmt.Sprintf(`{"queue":"%s", "data": %s, "providerId":"%s"}`, s.Queue, s.Data, s.ProviderID)
}

type RabbitMQEventManager struct {
	visitor                        *plan.Visitor
	variables                      *resolve.Variables
	eventMetadata                  EventMetadata
	eventConfiguration             *RabbitMQEventConfiguration
	publishEventConfiguration      *RabbitMQPublishEventConfiguration
	subscriptionEventConfiguration *RabbitMQSubscriptionEventConfiguration
}

func (p *RabbitMQEventManager) eventDataBytes(ref int) ([]byte, error) {
	return buildEventDataBytes(ref, p.visitor, p.variables)
}

func (p *RabbitMQEventManager) handlePublishEvent(ref int) {
	if len(p.eventConfiguration.Queues) != 1 {
		p.visitor.Walker.StopWithInternalErr(fmt.Errorf("publish events should define one queue but received %d", len(p.eventConfiguration.Queues)))
		return
	}
	queue := p.eventConfiguration.Queues[0]
	dataBytes, err := p.eventDataBytes(ref)
	if err != nil {
		p.visitor.Walker.StopWithInternalErr(fmt.Errorf("failed to write event data bytes: %w", err))
		return
	}

	p.publishEventConfiguration = &RabbitMQPublishEventConfiguration{
		ProviderID: p.eventMetadata.ProviderID,
		Queue:      queue,
		Data:       dataBytes,
	}
}

func (p *RabbitMQEventManager) handleSubscriptionEvent(ref int) {
	if len(p.eventConfiguration.Queues) == 0 || p.eventConfiguration.Exchange == "" {
		p.visitor.Walker.StopWithInternalErr(fmt.Errorf("expected at least one subscription queue or an exchange but received none"))
		return
	}

	slices.Sort(p.eventConfiguration.Queues)

	p.subscriptionEventConfiguration = &RabbitMQSubscriptionEventConfiguration{
		ProviderID: p.eventMetadata.ProviderID,
		Queues:     p.eventConfiguration.Queues,
		Exchange:   p.eventConfiguration.Exchange,
		RoutingKey: p.eventConfiguration.RoutingKey,
	}
}
