package pubsub_datasource

import (
	"encoding/json"
	"fmt"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
	"slices"
)

type RabbitMQSubscriptionEventConfiguration struct {
	ProviderID string   `json:"providerId"`
	Queues     []string `json:"queues"`
}

type RabbitMQPublishEventConfiguration struct {
	ProviderID string          `json:"providerId"`
	Queue      string          `json:"queue"`
	Data       json.RawMessage `json:"data"`
}

func (s *RabbitMQPublishEventConfiguration) MarshalJSONTemplate() string {
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
	if len(p.eventConfiguration.Queues) == 0 {
		p.visitor.Walker.StopWithInternalErr(fmt.Errorf("expected at least one subscription queue but received %d", len(p.eventConfiguration.Queues)))
		return
	}

	slices.Sort(p.eventConfiguration.Queues)

	p.subscriptionEventConfiguration = &RabbitMQSubscriptionEventConfiguration{
		ProviderID: p.eventMetadata.ProviderID,
		Queues:     p.eventConfiguration.Queues,
	}
}
