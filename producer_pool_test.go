package nsq

import (
	"testing"
)

type taintFunc func(sink []*TestProducer, errorRate float32, multiplier int)

// sequentialTaint marks producers in the TestProducer pool for controlled error.
// It does this sequentially starting at the first node in the pool at the 0th position.
// A multiplier greater than 1 can be used to force a producer to fail multiple times.
func sequentialTaint(sink []*TestProducer, errorRate float32, multiplier int) {
	sinkSize := len(sink)
	totalErrors := int(float32(sinkSize) * errorRate)
	for i := 0; i < sinkSize; i++ {
		if totalErrors == 0 {
			break
		}
		sink[i].ErrorCount = 1 * multiplier
		totalErrors--
	}
}

func assertCount(t *testing.T, actual int32, expected int32) {
	if actual != expected {
		t.Fatalf("expected %d, got %d", expected, actual)
	}
}

func TestProducerPool_NewProducerPool(t *testing.T) {
	// Given a list of N addresses
	hostA, hostB, hostC := "127.0.0.1:4150", "127.0.0.1:4151", "127.0.0.1:4152"
	addrs := []string{hostA, hostB, hostC}

	// When creating a new producer pool
	p, err := NewProducerPool(addrs, NewConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	// Then the producer pool should have N producers
	if len(p.Producers) != len(addrs) {
		t.Fatalf("expected %d producers, got %d", len(addrs), len(p.Producers))
	}

	// And the producer pool should have the correct addresses
	expectedString := "[" + p.Producers[0].String() + ", " + p.Producers[1].String() + ", " + p.Producers[2].String() + "]"
	if p.String() != expectedString {
		t.Fatalf("expected %s, got %s", expectedString, p.String())
	}
}

// TODO: test retry functionality by tainting producers?
// TODO: fail MaxAttempts times and ensure the transaction fails
// TODO: MaxAttempts > len(producers) should only try len(producers) times

func TestProducerPool_Publish(t *testing.T) {
	// Given a producer pool with N producers
	sink := make([]*TestProducer, 3)
	p := &ProducerPool{}
	for i := range sink {
		sink[i] = &TestProducer{}
		p.Producers = append(p.Producers, sink[i])
	}

	// When publishing N+1 messages
	messageCount := len(p.Producers) + 1
	for i := 0; i < messageCount; i++ {
		err := p.Publish("topic", []byte("body"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Then the first producer should publish 2 messages
	// And the rest of the producers should publish 1 message
	assertCount(t, sink[0].Count(), 2)
	for i := 1; i < len(p.Producers); i++ {
		assertCount(t, sink[i].Count(), 1)
	}
}

func TestProducerPool_MultiPublish(t *testing.T) {
	// Given a producer pool with N producers
	sink := make([]*TestProducer, 3)
	p := &ProducerPool{}
	for i := range sink {
		sink[i] = &TestProducer{}
		p.Producers = append(p.Producers, sink[i])
	}

	// When multi-publishing a set of messages
	var chunk [][]byte
	for i := 0; i < 10; i++ {
		chunk = append(chunk, []byte("body"))
	}
	err := p.MultiPublish("topic", chunk)
	if err != nil {
		t.Fatal(err)
	}

	// Then the first producer should publish them all
	// And the rest of the producers should not publish anything
	assertCount(t, sink[0].Count(), 10)
	for i := 1; i < len(p.Producers); i++ {
		assertCount(t, sink[i].Count(), 0)
	}
}

func TestProducerPool_DeferredPublish(t *testing.T) {
	// Given a producer pool with N producers
	sink := make([]*TestProducer, 3)
	p := &ProducerPool{}
	for i := range sink {
		sink[i] = &TestProducer{}
		p.Producers = append(p.Producers, sink[i])
	}

	// When publishing N+1 messages
	messageCount := len(p.Producers) + 1
	for i := 0; i < messageCount; i++ {
		err := p.DeferredPublish("topic", 0, []byte("body"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Then the first producer should publish 2 messages
	// And the rest of the producers should publish 1 message
	assertCount(t, sink[0].Count(), 2)
	for i := 1; i < len(p.Producers); i++ {
		assertCount(t, sink[i].Count(), 1)
	}
}

// paramify the MaxAttempts?
func TestProducerPool_PublishAsync(t *testing.T) {

	tests := []struct {
		description      string
		sinkSize         int
		errorRate        float32
		taint            taintFunc
		taintMultiplier  int
		transactionError bool
	}{
		{
			description:      "0% producer error rate",
			sinkSize:         10,
			errorRate:        0.0,
			transactionError: false,
		},
		{
			description:      "20% producer error rate",
			sinkSize:         10,
			errorRate:        .2,
			taint:            sequentialTaint,
			taintMultiplier:  1,
			transactionError: false,
		},
		{
			description:      "30% producer error rate",
			sinkSize:         10,
			errorRate:        .3,
			taint:            sequentialTaint,
			taintMultiplier:  1,
			transactionError: true,
		},
		{
			description:      "50% producer error rate",
			sinkSize:         10,
			errorRate:        .5,
			taint:            sequentialTaint,
			taintMultiplier:  1,
			transactionError: true,
		},
		{
			description:      "100% producer error rate",
			sinkSize:         10,
			errorRate:        1.00,
			taint:            sequentialTaint,
			taintMultiplier:  1,
			transactionError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(tt *testing.T) {
			sink := make([]*TestProducer, tc.sinkSize)
			p := &ProducerPool{}
			p.MaxAttempts = 3

			for i := range sink {
				sink[i] = &TestProducer{}
				p.Producers = append(p.Producers, sink[i])
			}

			if tc.taint != nil {
				tc.taint(sink, tc.errorRate, 1)
			}

			doneChan := make(chan *ProducerTransaction)
			p.PublishAsync("topic", []byte("body"), doneChan)

			transaction := <-doneChan

			// If 3 consecutive hosts return errors, then the transaction result will be an error.
			// This is due to the max number of sequential attempts for a producer pool being capped at 3.
			// So given a pool size of 10, a 30% failure rate of the hosts will result in a transaction failure.
			if tc.transactionError == true {
				if transaction.Error == nil {
					tt.Fatalf("expected error, got nil")
				}
			} else {
				if transaction.Error != nil {
					tt.Fatalf("expected nil, got %v", transaction.Error)
				}
			}
		})
	}
}

func TestProducerPool_MultiPublishAsync(t *testing.T) {
	t.Skip("skipping test")
}

func TestProducerPool_DeferredPublishAsync(t *testing.T) {
	t.Skip("skipping test")
}
