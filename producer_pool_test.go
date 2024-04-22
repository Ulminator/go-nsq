package nsq

import (
	"testing"
)

func TestProducerPoolPublish(t *testing.T) {
	sink := make([]*TestProducer, 5)
	p := &ProducerPool{}
	for i := range sink {
		sink[i] = &TestProducer{}
		p.Producers = append(p.Producers, sink[i])
	}
	for i := 0; i < 6; i++ {
		err := p.Publish("topic", []byte("body"))
		if err != nil {
			t.Fatal(err)
		}
	}
	if sink[0].Count() != 2 {
		t.Fatal("expected 2")
	}
	for i := 1; i < 5; i++ {
		if sink[i].Count() != 1 {
			t.Fatal("expected 1")
		}
	}
}

func TestProducerPoolMultiPublish(t *testing.T) {
	t.Skip("skipping test")
}

func TestProducerPoolDeferredPublish(t *testing.T) {
	t.Skip("skipping test")
}

func TestProducerPoolPublishAsync(t *testing.T) {
	t.Skip("skipping test")
}

func TestProducerPoolMultiPublishAsync(t *testing.T) {
	t.Skip("skipping test")
}

func TestProducerPoolDeferredPublishAsync(t *testing.T) {
	t.Skip("skipping test")
}
