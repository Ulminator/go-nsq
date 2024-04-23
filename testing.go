package nsq

import (
	"errors"
	"sync"
	"time"
)

// TestProducer implements Producer and just counts the number of messages
// It also includes an error count to test message to error scenarios
// optionally if it includes a Producer it will also send messages
type TestProducer struct {
	Counters    map[string]int32
	Producer    *Producer
	LastMessage []byte
	LastTopic   string
	Messages    [][]byte
	ErrorCount  int
	sync.Mutex
}

// Reset the counters
func (p *TestProducer) Reset() {
	p.Lock()
	p.Counters = nil
	p.LastMessage = make([]byte, 0)
	p.LastTopic = ""
	p.Messages = make([][]byte, 0)
	p.ErrorCount = 0
	p.Unlock()
}

// Count the total number of events
func (p *TestProducer) Count() int32 {
	p.Lock()
	defer p.Unlock()
	var i int32
	for _, c := range p.Counters {
		i += c
	}
	return i
}

func (p *TestProducer) Ping() error {
	return errors.New("not implemented")
}

// Publish tracks publishing to a topic
func (p *TestProducer) Publish(topic string, body []byte) error {
	p.Lock()
	defer p.Unlock()

	// modified in sequentialTaint
	if p.ErrorCount > 0 {
		p.ErrorCount--
		return errors.New("publishing message error")
	}

	if p.Counters == nil {
		p.Counters = make(map[string]int32)
	}

	if p.Producer != nil {
		// we don't ever set Producer in our code...
		err := p.Producer.Publish(topic, body)
		if err != nil {
			// don't increment on failure
			// we do have ErrorCount though...
			return err
		}
	}

	p.Counters[topic]++
	p.LastMessage = body
	p.LastTopic = topic
	p.Messages = append(p.Messages, body)
	return nil
}

func (p *TestProducer) MultiPublish(topic string, body [][]byte) error {
	// do we need to actually implement the MultiPublish method?
	// DeferredPublish was already using Publish itself
	for _, b := range body {
		err := p.Publish(topic, b)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeferredPublish is a wrapper on Publish that ignores the delay
func (p *TestProducer) DeferredPublish(topic string, delay time.Duration, body []byte) error {
	return p.Publish(topic, body)
}

// PublishAsync is a wrapper on Publish
func (p *TestProducer) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction, args ...interface{}) error {
	err := p.Publish(topic, body)
	if err != nil {
		return err
	}
	doneChan <- &ProducerTransaction{Args: args}
	return nil
}

func (p *TestProducer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction, args ...interface{}) error {
	return errors.New("not implemented")
}

func (p *TestProducer) DeferredPublishAsync(topic string, delay time.Duration, body []byte, doneChan chan *ProducerTransaction, args ...interface{}) error {
	return errors.New("not implemented")
}

func (p *TestProducer) String() string {
	if p.Producer != nil {
		return p.Producer.String()
	}
	return "TestProducer"
}

// Implementation of Stop for TestProducer
func (p *TestProducer) Stop() {
	if p.Producer != nil {
		p.Producer.Stop()
	}
}
