package cache

import "github.com/eapache/channels"

type Publication struct {
	Topic     string
	Partition string
	Entries   Entries
}

type Router struct {
	pub    *channels.InfiniteChannel
	remove chan *Publication
	add    chan *Publication
}

func (r *Router) Publish(topic, partition string, entries Entries) {
	r.pub.In() <- &Publication{
		Topic:     topic,
		Partition: partition,
		Entries:   entries,
	}
}

func (r *Router) Remove(topic string) {
	r.remove <- &Publication{
		Topic: topic,
	}
}

func (r *Router) Add(topic string) {
	r.add <- &Publication{
		Topic: topic,
	}
}

func (r *Router) Update(topic, partition string, action int) error {
	p := &Publication{
		Topic:     topic,
		Partition: partition,
	}
	switch action {
	case AddPartition:
		r.add <- p
	case RemovePartition:
		r.remove <- p
	case ClearPartition:
		fallthrough
	default:
		return nil
	}
	return nil
}

func Pull() <-chan interface{} {
	return masterCache.router.pub.Out()
}

func PullRemovals() <-chan *Publication {
	return masterCache.router.remove
}

func PullAdditions() <-chan *Publication {
	return masterCache.router.add
}
