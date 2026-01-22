package store

// Subscribe adds the client channel to the list of subscribers for the given redis channel.
func (k *Kv) Subscribe(channel string, ch chan interface{}) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if _, ok := k.subs[channel]; !ok { // make map if not exists for channel
		k.subs[channel] = make(map[chan interface{}]struct{})
	}
	k.subs[channel][ch] = struct{}{} // add channel to subscribers
}

// Unsubscribe removes the client channel from the subscription list.
func (k *Kv) Unsubscribe(channel string, ch chan interface{}) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if subs, ok := k.subs[channel]; ok {
		delete(subs, ch)
		if len(subs) == 0 { // remove channel map if empty
			delete(k.subs, channel)
		}
	}
}

// Publish sends the message to all subscribers of the channel.
// Returns the number of clients that received the message.
func (k *Kv) Publish(channel string, message interface{}) int {
	k.mu.Lock()
	defer k.mu.Unlock()

	subs, ok := k.subs[channel]
	if !ok {
		return 0
	}

	count := 0
	for ch := range subs {
		select {
		case ch <- message:
			count++
		default:
		}
	}
	return count
}

// UnsubscribeAll removes the client channel from all subscriptions.
// This is expensive as it iterates all channels.
func (k *Kv) UnsubscribeAll(ch chan interface{}) {
	k.mu.Lock()
	defer k.mu.Unlock()

	for _, subs := range k.subs {
		delete(subs, ch)
	}
}

// GetSubscriptionCount returns the number of channels the client is subscribed to.
func (k *Kv) GetSubscriptionCount(ch chan interface{}) int {
	k.mu.Lock()
	defer k.mu.Unlock()

	count := 0
	for _, subs := range k.subs {
		if _, ok := subs[ch]; ok {
			count++
		}
	}
	return count
}
