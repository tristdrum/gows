package meowcaller

// HasRelayAllocation reports whether this current call holds provider relay data.
// It does not expose that data or imply an established media path.
func (c *Call) HasRelayAllocation() bool {
	// Source of truth: https://github.com/tristdrum/gows/blob/12c2bbb57238abcc2eb1da90d66ca17d5c87f2e3/third_party/meowcaller/engine.go#L790-L818
	if c == nil || c.eng == nil {
		return false
	}
	c.eng.mu.Lock()
	defer c.eng.mu.Unlock()
	m := c.eng.calls[c.id]
	return m != nil && m.call == c && m.relay != nil
}
