package meowcaller

import (
	"sync"
	"testing"
)

func TestRelayAllocationObservationIsBoundToCurrentHandle(t *testing.T) {
	// Source of truth: https://github.com/tristdrum/gows/blob/12c2bbb57238abcc2eb1da90d66ca17d5c87f2e3/third_party/meowcaller/engine.go#L790-L818
	eng, _, _, _ := offerReceiptFixture()
	call := &Call{eng: eng, id: "CALL_ID", phase: CallPhaseRinging}
	eng.calls[call.id] = &engineCall{call: call, direction: CallDirectionIncoming}
	if call.HasRelayAllocation() {
		t.Fatal("unknown allocation reported ready")
	}
	eng.calls[call.id].relay = &relayData{}
	if !call.HasRelayAllocation() || call.State() != CallPhaseRinging || eng.calls[call.id].started {
		t.Fatal("observation altered state or media")
	}
	replacement := &Call{eng: eng, id: call.id, phase: CallPhaseRinging}
	eng.calls[call.id] = &engineCall{call: replacement, relay: &relayData{}}
	if call.HasRelayAllocation() || !replacement.HasRelayAllocation() {
		t.Fatal("retired handle observed replacement allocation")
	}
	eng.finishCall(replacement.id, "retired")
	if replacement.HasRelayAllocation() {
		t.Fatal("retired allocation remained visible")
	}
	var absent *Call
	if absent.HasRelayAllocation() || (&Call{}).HasRelayAllocation() {
		t.Fatal("unbound handle reported allocation")
	}
}

func TestRelayAllocationObservationConcurrentWithUpdates(t *testing.T) {
	// Source of truth: https://github.com/tristdrum/gows/blob/12c2bbb57238abcc2eb1da90d66ca17d5c87f2e3/third_party/meowcaller/engine.go#L790-L818
	eng, _, _, _ := offerReceiptFixture()
	call := &Call{eng: eng, id: "CALL_ID", phase: CallPhaseRinging}
	m := &engineCall{call: call}
	eng.calls[call.id] = m
	var work sync.WaitGroup
	work.Add(2)
	go func() {
		defer work.Done()
		for range 100 {
			eng.mu.Lock()
			m.relay = &relayData{}
			eng.mu.Unlock()
		}
	}()
	go func() {
		defer work.Done()
		for range 100 {
			_ = call.HasRelayAllocation()
		}
	}()
	work.Wait()
}
