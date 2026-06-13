package helpers

import "go.mau.fi/whatsmeow/types"

// ParticipantsMap holds a group's participant data in two structures:
//  1. A map from JID -> pointer to participant info for fast lookups/updates
//  2. Two slices to preserve order:
//     - originalOrder: The order of participants as loaded initially
//     - newlyJoined:   The order of participants who joined during an update
type ParticipantsMap struct {
	participants  map[types.JID]*types.GroupParticipant
	originalOrder []types.JID
	newlyJoined   []types.JID
}

// NewParticipantsMap initializes a ParticipantsMap from an existing slice of participants.
func NewParticipantsMap(original []types.GroupParticipant) *ParticipantsMap {
	pm := &ParticipantsMap{
		participants:  make(map[types.JID]*types.GroupParticipant, len(original)),
		originalOrder: make([]types.JID, 0, len(original)),
		newlyJoined:   []types.JID{},
	}

	// Populate map and record the original order
	for i := range original {
		p := &original[i]
		pm.participants[p.JID] = p
		pm.originalOrder = append(pm.originalOrder, p.JID)
	}
	return pm
}

// Join adds any missing JIDs as participants with default (false) Admin / SuperAdmin flags.
func (pm *ParticipantsMap) Join(jids []types.JID) {
	for _, jid := range jids {
		if _, exists := pm.participants[jid]; !exists {
			pm.participants[jid] = &types.GroupParticipant{
				JID:          jid,
				IsAdmin:      false,
				IsSuperAdmin: false,
			}
			// Keep track of the newly joined order
			pm.newlyJoined = append(pm.newlyJoined, jid)
		}
	}
}

// Leave removes the given JIDs from the participants map if they exist.
func (pm *ParticipantsMap) Leave(jids []types.JID) {
	for _, jid := range jids {
		if _, exists := pm.participants[jid]; exists {
			delete(pm.participants, jid)
		}
	}
}

// Promote applies "isAdmin = true, isSuperAdmin = false" to each matching participant.
func (pm *ParticipantsMap) Promote(jids []types.JID) {
	for _, jid := range jids {
		if p, exists := pm.participants[jid]; exists {
			p.IsAdmin = true
			p.IsSuperAdmin = false
		}
	}
}

// Demote sets both admin/super-admin flags to false for each matching participant.
func (pm *ParticipantsMap) Demote(jids []types.JID) {
	for _, jid := range jids {
		if p, exists := pm.participants[jid]; exists {
			p.IsAdmin = false
			p.IsSuperAdmin = false
		}
	}
}

// Participants rebuilds and returns the final ordered list of participants.
func (pm *ParticipantsMap) Participants() []types.GroupParticipant {
	// We'll reconstruct the final list in two parts:
	//  1) Original participants who still exist
	//  2) Newly joined participants who still exist
	finalList := make([]types.GroupParticipant, 0, len(pm.participants))

	// 1) Original order first (for those who remain in the map)
	for _, jid := range pm.originalOrder {
		if p, exists := pm.participants[jid]; exists {
			finalList = append(finalList, *p)
		}
	}

	// 2) Append newly joined in the order they appeared
	for _, jid := range pm.newlyJoined {
		if p, exists := pm.participants[jid]; exists {
			finalList = append(finalList, *p)
		}
	}

	return finalList
}
