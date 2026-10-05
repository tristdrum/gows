package server

import (
	"github.com/devlikeapro/gows/calling"
	pb "github.com/devlikeapro/gows/proto"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/grpc"
)

type mediaReturnKind string

const (
	mediaOwnedDone        mediaReturnKind = "owned_done"
	mediaClientEOF        mediaReturnKind = "client_eof"
	mediaContextCancelled mediaReturnKind = "context_cancelled"
	mediaReceiveError     mediaReturnKind = "receive_error"
	mediaSendError        mediaReturnKind = "send_error"
	mediaSinkError        mediaReturnKind = "sink_error"
)

type mediaRelayResult struct {
	kind mediaReturnKind
	err  error
}

type callMediaEndpoint struct {
	input         <-chan []byte
	done          <-chan struct{}
	write         func([]byte) error
	clear         func()
	terminalError func() error
	close         func() error
}

func serveOwnedCallMedia(stream grpc.BidiStreamingServer[pb.MediaPacket, pb.MediaPacket], first *pb.MediaPacket, media callMediaEndpoint) error {
	result := mediaRelayResult{kind: mediaSendError}
	defer media.close()
	// This observation precedes Close, which can itself initiate native hangup.
	// It names the selected media return branch, not the first network initiator.
	defer func() {
		waLog.Stdout("NativeVoice", "INFO", false).Infof("native_media_terminal binding_hash=%s return_kind=%s", calling.NativeBindingHash(first.GetSession(), first.GetCallId()), result.kind)
	}()
	if err := stream.Send(&pb.MediaPacket{CallId: first.GetCallId(), Kind: "ready"}); err != nil {
		return err
	}
	result = relayCallMediaResult(stream, first, media.input, media.done, media.write, media.clear, media.terminalError)
	return result.err
}
