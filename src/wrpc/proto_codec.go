package wrpc

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	protoencoding "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// protoCodecWithPool is a copy of gRPC's default proto codec, but it injects a
// custom buffer pool instead of using mem.DefaultBufferPool(). This keeps large
// marshaled responses from being retained in the default uncapped pool.
type protoCodecWithPool struct {
	pool mem.BufferPool
}

var _ encoding.CodecV2 = (*protoCodecWithPool)(nil)

// NewProtoCodec builds a proto codec that routes marshaling/unmarshaling
// buffers through the provided pool.
func NewProtoCodec(pool mem.BufferPool) encoding.CodecV2 {
	if pool == nil {
		// Fall back to no pooling rather than the default uncapped pool.
		pool = mem.NopBufferPool{}
	}
	return &protoCodecWithPool{pool: pool}
}

func (c *protoCodecWithPool) Marshal(v any) (mem.BufferSlice, error) {
	vv := messageV2Of(v)
	if vv == nil {
		return nil, fmt.Errorf("proto: failed to marshal, message is %T, want proto.Message", v)
	}

	// Important: if we remove this Size call then we cannot use UseCachedSize in
	// MarshalOptions below. This mirrors grpc/encoding/proto.
	size := gproto.Size(vv)
	marshalOptions := gproto.MarshalOptions{UseCachedSize: true}

	var data mem.BufferSlice
	if mem.IsBelowBufferPoolingThreshold(size) {
		buf, err := marshalOptions.Marshal(vv)
		if err != nil {
			return nil, err
		}
		data = append(data, mem.SliceBuffer(buf))
		return data, nil
	}

	buf := c.pool.Get(size)
	if _, err := marshalOptions.MarshalAppend((*buf)[:0], vv); err != nil {
		c.pool.Put(buf)
		return nil, err
	}
	data = append(data, mem.NewBuffer(buf, c.pool))
	return data, nil
}

func (c *protoCodecWithPool) Unmarshal(data mem.BufferSlice, v any) error {
	vv := messageV2Of(v)
	if vv == nil {
		return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
	}

	buf := data.MaterializeToBuffer(c.pool)
	defer buf.Free()
	return gproto.Unmarshal(buf.ReadOnlyData(), vv)
}

func (c *protoCodecWithPool) Name() string {
	return protoencoding.Name
}

// messageV2Of mirrors the helper from grpc/encoding/proto.
func messageV2Of(v any) gproto.Message {
	switch v := v.(type) {
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(v)
	case protoadapt.MessageV2:
		return v
	}
	return nil
}
