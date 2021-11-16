// Code generated by protoc-gen-go. DO NOT EDIT.
// source: types.proto

package pb

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type BlockMsg struct {
	Id                   int64       `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Vnf                  int32       `protobuf:"varint,2,opt,name=vnf,proto3" json:"vnf,omitempty"`
	Ar                   int32       `protobuf:"varint,3,opt,name=ar,proto3" json:"ar,omitempty"`
	SnID                 int32       `protobuf:"varint,4,opt,name=snID,proto3" json:"snID,omitempty"`
	Shards               []*ShardMsg `protobuf:"bytes,5,rep,name=shards,proto3" json:"shards,omitempty"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

func (m *BlockMsg) Reset()         { *m = BlockMsg{} }
func (m *BlockMsg) String() string { return proto.CompactTextString(m) }
func (*BlockMsg) ProtoMessage()    {}
func (*BlockMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_d938547f84707355, []int{0}
}

func (m *BlockMsg) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BlockMsg.Unmarshal(m, b)
}
func (m *BlockMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BlockMsg.Marshal(b, m, deterministic)
}
func (m *BlockMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BlockMsg.Merge(m, src)
}
func (m *BlockMsg) XXX_Size() int {
	return xxx_messageInfo_BlockMsg.Size(m)
}
func (m *BlockMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_BlockMsg.DiscardUnknown(m)
}

var xxx_messageInfo_BlockMsg proto.InternalMessageInfo

func (m *BlockMsg) GetId() int64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *BlockMsg) GetVnf() int32 {
	if m != nil {
		return m.Vnf
	}
	return 0
}

func (m *BlockMsg) GetAr() int32 {
	if m != nil {
		return m.Ar
	}
	return 0
}

func (m *BlockMsg) GetSnID() int32 {
	if m != nil {
		return m.SnID
	}
	return 0
}

func (m *BlockMsg) GetShards() []*ShardMsg {
	if m != nil {
		return m.Shards
	}
	return nil
}

type ShardMsg struct {
	Id                   int64    `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	NodeID               int32    `protobuf:"varint,2,opt,name=nodeID,proto3" json:"nodeID,omitempty"`
	Vhf                  []byte   `protobuf:"bytes,3,opt,name=vhf,proto3" json:"vhf,omitempty"`
	BlockID              int64    `protobuf:"varint,4,opt,name=blockID,proto3" json:"blockID,omitempty"`
	NodeID2              int32    `protobuf:"varint,5,opt,name=nodeID2,proto3" json:"nodeID2,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ShardMsg) Reset()         { *m = ShardMsg{} }
func (m *ShardMsg) String() string { return proto.CompactTextString(m) }
func (*ShardMsg) ProtoMessage()    {}
func (*ShardMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_d938547f84707355, []int{1}
}

func (m *ShardMsg) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ShardMsg.Unmarshal(m, b)
}
func (m *ShardMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ShardMsg.Marshal(b, m, deterministic)
}
func (m *ShardMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ShardMsg.Merge(m, src)
}
func (m *ShardMsg) XXX_Size() int {
	return xxx_messageInfo_ShardMsg.Size(m)
}
func (m *ShardMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_ShardMsg.DiscardUnknown(m)
}

var xxx_messageInfo_ShardMsg proto.InternalMessageInfo

func (m *ShardMsg) GetId() int64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *ShardMsg) GetNodeID() int32 {
	if m != nil {
		return m.NodeID
	}
	return 0
}

func (m *ShardMsg) GetVhf() []byte {
	if m != nil {
		return m.Vhf
	}
	return nil
}

func (m *ShardMsg) GetBlockID() int64 {
	if m != nil {
		return m.BlockID
	}
	return 0
}

func (m *ShardMsg) GetNodeID2() int32 {
	if m != nil {
		return m.NodeID2
	}
	return 0
}

type CheckPointMsg struct {
	Id                   int32    `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Start                int64    `protobuf:"varint,2,opt,name=start,proto3" json:"start,omitempty"`
	Timestamp            int64    `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CheckPointMsg) Reset()         { *m = CheckPointMsg{} }
func (m *CheckPointMsg) String() string { return proto.CompactTextString(m) }
func (*CheckPointMsg) ProtoMessage()    {}
func (*CheckPointMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_d938547f84707355, []int{2}
}

func (m *CheckPointMsg) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CheckPointMsg.Unmarshal(m, b)
}
func (m *CheckPointMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CheckPointMsg.Marshal(b, m, deterministic)
}
func (m *CheckPointMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CheckPointMsg.Merge(m, src)
}
func (m *CheckPointMsg) XXX_Size() int {
	return xxx_messageInfo_CheckPointMsg.Size(m)
}
func (m *CheckPointMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_CheckPointMsg.DiscardUnknown(m)
}

var xxx_messageInfo_CheckPointMsg proto.InternalMessageInfo

func (m *CheckPointMsg) GetId() int32 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *CheckPointMsg) GetStart() int64 {
	if m != nil {
		return m.Start
	}
	return 0
}

func (m *CheckPointMsg) GetTimestamp() int64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

func init() {
	proto.RegisterType((*BlockMsg)(nil), "pb.BlockMsg")
	proto.RegisterType((*ShardMsg)(nil), "pb.ShardMsg")
	proto.RegisterType((*CheckPointMsg)(nil), "pb.CheckPointMsg")
}

func init() { proto.RegisterFile("types.proto", fileDescriptor_d938547f84707355) }

var fileDescriptor_d938547f84707355 = []byte{
	// 249 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x64, 0x90, 0x41, 0x4b, 0xc3, 0x30,
	0x14, 0xc7, 0x69, 0xb2, 0xd4, 0xf9, 0x36, 0x45, 0x82, 0x48, 0x0e, 0x1e, 0x4a, 0xf1, 0xd0, 0x53,
	0x0f, 0xf3, 0x1b, 0xcc, 0x1d, 0xdc, 0x61, 0x20, 0xd9, 0x27, 0x48, 0xd7, 0x6e, 0x2d, 0x73, 0x4d,
	0xc8, 0x0b, 0x43, 0xbf, 0xbd, 0xe4, 0xb5, 0x45, 0xc4, 0xdb, 0xfb, 0xff, 0x5e, 0x92, 0xdf, 0x9f,
	0xc0, 0x22, 0x7c, 0xbb, 0x06, 0x4b, 0xe7, 0x6d, 0xb0, 0x92, 0xb9, 0x2a, 0xbf, 0xc2, 0x7c, 0xfd,
	0x69, 0x0f, 0xe7, 0x1d, 0x9e, 0xe4, 0x3d, 0xb0, 0xae, 0x56, 0x49, 0x96, 0x14, 0x5c, 0xb3, 0xae,
	0x96, 0x0f, 0xc0, 0xaf, 0xfd, 0x51, 0xb1, 0x2c, 0x29, 0x84, 0x8e, 0x63, 0x3c, 0x61, 0xbc, 0xe2,
	0x04, 0x98, 0xf1, 0x52, 0xc2, 0x0c, 0xfb, 0xed, 0x46, 0xcd, 0x88, 0xd0, 0x2c, 0x5f, 0x20, 0xc5,
	0xd6, 0xf8, 0x1a, 0x95, 0xc8, 0x78, 0xb1, 0x58, 0x2d, 0x4b, 0x57, 0x95, 0xfb, 0x48, 0x76, 0x78,
	0xd2, 0xe3, 0x2e, 0xff, 0x82, 0xf9, 0xc4, 0xfe, 0x79, 0x9f, 0x20, 0xed, 0x6d, 0xdd, 0x6c, 0x37,
	0xa3, 0x7a, 0x4c, 0xd4, 0xa7, 0x3d, 0x92, 0x7e, 0xa9, 0xe3, 0x28, 0x15, 0xdc, 0x54, 0xb1, 0xfd,
	0x58, 0x81, 0xeb, 0x29, 0xc6, 0xcd, 0x70, 0x6b, 0xa5, 0x04, 0x3d, 0x32, 0xc5, 0x7c, 0x0f, 0x77,
	0x6f, 0x6d, 0x73, 0x38, 0x7f, 0xd8, 0xae, 0x0f, 0x7f, 0xf5, 0x82, 0xf4, 0x8f, 0x20, 0x30, 0x18,
	0x1f, 0xc8, 0xce, 0xf5, 0x10, 0xe4, 0x33, 0xdc, 0x86, 0xee, 0xd2, 0x60, 0x30, 0x17, 0x47, 0x15,
	0xb8, 0xfe, 0x05, 0x6b, 0xf6, 0x9e, 0x54, 0x29, 0xfd, 0xea, 0xeb, 0x4f, 0x00, 0x00, 0x00, 0xff,
	0xff, 0x45, 0x8c, 0xa9, 0x1b, 0x64, 0x01, 0x00, 0x00,
}
