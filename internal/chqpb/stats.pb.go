// Copyright 2024 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.1
// 	protoc        v5.26.1
// source: stats.proto

package chqpb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type LogStatsReport struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	SubmittedAt int64       `protobuf:"varint,1,opt,name=submittedAt,proto3" json:"submittedAt,omitempty"`
	Stats       []*LogStats `protobuf:"bytes,2,rep,name=stats,proto3" json:"stats,omitempty"`
}

func (x *LogStatsReport) Reset() {
	*x = LogStatsReport{}
	if protoimpl.UnsafeEnabled {
		mi := &file_stats_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LogStatsReport) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogStatsReport) ProtoMessage() {}

func (x *LogStatsReport) ProtoReflect() protoreflect.Message {
	mi := &file_stats_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogStatsReport.ProtoReflect.Descriptor instead.
func (*LogStatsReport) Descriptor() ([]byte, []int) {
	return file_stats_proto_rawDescGZIP(), []int{0}
}

func (x *LogStatsReport) GetSubmittedAt() int64 {
	if x != nil {
		return x.SubmittedAt
	}
	return 0
}

func (x *LogStatsReport) GetStats() []*LogStats {
	if x != nil {
		return x.Stats
	}
	return nil
}

type LogStats struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ServiceName string `protobuf:"bytes,1,opt,name=serviceName,proto3" json:"serviceName,omitempty"`
	Fingerprint int64  `protobuf:"varint,2,opt,name=fingerprint,proto3" json:"fingerprint,omitempty"`
	WasFiltered bool   `protobuf:"varint,3,opt,name=wasFiltered,proto3" json:"wasFiltered,omitempty"`
	WouldFilter bool   `protobuf:"varint,4,opt,name=wouldFilter,proto3" json:"wouldFilter,omitempty"`
	Count       int64  `protobuf:"varint,5,opt,name=count,proto3" json:"count,omitempty"`
}

func (x *LogStats) Reset() {
	*x = LogStats{}
	if protoimpl.UnsafeEnabled {
		mi := &file_stats_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LogStats) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogStats) ProtoMessage() {}

func (x *LogStats) ProtoReflect() protoreflect.Message {
	mi := &file_stats_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogStats.ProtoReflect.Descriptor instead.
func (*LogStats) Descriptor() ([]byte, []int) {
	return file_stats_proto_rawDescGZIP(), []int{1}
}

func (x *LogStats) GetServiceName() string {
	if x != nil {
		return x.ServiceName
	}
	return ""
}

func (x *LogStats) GetFingerprint() int64 {
	if x != nil {
		return x.Fingerprint
	}
	return 0
}

func (x *LogStats) GetWasFiltered() bool {
	if x != nil {
		return x.WasFiltered
	}
	return false
}

func (x *LogStats) GetWouldFilter() bool {
	if x != nil {
		return x.WouldFilter
	}
	return false
}

func (x *LogStats) GetCount() int64 {
	if x != nil {
		return x.Count
	}
	return 0
}

type MetricStatsReport struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	SubmittedAt int64          `protobuf:"varint,1,opt,name=submittedAt,proto3" json:"submittedAt,omitempty"`
	Stats       []*MetricStats `protobuf:"bytes,2,rep,name=stats,proto3" json:"stats,omitempty"`
}

func (x *MetricStatsReport) Reset() {
	*x = MetricStatsReport{}
	if protoimpl.UnsafeEnabled {
		mi := &file_stats_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MetricStatsReport) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MetricStatsReport) ProtoMessage() {}

func (x *MetricStatsReport) ProtoReflect() protoreflect.Message {
	mi := &file_stats_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MetricStatsReport.ProtoReflect.Descriptor instead.
func (*MetricStatsReport) Descriptor() ([]byte, []int) {
	return file_stats_proto_rawDescGZIP(), []int{2}
}

func (x *MetricStatsReport) GetSubmittedAt() int64 {
	if x != nil {
		return x.SubmittedAt
	}
	return 0
}

func (x *MetricStatsReport) GetStats() []*MetricStats {
	if x != nil {
		return x.Stats
	}
	return nil
}

type MetricStats struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name                string  `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	TagName             string  `protobuf:"bytes,2,opt,name=tagName,proto3" json:"tagName,omitempty"`
	CardinalityEstimate float64 `protobuf:"fixed64,3,opt,name=cardinalityEstimate,proto3" json:"cardinalityEstimate,omitempty"`
	Hll                 []byte  `protobuf:"bytes,4,opt,name=hll,proto3" json:"hll,omitempty"`
}

func (x *MetricStats) Reset() {
	*x = MetricStats{}
	if protoimpl.UnsafeEnabled {
		mi := &file_stats_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MetricStats) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MetricStats) ProtoMessage() {}

func (x *MetricStats) ProtoReflect() protoreflect.Message {
	mi := &file_stats_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MetricStats.ProtoReflect.Descriptor instead.
func (*MetricStats) Descriptor() ([]byte, []int) {
	return file_stats_proto_rawDescGZIP(), []int{3}
}

func (x *MetricStats) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *MetricStats) GetTagName() string {
	if x != nil {
		return x.TagName
	}
	return ""
}

func (x *MetricStats) GetCardinalityEstimate() float64 {
	if x != nil {
		return x.CardinalityEstimate
	}
	return 0
}

func (x *MetricStats) GetHll() []byte {
	if x != nil {
		return x.Hll
	}
	return nil
}

var File_stats_proto protoreflect.FileDescriptor

var file_stats_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x73, 0x74, 0x61, 0x74, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x63,
	0x68, 0x71, 0x70, 0x62, 0x22, 0x59, 0x0a, 0x0e, 0x4c, 0x6f, 0x67, 0x53, 0x74, 0x61, 0x74, 0x73,
	0x52, 0x65, 0x70, 0x6f, 0x72, 0x74, 0x12, 0x20, 0x0a, 0x0b, 0x73, 0x75, 0x62, 0x6d, 0x69, 0x74,
	0x74, 0x65, 0x64, 0x41, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x73, 0x75, 0x62,
	0x6d, 0x69, 0x74, 0x74, 0x65, 0x64, 0x41, 0x74, 0x12, 0x25, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x74,
	0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x63, 0x68, 0x71, 0x70, 0x62, 0x2e,
	0x4c, 0x6f, 0x67, 0x53, 0x74, 0x61, 0x74, 0x73, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x73, 0x22,
	0xa8, 0x01, 0x0a, 0x08, 0x4c, 0x6f, 0x67, 0x53, 0x74, 0x61, 0x74, 0x73, 0x12, 0x20, 0x0a, 0x0b,
	0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0b, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x20,
	0x0a, 0x0b, 0x66, 0x69, 0x6e, 0x67, 0x65, 0x72, 0x70, 0x72, 0x69, 0x6e, 0x74, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x0b, 0x66, 0x69, 0x6e, 0x67, 0x65, 0x72, 0x70, 0x72, 0x69, 0x6e, 0x74,
	0x12, 0x20, 0x0a, 0x0b, 0x77, 0x61, 0x73, 0x46, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x65, 0x64, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0b, 0x77, 0x61, 0x73, 0x46, 0x69, 0x6c, 0x74, 0x65, 0x72,
	0x65, 0x64, 0x12, 0x20, 0x0a, 0x0b, 0x77, 0x6f, 0x75, 0x6c, 0x64, 0x46, 0x69, 0x6c, 0x74, 0x65,
	0x72, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0b, 0x77, 0x6f, 0x75, 0x6c, 0x64, 0x46, 0x69,
	0x6c, 0x74, 0x65, 0x72, 0x12, 0x14, 0x0a, 0x05, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x05, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x22, 0x5f, 0x0a, 0x11, 0x4d, 0x65,
	0x74, 0x72, 0x69, 0x63, 0x53, 0x74, 0x61, 0x74, 0x73, 0x52, 0x65, 0x70, 0x6f, 0x72, 0x74, 0x12,
	0x20, 0x0a, 0x0b, 0x73, 0x75, 0x62, 0x6d, 0x69, 0x74, 0x74, 0x65, 0x64, 0x41, 0x74, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x73, 0x75, 0x62, 0x6d, 0x69, 0x74, 0x74, 0x65, 0x64, 0x41,
	0x74, 0x12, 0x28, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x12, 0x2e, 0x63, 0x68, 0x71, 0x70, 0x62, 0x2e, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x53,
	0x74, 0x61, 0x74, 0x73, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x73, 0x22, 0x7f, 0x0a, 0x0b, 0x4d,
	0x65, 0x74, 0x72, 0x69, 0x63, 0x53, 0x74, 0x61, 0x74, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61,
	0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x18,
	0x0a, 0x07, 0x74, 0x61, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x07, 0x74, 0x61, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x30, 0x0a, 0x13, 0x63, 0x61, 0x72, 0x64,
	0x69, 0x6e, 0x61, 0x6c, 0x69, 0x74, 0x79, 0x45, 0x73, 0x74, 0x69, 0x6d, 0x61, 0x74, 0x65, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x01, 0x52, 0x13, 0x63, 0x61, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x6c, 0x69,
	0x74, 0x79, 0x45, 0x73, 0x74, 0x69, 0x6d, 0x61, 0x74, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x68, 0x6c,
	0x6c, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x68, 0x6c, 0x6c, 0x42, 0x09, 0x5a, 0x07,
	0x2e, 0x3b, 0x63, 0x68, 0x71, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_stats_proto_rawDescOnce sync.Once
	file_stats_proto_rawDescData = file_stats_proto_rawDesc
)

func file_stats_proto_rawDescGZIP() []byte {
	file_stats_proto_rawDescOnce.Do(func() {
		file_stats_proto_rawDescData = protoimpl.X.CompressGZIP(file_stats_proto_rawDescData)
	})
	return file_stats_proto_rawDescData
}

var file_stats_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_stats_proto_goTypes = []interface{}{
	(*LogStatsReport)(nil),    // 0: chqpb.LogStatsReport
	(*LogStats)(nil),          // 1: chqpb.LogStats
	(*MetricStatsReport)(nil), // 2: chqpb.MetricStatsReport
	(*MetricStats)(nil),       // 3: chqpb.MetricStats
}
var file_stats_proto_depIdxs = []int32{
	1, // 0: chqpb.LogStatsReport.stats:type_name -> chqpb.LogStats
	3, // 1: chqpb.MetricStatsReport.stats:type_name -> chqpb.MetricStats
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_stats_proto_init() }
func file_stats_proto_init() {
	if File_stats_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_stats_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LogStatsReport); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_stats_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LogStats); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_stats_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MetricStatsReport); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_stats_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MetricStats); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_stats_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_stats_proto_goTypes,
		DependencyIndexes: file_stats_proto_depIdxs,
		MessageInfos:      file_stats_proto_msgTypes,
	}.Build()
	File_stats_proto = out.File
	file_stats_proto_rawDesc = nil
	file_stats_proto_goTypes = nil
	file_stats_proto_depIdxs = nil
}