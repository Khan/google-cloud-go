// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datastore

import (
	"errors"
	"fmt"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// A Mutation represents a change to a Datastore entity.
type Mutation struct {
	key *Key // needed for transaction PendingKeys and to dedup deletions
	mut *pb.Mutation

	// err is set to a Datastore or gRPC error, if Mutation is not valid
	// (see https://godoc.org/google.golang.org/grpc/codes).
	err error
}

func (m *Mutation) isDelete() bool {
	_, ok := m.mut.Operation.(*pb.Mutation_Delete)
	return ok
}

// WithTransforms adds one or more server-side property transformations to the mutation.
// It can be called multiple times to add more transforms.
// The order of transforms is preserved, first by the order of calls to WithTransforms,
// and then by the order of transforms within a single call.
func (m *Mutation) WithTransforms(transforms ...PropertyTransform) *Mutation {
	if m.err != nil {
		return m
	}
	if m.mut == nil {
		m.err = errors.New("datastore: WithTransforms called on uninitialized mutation")
		return m
	}
	if m.isDelete() {
		m.err = errors.New("datastore: property transforms cannot be applied to a delete mutation")
		return m
	}

	for _, transform := range transforms {
		if transform.pb == nil {
			m.err = errors.New("datastore: WithTransforms called with an uninitialized PropertyTransform")
			return m
		}
		m.mut.PropertyTransforms = append(m.mut.PropertyTransforms, transform.pb)
	}

	setMutationProtoPropertyMaskForTransforms(m.mut)

	return m
}

// setMutationProtoPropertyMaskForTransforms sets the property mask on the
// given mutation to match the client-provided property names in the
// mutation. This is only done when transforms are present. Otherwise, no
// property mask (the default) is used, which means "write all properties".
//
// When transforming properties there are two cases: 1) a property is written
// in the mutation (using a value from the client) and then transformed, and
// 2) the server value of the property is read and transformed.
//
// The first case is what the pre-existing datastore library integration tests
// verify (the "PutAndTransform" tests). But this isn't a very interesting
// case! If you're writing a value from the client and then transforming it,
// the client might as well just write the final value in the first place.
//
// The second case is interesting and useful -- it enables transformation of
// server property values in a mutation without a client round trip. To use
// this behavior the property mask must NOT include the property being
// transformed.
//
// This function enables clients to specify an empty entity, e.g.
// &PropertyList{}, to indicate that no client values should be written. If
// the client provide properties to write, this function will add those
// properties to the property mask so they're written as expected (and then
// if a property transformed is also specified). This behavior still allows
// other properties to be omitted so server transforms can occur.
func setMutationProtoPropertyMaskForTransforms(mut *pb.Mutation) {
	if len(mut.PropertyTransforms) == 0 {
		return
	}

	paths := []string{}
	switch o := mut.Operation.(type) {
	case *pb.Mutation_Insert:
		for name := range o.Insert.Properties {
			paths = append(paths, name)
		}
	case *pb.Mutation_Update:
		for name := range o.Update.Properties {
			paths = append(paths, name)
		}
	case *pb.Mutation_Upsert:
		for name := range o.Upsert.Properties {
			paths = append(paths, name)
		}
	}
	mut.PropertyMask = &pb.PropertyMask{Paths: paths}
}

// NewInsert creates a Mutation that will save the entity src into the
// datastore with key k. If k already exists, calling Mutate with the
// Mutation will lead to a gRPC codes.AlreadyExists error.
func NewInsert(k *Key, src interface{}) *Mutation {
	if !k.valid() {
		return &Mutation{err: ErrInvalidKey}
	}
	p, err := saveEntity(k, src)
	if err != nil {
		return &Mutation{err: err}
	}
	return &Mutation{
		key: k,
		mut: &pb.Mutation{Operation: &pb.Mutation_Insert{Insert: p}},
	}
}

// NewUpsert creates a Mutation that saves the entity src into the datastore with key
// k, whether or not k exists. See Client.Put for valid values of src.
func NewUpsert(k *Key, src interface{}) *Mutation {
	if !k.valid() {
		return &Mutation{err: ErrInvalidKey}
	}
	p, err := saveEntity(k, src)
	if err != nil {
		return &Mutation{err: err}
	}
	return &Mutation{
		key: k,
		mut: &pb.Mutation{Operation: &pb.Mutation_Upsert{Upsert: p}},
	}
}

// NewUpdate creates a Mutation that replaces the entity in the datastore with
// key k. If k does not exist, calling Mutate with the Mutation will lead to a
// gRPC codes.NotFound error.
// See Client.Put for valid values of src.
func NewUpdate(k *Key, src interface{}) *Mutation {
	if !k.valid() {
		return &Mutation{err: ErrInvalidKey}
	}
	if k.Incomplete() {
		return &Mutation{err: fmt.Errorf("datastore: can't update the incomplete key: %v", k)}
	}
	p, err := saveEntity(k, src)
	if err != nil {
		return &Mutation{err: err}
	}
	return &Mutation{
		key: k,
		mut: &pb.Mutation{Operation: &pb.Mutation_Update{Update: p}},
	}
}

// NewDelete creates a Mutation that deletes the entity with key k.
func NewDelete(k *Key) *Mutation {
	if !k.valid() {
		return &Mutation{err: ErrInvalidKey}
	}
	if k.Incomplete() {
		return &Mutation{err: fmt.Errorf("datastore: can't delete the incomplete key: %v", k)}
	}
	return &Mutation{
		key: k,
		mut: &pb.Mutation{Operation: &pb.Mutation_Delete{Delete: keyToProto(k)}},
	}
}

func mutationProtos(muts []*Mutation) ([]*pb.Mutation, error) {
	// If any of the mutations have errors, collect and return them.
	var merr MultiError
	for i, m := range muts {
		if m.err != nil {
			if merr == nil {
				merr = make(MultiError, len(muts))
			}
			merr[i] = m.err
		}
	}
	if merr != nil {
		return nil, merr
	}

	var protos []*pb.Mutation
	// Collect protos. Remove duplicate deletions (see deleteMutations).
	seen := map[string]bool{}
	for _, m := range muts {
		if m.isDelete() {
			ks := m.key.stringInternal()
			if seen[ks] {
				continue
			}
			seen[ks] = true
		}
		protos = append(protos, m.mut)
	}
	return protos, nil
}
