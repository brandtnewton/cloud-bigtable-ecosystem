/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package bigtableclient

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
)

type BulkOperationResponse struct {
	FailedRows string
}

type BigtableBulkMutation struct {
	mutations map[types.TableName][]types.IBigtableMutation
}

func NewBigtableBulkMutation() *BigtableBulkMutation {
	return &BigtableBulkMutation{mutations: make(map[types.TableName][]types.IBigtableMutation)}
}

func (b *BigtableBulkMutation) Mutations() map[types.TableName][]types.IBigtableMutation {
	return b.mutations
}

func (b *BigtableBulkMutation) AddMutation(mut types.IBigtableMutation) {
	if b.mutations[mut.Table()] == nil {
		b.mutations[mut.Table()] = []types.IBigtableMutation{}
	}
	b.mutations[mut.Table()] = append(b.mutations[mut.Table()], mut)
}
