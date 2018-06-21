/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ignite/impl/binary/binary_object_impl.h>

#include <ignite/impl/thin/response_status.h>
#include <ignite/impl/thin/message.h>

#include <ignite/impl/thin/cache/cache_client_impl.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                CacheClientImpl::CacheClientImpl(const SP_DataRouter& router, const std::string& name, int32_t id):
                    router(router),
                    name(name),
                    id(id),
                    binary(false),
                    assignment(),
                    mask(-1)
                {
                    // No-op.
                }

                CacheClientImpl::~CacheClientImpl()
                {
                    // No-op.
                }

                void CacheClientImpl::Put(const Writable& key, const Writable& value)
                {
                    CachePutRequest req(id, binary, key, value);
                    Response rsp;

                    const std::vector<net::EndPoint>& endPoints = GetEndPointsForKey(key);

                    router.Get()->SyncMessage(req, rsp, endPoints);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                void CacheClientImpl::Get(const Writable& key, Readable& value)
                {
                    CacheKeyRequest<RequestType::CACHE_GET> req(id, binary, key);
                    CacheGetResponse rsp(value);
                    
                    const std::vector<net::EndPoint>& endPoints = GetEndPointsForKey(key);

                    router.Get()->SyncMessage(req, rsp, endPoints);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                bool CacheClientImpl::ContainsKey(const Writable& key)
                {
                    CacheKeyRequest<RequestType::CACHE_CONTAINS_KEY> req(id, binary, key);
                    BoolResponse rsp;
                    
                    const std::vector<net::EndPoint>& endPoints = GetEndPointsForKey(key);

                    router.Get()->SyncMessage(req, rsp, endPoints);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());

                    return rsp.GetValue();
                }

                void CacheClientImpl::UpdatePartitions()
                {
                    std::vector<ConnectableNodePartitions> nodeParts;

                    CacheRequest<RequestType::CACHE_NODE_PARTITIONS> req(id, binary);
                    ClientCacheNodePartitionsResponse rsp(nodeParts);

                    router.Get()->SyncMessageNoMetaUpdate(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());

                    std::vector<ConnectableNodePartitions>::const_iterator it;

                    for (it = nodeParts.begin(); it != nodeParts.end(); ++it)
                    {
                        const std::vector<int32_t>& parts = it->GetPartitions();
                        const std::vector<net::EndPoint>& endPoints = it->GetEndPoints();

                        for (size_t i = 0; i < parts.size(); ++i)
                        {
                            assert(parts[i] >= 0);

                            size_t upart = static_cast<size_t>(parts[i]);

                            if (upart >= assignment.size())
                                assignment.resize(upart + 1);

                            std::vector<net::EndPoint>& dst = assignment[upart];

                            assert(dst.empty());

                            dst.insert(dst.end(), endPoints.begin(), endPoints.end());
                        }
                    }

                    int32_t parts = static_cast<int32_t>(assignment.size());

                    mask = (parts & (parts - 1)) == 0 ? parts - 1 : -1;

                    for (size_t i = 0; i < assignment.size(); ++i)
                    {
                        std::cout << assignment[i].size() << " ";

                        for (size_t j = 0; j < assignment[i].size(); ++j)
                            std::cout << assignment[i][j].host << " ";
                        
                        std::cout << assignment[i][0].port << std::endl;
                    }
                }

                const std::vector<net::EndPoint>& CacheClientImpl::GetEndPointsForKey(const Writable& key) const
                {
                    assert(!assignment.empty());
                    
                    int32_t hash = GetHash(key);
                    int32_t part = 0;

                    if (mask >= 0)
                        part = (hash ^ (static_cast<uint32_t>(hash) >> 16)) & mask;
                    else
                    {
                        part = std::abs(hash % static_cast<int32_t>(assignment.size()));

                        if (part < 0)
                            part = 0;
                    }

                    return assignment[part];
                }

                int32_t CacheClientImpl::GetHash(const Writable& key) const
                {
                    enum { BUFFER_SIZE = 1024 };

                    interop::InteropUnpooledMemory mem(BUFFER_SIZE);
                    interop::InteropOutputStream stream(&mem);
                    binary::BinaryWriterImpl writer(&stream, 0);

                    key.Write(writer);

                    binary::BinaryObjectImpl binaryKey(mem, 0, 0, 0);

                    return binaryKey.GetHashCode();
                }
            }
        }
    }
}

