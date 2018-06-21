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

#ifndef _IGNITE_IMPL_THIN_NET_TCP_RANGE
#define _IGNITE_IMPL_THIN_NET_TCP_RANGE

#include <stdint.h>
#include <string>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace net
            {
                /**
                 * TCP port range.
                 */
                struct TcpRange
                {
                    /**
                     * Default constructor.
                     */
                    TcpRange() :
                        port(0),
                        range(0)
                    {
                        // No-op.
                    }

                    /**
                     * Constructor.
                     *
                     * @param host Host.
                     * @param port Port.
                     * @param range Number of ports after the @c port that
                     *    should be tried if the previous are unavailable.
                     */
                    TcpRange(const std::string& host, uint16_t port, uint16_t range = 0) :
                        host(host),
                        port(port),
                        range(range)
                    {
                        // No-op.
                    }

                    /** Remote host. */
                    std::string host;

                    /** TCP port. */
                    uint16_t port;

                    /**
                     * Number of ports after the port that should be tried if
                     * the previous are unavailable.
                     */
                    uint16_t range;
                };
            }
        }
    }
}

#endif //_IGNITE_IMPL_THIN_NET_TCP_RANGE