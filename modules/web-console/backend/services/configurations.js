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

'use strict';

// Fire me up!

module.exports = {
    implements: 'services/configurations',
    inject: ['mongo', 'services/spaces', 'services/clusters', 'services/caches', 'services/domains']
};

/**
 * @param mongo
 * @param {SpacesService} spacesService
 * @param {ClustersService} clustersService
 * @param {CachesService} cachesService
 * @param {DomainsService} domainsService
 * @returns {ConfigurationsService}
 */
module.exports.factory = (mongo, spacesService, clustersService, cachesService, domainsService) => {
    class ConfigurationsService {
        static list(userId, demo) {
            let spaces;

            return spacesService.spaces(userId, demo)
                .then((_spaces) => {
                    spaces = _spaces;

                    return spaces.map((space) => space._id);
                })
                .then((spaceIds) => Promise.all([
                    clustersService.listBySpaces(spaceIds),
                    domainsService.listBySpaces(spaceIds),
                    cachesService.listBySpaces(spaceIds)
                ]))
                .then(([clusters, domains, caches]) => ({clusters, domains, caches, spaces}));
        }

        static get(userId, demo, _id) {
            return clustersService.get(userId, demo, _id)
                .then((cluster) =>
                    Promise.all([
                        mongo.Cache.find({space: cluster.space, _id: {$in: cluster.caches}}).lean().exec(),
                        mongo.DomainModel.find({space: cluster.space, _id: {$in: cluster.models}}).lean().exec()
                    ])
                        .then(([caches, models]) => ({cluster, caches, models}))
                );
        }
    }

    return ConfigurationsService;
};
