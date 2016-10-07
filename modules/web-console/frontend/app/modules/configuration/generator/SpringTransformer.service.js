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

import _ from 'lodash';

import AbstractTransformer from './AbstractTransformer';
import StringBuilder from './StringBuilder';

export default ['JavaTypes', 'igniteEventGroups', 'IgniteConfigurationGenerator', (JavaTypes, eventGroups, generator) => {
    return class SpringTransformer extends AbstractTransformer {
        static generator = generator;

        static comment(sb, ...lines) {
            if (lines.length > 1) {
                sb.append('<!--');

                _.forEach(lines, (line) => sb.append(`  ${line}`));

                sb.append('-->');
            }
            else
                sb.append(`<!-- ${_.head(lines)} -->`);
        }

        static appendBean(sb, bean, appendId) {
            if (appendId)
                sb.startBlock(`<bean id="${bean.id}" class="${bean.clsName}">`);
            else
                sb.startBlock(`<bean class="${bean.clsName}">`);

            _.forEach(bean.arguments, (arg) => {
                if (_.isNil(arg.value)) {
                    sb.startBlock('<constructor-arg>');
                    sb.append('<null/>');
                    sb.endBlock('</constructor-arg>');
                }
                else if (arg.constant) {
                    sb.startBlock('<constructor-arg>');
                    sb.append(`<util:constant static-field="${arg.clsName}.${arg.value}"/>`);
                    sb.endBlock('</constructor-arg>');
                }
                else if (arg.clsName === 'Bean') {
                    sb.startBlock('<constructor-arg>');
                    this.appendBean(sb, arg.value);
                    sb.endBlock('</constructor-arg>');
                }
                else
                    sb.append(`<constructor-arg value="${this._toObject(arg.clsName, arg.value)}"/>`);
            });

            this._setProperties(sb, bean);

            sb.endBlock('</bean>');
        }

        static _toObject(clsName, items) {
            return _.map(_.isArray(items) ? items : [items], (item) => {
                switch (clsName) {
                    case 'PROPERTY':
                    case 'PROPERTY_CHAR':
                        return `\${${item}}`;
                    case 'java.lang.Class':
                        return JavaTypes.fullClassName(item);
                    default:
                        return item;
                }
            });
        }

        static _isBean(clsName) {
            return JavaTypes.nonBuiltInClass(clsName) && JavaTypes.nonEnum(clsName) && !JavaTypes.isJavaPrimitive(clsName);
        }

        static _setCollection(sb, prop, tag) {
            sb.startBlock(`<property name="${prop.name}">`);
            sb.startBlock(`<${tag}>`);

            _.forEach(prop.items, (item) => {
                if (this._isBean(prop.typeClsName))
                    this.appendBean(sb, item);
                else
                    sb.append(`<value>${item}</value>`);
            });

            sb.endBlock(`</${tag}>`);
            sb.endBlock('</property>');
        }

        /**
         *
         * @param {StringBuilder} sb
         * @param {Bean} bean
         * @returns {StringBuilder}
         */
        static _setProperties(sb, bean) {
            _.forEach(bean.properties, (prop, idx) => {
                switch (prop.clsName) {
                    case 'DATA_SOURCE':
                        sb.append(`<property name="${prop.name}" ref="${prop.id}"/>`);

                        break;
                    case 'EVENT_TYPES':
                        sb.startBlock(`<property name="${prop.name}">`);

                        if (prop.eventTypes.length === 1) {
                            const evtGrp = _.find(eventGroups, {value: _.head(prop.eventTypes)});

                            evtGrp && sb.append(`<util:constant static-field="${evtGrp.class}.${evtGrp.value}"/>`);
                        }
                        else {
                            sb.startBlock('<list>');

                            _.forEach(prop.eventTypes, (item, ix) => {
                                ix > 0 && sb.emptyLine();

                                const evtGrp = _.find(eventGroups, {value: item});

                                if (evtGrp) {
                                    sb.append(`<!-- EventType.${item} -->`);

                                    _.forEach(evtGrp.events, (event) =>
                                        sb.append(`<util:constant static-field="${evtGrp.class}.${event}"/>`));
                                }
                            });

                            sb.endBlock('</list>');
                        }

                        sb.endBlock('</property>');

                        break;
                    case 'ARRAY':
                        this._setCollection(sb, prop, 'array');

                        break;
                    case 'COLLECTION':
                        this._setCollection(sb, prop, 'list');

                        break;
                    case 'MAP':
                        sb.startBlock(`<property name="${prop.name}">`);
                        sb.startBlock('<map>');

                        const keyClsName = JavaTypes.shortClassName(prop.keyClsName);
                        const valClsName = JavaTypes.shortClassName(prop.valClsName);

                        _.forEach(prop.entries, (entry) => {
                            const key = entry[prop.keyField];
                            const val = entry[prop.valField];

                            if (keyClsName === 'Bean' || valClsName === 'Bean') {
                                sb.startBlock('<entry>');
                                sb.startBlock('<key>');
                                if (keyClsName === 'Bean')
                                    this.appendBean(sb, key);
                                else
                                    sb.append(this._toObject(keyClsName, key));
                                sb.endBlock('</key>');
                                sb.startBlock('<value>');
                                if (valClsName === 'Bean')
                                    this.appendBean(sb, val);
                                else
                                    sb.append(this._toObject(valClsName, val));
                                sb.endBlock('</value>');
                                sb.endBlock('</entry>');
                            }
                            else
                                sb.append(`<entry key="${this._toObject(keyClsName, key)}" value="${this._toObject(valClsName, val)}"/>`);
                        });

                        sb.endBlock('</map>');
                        sb.endBlock('</property>');

                        break;
                    case 'PROPERTIES':
                        sb.startBlock(`<property name="${prop.name}">`);
                        sb.startBlock('<props>');

                        _.forEach(prop.entries, (entry) => {
                            sb.append(`<prop key="${entry.name}">${entry.value}</prop>`);
                        });

                        sb.endBlock('</props>');
                        sb.endBlock('</property>');

                        break;
                    case 'BEAN':
                        if (idx !== 0)
                            sb.emptyLine();

                        sb.startBlock(`<property name="${prop.name}">`);

                        this.appendBean(sb, prop.value);

                        sb.endBlock('</property>');

                        if (idx !== bean.properties.length - 1 && JavaTypes.shortClassName(bean.properties[idx + 1].clsName).toUpperCase() !== 'BEAN')
                            sb.emptyLine();

                        break;
                    default:
                        sb.append(`<property name="${prop.name}" value="${this._toObject(prop.clsName, prop.value)}"/>`);
                }
            });

            return sb;
        }

        /**
         * Build final XML.
         *
         * @param {Object} cluster
         * @param {Boolean} client
         * @returns {StringBuilder}
         */
        static igniteConfiguration(cluster, client) {
            const cfg = generator.igniteConfiguration(cluster, client);
            const sb = new StringBuilder();

            // 0. Add header.
            sb.append('<?xml version="1.0" encoding="UTF-8"?>');
            sb.emptyLine();

            this.mainComment(sb);
            sb.emptyLine();

            // 1. Start beans section.
            sb.startBlock([
                '<beans xmlns="http://www.springframework.org/schema/beans"',
                '       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
                '       xmlns:util="http://www.springframework.org/schema/util"',
                '       xsi:schemaLocation="http://www.springframework.org/schema/beans',
                '                           http://www.springframework.org/schema/beans/spring-beans.xsd',
                '                           http://www.springframework.org/schema/util',
                '                           http://www.springframework.org/schema/util/spring-util.xsd">']);

            // 2. Add external property file
            if (this.hasProperties(cfg)) {
                this.comment(sb, 'Load external properties file.');

                sb.startBlock('<bean id="placeholderConfig" class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">');
                sb.append('<property name="location" value="classpath:secret.properties"/>');
                sb.endBlock('</bean>');

                sb.emptyLine();
            }

            // 3. Add data sources.
            const dataSources = this.collectDataSources(cfg);

            if (dataSources.length) {
                this.comment(sb, 'Data source beans will be initialized from external properties file.');

                _.forEach(dataSources, (ds) => {
                    this.appendBean(sb, ds, true);

                    sb.emptyLine();
                });
            }

            if (client) {
                const nearCaches = _.filter(cluster.caches, (cache) => _.get(cache, 'clientNearConfiguration.enabled'));

                _.forEach(nearCaches, (cache) => {
                    this.comment(sb, 'Configuration of near cache for cache "' + cache.name + '"');

                    this.appendBean(sb, generator.cacheNearClient(cache), true);

                    sb.emptyLine();
                });
            }

            // 3. Add main content.
            this.appendBean(sb, cfg);

            // 4. Close beans section.
            sb.endBlock('</beans>');

            return sb;
        }
    };
}];
