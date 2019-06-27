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

import {CancellationError} from 'app/errors/CancellationError';

/**
 * Service to show various information and error messages.
 * @param {mgcrea.ngStrap.alert.IAlertService} $alert
 * @param {import('./ErrorParser.service').default} errorParser
 */
export default function factory($alert, errorParser) {
    // Common instance of alert modal.
    let msgModal;

    const hideAlert = () => {
        if (msgModal) {
            msgModal.hide();
            msgModal.destroy();
            msgModal = null;
        }
    };

    const _showMessage = (message, err, type, duration) => {
        hideAlert();

        const parsedErr = err ? errorParser.parse(err, message) : errorParser.parse(message, null);
        const causes = parsedErr.causes;
        const title = parsedErr.message + (causes.length ? '<ul><li>' + causes.join('</li><li>') + '</li></ul>See node logs for more details.' : '');

        msgModal = $alert({type, title, duration: duration + causes.length * 5});

        msgModal.$scope.icon = `icon-${type}`;
    };

    return {
        hideAlert,
        /**
         * @param {string|CancellationError} message
         * @param [err]
         */
        showError(message, err, duration = 10) {
            if (message instanceof CancellationError)
                return false;

            _showMessage(message, err, 'danger', duration);

            return false;
        },
        /**
         * @param {string} message
         */
        showInfo(message, duration = 5) {
            _showMessage(message, null, 'success', duration);
        }
    };
}

factory.$inject = ['$alert', 'IgniteErrorParser'];
