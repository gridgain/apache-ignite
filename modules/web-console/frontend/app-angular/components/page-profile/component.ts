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

import {Component, Inject, OnInit} from '@angular/core';
import {FormGroup, FormControl, Validators} from '@angular/forms';
import templateUrl from 'file-loader!./template.html';
import './style.scss';
import {default as CountriesFactory, Country} from 'app/services/Countries.service';
import {default as UserFactory, User} from 'app/modules/user/User.service';
import {
    FORM_FIELD_OPTIONS, FormFieldRequiredMarkerStyles, FormFieldErrorStyles
} from '../formField.component';

@Component({
    selector: 'page-profile-angular',
    templateUrl,
    viewProviders: [
        {
            provide: FORM_FIELD_OPTIONS,
            useValue: {
                requiredMarkerStyle: FormFieldRequiredMarkerStyles.OPTIONAL,
                // requiredMarkerStyle: FormFieldRequiredMarkerStyles.REQUIRED,
                errorStyle: FormFieldErrorStyles.ICON
                // errorStyle: FormFieldErrorStyles.INLINE
            }
        }
    ]
})
export class PageProfile implements OnInit {
    static parameters = [
        [new Inject('IgniteCountries')],
        [new Inject('User')]
    ]
    countries: Country[]
    user: User
    constructor(
        Countries: ReturnType<typeof CountriesFactory>,
        private User: ReturnType<typeof UserFactory>
    ) {
        this.countries = Countries.getAll();
    }
    async ngOnInit() {
        this.user = await this.User.read();
        this.form.patchValue(this.user);
    }
    saveUser() {
        console.log(this.form.getRawValue());
    }
    form = new FormGroup({
        firstName: new FormControl('', [Validators.required]),
        lastName: new FormControl('', [Validators.required]),
        email: new FormControl('', [Validators.required, Validators.email]),
        phone: new FormControl('', []),
        country: new FormControl('', [Validators.required]),
        company: new FormControl('', [Validators.required]),
        token: new FormControl({value: '', disabled: true}, []),
        password: new FormControl('', [Validators.required]),
        oldPassword: new FormControl('', [Validators.required])
    })
}
