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

import {merge} from 'rxjs';
import {tap} from 'rxjs/operators';
import {Component, Inject, OnInit, OnDestroy} from '@angular/core';
import {FormGroup, FormControl, Validators, FormBuilder} from '@angular/forms';
import templateUrl from 'file-loader!./template.html';
import './style.scss';
import {default as CountriesFactory, Country} from 'app/services/Countries.service';
import {default as UserFactory, User} from 'app/modules/user/User.service';
import {Confirm} from 'app/services/Confirm.service';
import {default as LegacyUtilsFactory} from 'app/services/LegacyUtils.service';
import {
    FORM_FIELD_OPTIONS, FormFieldRequiredMarkerStyles, FormFieldErrorStyles
} from '../formField.component';

const passwordMatch = (newPassword: string) => (confirmPassword: FormControl) => newPassword === confirmPassword.value
    ? null
    : {passwordMatch: true};

const disableFormGroup = (fg: FormGroup) => {fg.disable(); return fg;};

@Component({
    selector: 'page-profile-angular',
    templateUrl,
    viewProviders: [
        {
            provide: FORM_FIELD_OPTIONS,
            useValue: {
                requiredMarkerStyle: FormFieldRequiredMarkerStyles.OPTIONAL,
                errorStyle: FormFieldErrorStyles.ICON
            }
        }
    ]
})
export class PageProfile implements OnInit, OnDestroy {
    static parameters = [
        [new Inject('IgniteCountries')],
        [new Inject('User')],
        [new Inject('Confirm')],
        [new Inject('IgniteLegacyUtils')],
        [new Inject(FormBuilder)]
    ]
    constructor(
        Countries: ReturnType<typeof CountriesFactory>,
        private User: ReturnType<typeof UserFactory>,
        private Confirm: Confirm,
        private LegacyUtils: ReturnType<typeof LegacyUtilsFactory>,
        private fb: FormBuilder
    ) {
        this.countries = Countries.getAll();
    }

    countries: Country[]
    user: User

    async ngOnInit() {
        this.user = await this.User.read();
        this.form.patchValue(this.user);
    }
    ngOnDestroy() {
        this.subscriber.unsubscribe();
    }
    async saveUser(): Promise<void> {
        if (this.form.invalid) return;
        await this.User.save(this.prepareFormValue(this.form));
        this.form.get('passwordPanelOpened').setValue(false);
    }
    prepareFormValue(form: PageProfile['form']): Partial<User> {
        return {
            firstName: form.value.firstName,
            lastName: form.value.lastName,
            email: form.value.email,
            phone: form.value.phone,
            country: form.value.country,
            company: form.value.company,
            token: form.controls.token.value,
            ...form.value.passwordPanelOpened ? {password: form.value.password.new} : {}
        };
    }
    async generateToken() {
        try {
            await this.Confirm.confirm('Are you sure you want to change security token?<br>If you change the token you will need to restart the agent.');
            this.form.get('token').setValue(this.LegacyUtils.randomString(20));
        } catch (e) {
            // no-op
        }
    }
    form = this.fb.group({
        firstName: ['', Validators.required],
        lastName: ['', Validators.required],
        email: ['', [Validators.required, Validators.email]],
        phone: '',
        country: ['', Validators.required],
        company: ['', Validators.required],
        password: disableFormGroup(this.fb.group({
            new: ['', Validators.required],
            confirm: ''
        })),
        passwordPanelOpened: false,
        token: this.fb.control({value: '', disabled: true}, [Validators.required])
    })
    subscriber = merge(
        this.form.get('passwordPanelOpened').valueChanges.pipe(
            tap((opened: boolean) => {
                this.form.get('password')[opened ? 'enable' : 'disable']();
                this.form.get('password').updateValueAndValidity();
                if (opened) this.form.get('password').reset();
            })
        ),
        this.form.get('password.new').valueChanges.pipe(
            tap((newPassword: string) => {
                this.form.get('password.confirm').setValidators([Validators.required, passwordMatch(newPassword)]);
                this.form.get('password.confirm').updateValueAndValidity();
            })
        )
    ).subscribe();
}
