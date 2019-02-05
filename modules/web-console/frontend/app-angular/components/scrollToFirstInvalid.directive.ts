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

import {Directive, HostListener, Input, Inject, ElementRef} from '@angular/core';
import {NgForm} from '@angular/forms';

@Directive({
    selector: '[scrollToFirstInvalid]'
})
export class ScrollToFirstInvalid {
    @Input()
    formGroup: NgForm

    @HostListener('ngSubmit', ['$event'])
    onSubmit(e: Event) {
        if (this.formGroup.invalid) {
            const invalidEl = this.findFirstInvalid();
            if (invalidEl) {
                this.scrollIntoView(invalidEl);
                invalidEl.focus();
                this.maybeShowValidationErrorPopover(invalidEl);
            }
        }
    }

    private maybeShowValidationErrorPopover(el: HTMLInputElement): void {
        try {
            el.closest('form-field').querySelector('form-field-errors ignite-icon').dispatchEvent(new MouseEvent('mouseenter'));
        } catch (e) {
            // no-op
        }
    }

    private findFirstInvalid(): HTMLInputElement | null {
        return this.el.nativeElement.querySelector('.ng-invalid');
    }

    private scrollIntoView(el: Element): void {
        el.scrollIntoView({block: 'center'});
    }

    static parameters = [[new Inject(ElementRef)]]
    constructor(private el: ElementRef<HTMLFormElement>) {}
}
