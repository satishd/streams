/*
 * Copyright 2015-2016 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This needs to be required, otherwise React doesn't play nice with jsdom...
var ExecutionEnvironment = require('../../../../node_modules/fbjs/lib/ExecutionEnvironment');
ExecutionEnvironment.canUseDOM = true;

import './jsdom-setup';
import './require-extensions';

import '../../../common/utils/test-utils/index';

export * from './mock-require-ensure';
export * from './mock-react-component';
export * from './find-dom-node';