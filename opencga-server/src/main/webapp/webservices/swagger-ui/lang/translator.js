/*
 * Copyright 2015-2020 OpenCB
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

'use strict';

/**
 * Translator for documentation pages.
 *
 * To enable translation you should include one of language-files in your index.html
 * after <script src='lang/translator.js' type='text/javascript'></script>.
 * For example - <script src='lang/ru.js' type='text/javascript'></script>
 *
 * If you wish to translate some new texsts you should do two things:
 * 1. Add a new phrase pair ("New Phrase": "New Translation") into your language file (for example lang/ru.js). It will be great if you add it in other language files too.
 * 2. Mark that text it templates this way <anyHtmlTag data-sw-translate>New Phrase</anyHtmlTag> or <anyHtmlTag data-sw-translate value='New Phrase'/>.
 * The main thing here is attribute data-sw-translate. Only inner html, title-attribute and value-attribute are going to translate.
 *
 */
window.SwaggerTranslator = {

    _words:[],

    translate: function(sel) {
      var $this = this;
      sel = sel || '[data-sw-translate]';

      $(sel).each(function() {
        $(this).html($this._tryTranslate($(this).html()));

        $(this).val($this._tryTranslate($(this).val()));
        $(this).attr('title', $this._tryTranslate($(this).attr('title')));
      });
    },

    _tryTranslate: function(word) {
      return this._words[$.trim(word)] !== undefined ? this._words[$.trim(word)] : word;
    },

    learn: function(wordsMap) {
      this._words = wordsMap;
    }
};
