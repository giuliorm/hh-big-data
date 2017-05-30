from pymorphy2 import  *
from collections import defaultdict
from abc import *
import re

class TextHandler:

    _grammems = ['NOUN', 'ADJF', 'ADJS', 'COMP', 'VERB', 'INFN', 'PRTF', 'PRTS', 'GRND', 'ADVB']
    _analyzer = None
    _queue = None
    _symbols = [',','.','(',')','?',':',';', '"']
    _info_cache  = {}
    _stop = False
    _document_model = None

    def __init__(self, morph_analyzer):
        self._analyzer = morph_analyzer

    def  handle (self, words):
        n_words = []
        for word in words:
            if not self._useless(word) and not self._useless_pos(word):
                new_word = self._normalize(word)
                if new_word != '':
                    n_words.append(new_word)

        return n_words

    @property
    def document_model(self):
        return self._document_model

    @document_model.setter
    def document_model(self, value):
        self._document_model = value

    @property
    def stop(self):
        return self._stop

    @stop.setter
    def stop(self, value):
        self._stop  = value

    def _useless(self, word):
        return word in self._symbols

    def _useless_pos(self, word):
        if word not in self._info_cache.keys():
            self._info_cache[word] = self._analyzer.parse(word)[0]
        info = self._info_cache[word]
        tag = str (info.tag)
        tag =  re.sub(r',\.\(\)\?:; "', r',', tag).split(',')[0]
        return tag not in self._grammems

    def _normalize(self, word):
        info = self._analyzer.parse(word)[0]
        return info.normal_form