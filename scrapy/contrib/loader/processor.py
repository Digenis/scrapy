"""
This module provides some commonly used processors for Item Loaders.

See documentation in docs/topics/loaders.rst
"""

from scrapy.utils.misc import arg_to_iter
from scrapy.utils.datatypes import MergeDict
from .common import wrap_loader_context

class MapCompose(object):

    def __init__(self, *functions, **default_loader_context):
        self.functions = functions
        self.default_loader_context = default_loader_context

    def __call__(self, value, loader_context=None):
        values = arg_to_iter(value)
        if loader_context:
            context = MergeDict(loader_context, self.default_loader_context)
        else:
            context = self.default_loader_context
        wrapped_funcs = [wrap_loader_context(f, context) for f in self.functions]
        for func in wrapped_funcs:
            next_values = []
            for v in values:
                next_values += arg_to_iter(func(v))
            values = next_values
        return values


class Compose(object):

    def __init__(self, *functions, **default_loader_context):
        self.functions = functions
        self.stop_on_none = default_loader_context.get('stop_on_none', True)
        self.default_loader_context = default_loader_context

    def __call__(self, value, loader_context=None):
        if loader_context:
            context = MergeDict(loader_context, self.default_loader_context)
        else:
            context = self.default_loader_context
        wrapped_funcs = [wrap_loader_context(f, context) for f in self.functions]
        for func in wrapped_funcs:
            if value is None and self.stop_on_none:
                break
            value = func(value)
        return value


class Filter(object):
    '''
    >>> Filter()(['A', 0, '', 0.0, None, -1])
    ('A', 0, 0.0, -1)
    >>> Filter(None)(['A', 0, '', 0.00, None, -1])
    ('A', -1)
    >>> Filter(lambda s: len(str(s)) > 1)(['A', 0, '', 0.00, None, -1])
    (0.0, None, -1)
    '''

    def __init__(self, function=lambda v: v is not None and v != ''):
        self.function = function

    def __call__(self, values):
        return tuple(filter(self.function, values))


class Slice(object):
    '''
    >>> Slice()([2, 3, 5, 7])
    [2, 3, 5, 7]
    >>> Slice(None, 3)([2, 3, 5, 7])
    [2, 3, 5]
    '''

    def __init__(self, begin=None, end=None):
        self.begin, self.end = begin, end

    def __call__(self, values):
        return values[self.begin:self.end]


class TakeFirst(object):
    '''
    >>> [TakeFirst()(c) for c in ((0,'A'), ('', 0), (None, 'A'))]
    [0, 0, 'A']
    >>> [TakeFirst(None)(c) for c in ((0,'A'), ('', 0), (None, 'A'))]
    ['A', None, 'A']
    >>> [TakeFirst(lambda s: len(str(s)) > 1)(c) for c in ((0,'A'), ('', 0), (None, 'A'))]
    [None, None, None]
    '''

    def __init__(self, function=lambda v: v is not None and v != ''):
        self.function = bool if function is None else function

    def __call__(self, values):
        for value in values:
            if self.function(value):
                return value


class Identity(object):

    def __call__(self, values):
        return values


class Join(object):

    def __init__(self, separator=u' '):
        self.separator = separator

    def __call__(self, values):
        return self.separator.join(values)
