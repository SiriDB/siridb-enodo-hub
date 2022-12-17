import json
from string import Template
from collections import ChainMap as _ChainMap

_sentinel_dict = {}


class CTemplate(Template):
    idpattern = r'(?a:[?]?[_a-z,.][_a-z0-9,.]*)'

    def __init__(self, template):
        if template.startswith('$JSON::'):
            self._json_map = []
            pairs = [part.split('->') for part in template.split('::')[1:]]
            self._json_build_map(pairs)
        else:
            self._json_map = None
            super().__init__(template)

    def _print_key_value(self, str_val, mapping) -> str:
        value, key = str_val.split(',')
        conditional = False
        if value[0] == "?":
            conditional = True
            value = value[1:]
        if conditional and not self._get_value(mapping, value):
            return ""
        return f'"{key}": "{str(self._get_value(mapping, value))}",'

    def _get_value(self, mapping, path) -> str:
        path = path.split(".")
        curr_val = mapping
        for path_item in path:
            if hasattr(curr_val, path_item):
                curr_val = getattr(curr_val, path_item)
            elif path_item in curr_val:
                curr_val = curr_val[path_item]
            else:
                return None
        return curr_val

    def _json_build_map(self, pairs):
        for source, target in pairs:
            tp = source.startswith('`')
            if tp:
                source = json.loads(source.strip('`'))
            else:
                if source.endswith('?'):
                    tp = None  # Exclude None values
                    source = source[:-1]
                source = source.split('.')
            self._json_map.append((tp, source, target))

    @staticmethod
    def _get_val(o, name):
        val = getattr(o, name, None)
        if val is None:
            try:
                val = o[name]
            except (TypeError, KeyError):
                pass
        return val

    def _get_json_map(self, mapping):
        jmap = {}
        for tp, source, target in self._json_map:
            if tp:
                jmap[target] = source
            else:
                o = mapping
                for name in source:
                    o = self._get_val(o, name)
                if o is not None or tp is False:
                    jmap[target] = o
        return json.dumps(jmap, separators=(',', ':'))

    def safe_substitute(self, mapping=_sentinel_dict, /, **kws):
        if mapping is _sentinel_dict:
            mapping = kws
        elif kws:
            mapping = _ChainMap(kws, mapping)

        # Helper function for .sub()
        if self._json_map is not None:
            return self._get_json_map(mapping)

        def convert(mo):
            named = mo.group('named') or mo.group('braced')
            if named is not None:
                try:
                    if "," in named:
                        return self._print_key_value(named, mapping)
                    return str(self._get_value(mapping, named))
                except Exception as _:
                    return mo.group()
            if mo.group('escaped') is not None:
                return self.delimiter
            if mo.group('invalid') is not None:
                return mo.group()
            raise ValueError('Unrecognized named group in pattern',
                             self.pattern)
        return self.pattern.sub(convert, self.template)


if __name__ == '__main__':
    t = CTemplate('{${x.y}}')

    class x:
        y = 'Y!!'

    out = t.safe_substitute(x=x)
    assert out == '{Y!!}', out

    t = CTemplate('$JSON::x.y->n::`[1,2]`->o::a->a::b?->b')
    out = t.safe_substitute(x=x)

    assert json.loads(out) == {"n": "Y!!", "o": [1, 2], 'a': None}, out
