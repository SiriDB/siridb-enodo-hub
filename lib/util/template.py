from string import Template
from collections import ChainMap as _ChainMap

_sentinel_dict = {}


class CTemplate(Template):
    idpattern = r'(?a:[?]?[_a-z,.][_a-z0-9,.]*)'

    def _print_key_value(self, str_val, mapping) -> str:
        value, key = str_val.split(',')
        conditional = False
        if value[0] == "?":
            conditional = True
            value = value[1:]
        if conditional and not self._get_value(mapping, value):
            return ""
        return f'"{key}": "{self._get_value(mapping, value)}",'

    def _get_value(self, mapping, path) -> str:
        path = path.split(".")
        curr_val = mapping
        for path_item in path:
            if hasattr(curr_val, path_item):
                curr_val = getattr(curr_val, path_item)
            else:
                curr_val = curr_val[path_item]
        return str(curr_val)

    def safe_substitute(self, mapping=_sentinel_dict, /, **kws):
        if mapping is _sentinel_dict:
            mapping = kws
        elif kws:
            mapping = _ChainMap(kws, mapping)
        # Helper function for .sub()

        def convert(mo):
            named = mo.group('named') or mo.group('braced')
            if named is not None:
                try:
                    if "," in named:
                        return self._print_key_value(named, mapping)
                    return self._get_value(mapping, named)
                except Exception as _:
                    return mo.group()
            if mo.group('escaped') is not None:
                return self.delimiter
            if mo.group('invalid') is not None:
                return mo.group()
            raise ValueError('Unrecognized named group in pattern',
                             self.pattern)
        return self.pattern.sub(convert, self.template)
