#!/usr/bin/env python3
"""Fail if Python async wrappers capture Python-owned pointers into worker lambdas."""
from pathlib import Path
import re
import sys

src = Path('python_interface/_pyfalconfs_internal/_pyfalconfs_internal.cpp').read_text()
bad_patterns = {
    'AsyncExists captures raw path': r'PyWrapper_AsyncExists[\s\S]*?auto task = \[path, state\]',
    'AsyncGet captures raw path/buffer': r'PyWrapper_AsyncGet[\s\S]*?auto task = \[path, buffer, size, offset, state\]',
    'AsyncPut captures raw path/buffer': r'PyWrapper_AsyncPut[\s\S]*?auto task = \[path, buffer, size, offset, state\]',
}
failed = [name for name, pattern in bad_patterns.items() if re.search(pattern, src)]
required_patterns = {
    'AsyncExists keeps AsyncState alive': r'PyWrapper_AsyncExists[\s\S]*?Py_INCREF\(\(PyObject\*\)state\);[\s\S]*?auto task',
    'AsyncGet keeps AsyncState alive': r'PyWrapper_AsyncGet[\s\S]*?Py_INCREF\(\(PyObject\*\)state\);[\s\S]*?auto task',
    'AsyncPut keeps AsyncState alive': r'PyWrapper_AsyncPut[\s\S]*?Py_INCREF\(\(PyObject\*\)state\);[\s\S]*?auto task',
}
failed.extend(name for name, pattern in required_patterns.items() if not re.search(pattern, src))
if failed:
    for name in failed:
        print(f'FAIL: {name}')
    sys.exit(1)
print('OK: async wrappers keep Python-owned path/buffer/state alive correctly')
