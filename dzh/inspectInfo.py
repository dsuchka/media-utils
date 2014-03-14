# coding: UTF-8
# dzheika © 2012

import inspect

def currentMethodName(withArgs=True):
    fcode = inspect.stack()[1][0].f_code
    if withArgs:
        return "%s(%s)" % (fcode.co_name,
            ', '.join(fcode.co_varnames[0:fcode.co_argcount]))
    return fcode.co_name
