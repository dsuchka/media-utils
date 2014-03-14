# coding: UTF-8
# dzheika © 2012

import optparse

options = None
def setupOptions(newOptions):
    global options
    options = newOptions

#
# extended epilog-formatter
#
class EpilogHelpFormatter(optparse.IndentedHelpFormatter):
    def format_epilog(self, epilog):
        import textwrap
        text_width = self.width - self.current_indent
        indent = " " * self.current_indent
        if not isinstance(epilog, list):
            epilog = [epilog]
        return "\n\n" + "\n".join(
            textwrap.fill(line or '', text_width,
                initial_indent=indent,
                subsequent_indent=indent,
                replace_whitespace=False,
                drop_whitespace=False)
            for line in epilog
        ) + "\n\n"

def setupVerboseOptions(parser, useLevel=True, withVerbose=True, withQuiet=True, withTotally=False):
    opts = []
    dest = "verbose"
    if useLevel:
        dest+="_level"
        opts.append(parser.add_option("", "--verbose-level", metavar="LVL",
            dest=dest, action="store", type="int", default=0,
            help="specify verbose level [quiet(-) << normal(0) << verbose(+)]"
                " [default: %default]"))
    if withVerbose:
        opts.append(parser.add_option("-v", "--verbose",
            dest=dest, action="store_const", const=(1 if useLevel else True),
            help="enable verbose output"))
    if withQuiet:
        opts.append(parser.add_option("-q", "--quiet",
            dest=dest, action="store_const", const=(-1 if useLevel else False),
            help="disable output (messages, notices)"))
    if withVerbose and withTotally:
        xdest = (dest if useLevel else (dest+"_totally"))
        opts.append(parser.add_option("", "--totally-verbose",
            dest=xdest, action="store_const", const=(2 if useLevel else True),
            help="enable very (totally) verbose output"))
    if withQuiet and withTotally:
        xdest = (dest if useLevel else (dest+"_totally"))
        opts.append(parser.add_option("", "--totally-quiet",
            dest=xdest, action="store_const", const=(-2 if useLevel else True),
            help="disable any output (messages, notices, warnings, errors)"))
    return opts

def setupContinueOptions(parser, alwaysShort="-Y", neverShort="-N", dest="cont"):
    opts = []
    opts.append(parser.add_option(alwaysShort, "--always-continue",
        action="store_true", dest=dest,
        help="do not ask user interactively, on errors always continue"))
    opts.append(parser.add_option(neverShort, "--never-continue",
        action="store_false", dest=dest,
        help="do not ask user interactively, on errors always exit"))
    return opts

def setupConfigOptions(parser, default, withNoConfig=True, noConfig="/dev/null"):
    opts = []
    opts.append(parser.add_option("-c", "--config", metavar='FILE',
        action="store", default=default,
        help="load settings from confing file [default: %default]"))
    if withNoConfig:
        opts.append(parser.add_option("-n", "--no-config", dest="config",
            action="store_const", const=noConfig,
            help="don't load settings from config file"))
    return opts

def _get_options(options=None):
    if options is not None:
        return options
    if globals().has_key('options'):
        return globals()['options']
    return None

def isVerbose(options=None, totally=False):
    options = _get_options(options)
    if not options:
        return False
    if hasattr(options, 'verbose'):
        return options.verbose == True
    if hasattr(options, 'verbose_level'):
        return options.verbose_level > (1 if totally else 0)
    if hasattr(options, 'verbose_totally') and totally:
        return options.verbose_totally == True
    return False

def isQuiet(options=None, totally=False):
    options = _get_options(options)
    if not options:
        return False
    if hasattr(options, 'verbose'):
        return options.verbose == False
    if hasattr(options, 'verbose_level'):
        return options.verbose_level < (-1 if totally else 0)
    if hasattr(options, 'verbose_totally') and totally:
        return options.verbose_totally == False
    return False

def getContinueAutoanswer(options=None, destName='cont'):
    options = _get_options(options)
    if not options:
        return None
    if hasattr(options, destName):
        cont = getattr(options, destName)
        if cont is not None:
            return ('yes' if cont else 'no')
    return None

def loadConfigOptions(parser, configFileOpts, options=None):
    options = _get_options(options)
    if not options or not hasattr(options, 'config'):
        return options
    import os
    path = os.path.expanduser(options.config)
    if path == '/dev/null':
        return options
    from dzh.interactive import showMesg, showMesgIfVerbose, showWarn, showErr
    if not os.path.exists(path):
        if isVerbose(options=options):
            showWarn('no such config file: ' + path)
        return options
    showMesgIfVerbose('try to load settings from config file: ' + path)
    vals = optparse.Values()
    try:
        with open(path) as f:
            arg2opt = {}
            for opt in configFileOpts:
                for lopt in opt._long_opts:
                    xopt = lopt[2:]
                    arg2opt[xopt] = opt
            lineno = 0
            def warnOnLine(text):
                showWarn('config(%s) at line %d: %s', (path, lineno, text))
            for line in f:
                line = line.strip()
                lineno += 1
                if not line or line.startswith('#'):
                    # a comment
                    continue
                pair = line.split('=', 1)
                arg = pair[0].strip()
                opt = arg2opt.get(arg, None)
                if not opt:
                    warnOnLine('unknown option: ' + arg)
                    continue
                if opt.action in opt.ALWAYS_TYPED_ACTIONS and len(pair) != 2:
                    warnOnLine('option(%s) requires an parameter for option: %s', (opt, arg))
                    continue
                val = pair[1].strip() if len(pair) == 2 else None
                if val and val[0] == val[-1] and val[0] in "\'\"":
                    val = val[1:-1]
                opt.process(opt, val, vals, parser)
            # end: for line in f
        # end: with open(...) as f
        def showValsIfVerbose(info):
            if isVerbose():
                showMesg(info)
                for k, v in vals.__dict__.items():
                    showMesg(' >> %s: %s', (k, v))
        showValsIfVerbose('config settings:')
        parser.parse_args(values=vals)
        for k, v in options.__dict__.items():
            if not hasattr(vals, k):
                setattr(vals, k, v)
        showValsIfVerbose('final settings:')
        return vals
    except Exception, e:
        showErr(str(e))
    return options
