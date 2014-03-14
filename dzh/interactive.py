# coding: UTF-8
# dzheika © 2012

import sys
from dzh.optParseExt import isVerbose, isQuiet, getContinueAutoanswer

def doExit(exitCode=0):
    sys.exit(exitCode)

def showMesg(fmt, fmtArgs=(), out=sys.stderr):
    if isQuiet(totally=True):
        return
    if fmtArgs:
        fmt = fmt % fmtArgs
    if isinstance(fmt, unicode):
        fmt = fmt.encode("UTF-8")
    print >> out, fmt

def showWarn(fmt, fmtArgs=()):
    showMesg("WARN: " + fmt, fmtArgs=fmtArgs)

def showErr(fmt, fmtArgs=(), exit=True, exitCode=1):
    showMesg("ERROR: " + fmt, fmtArgs=fmtArgs)
    if exit:
        doExit(exitCode)

def showMesgIfVerbose(fmt, fmtArgs=(), totally=False, out=sys.stderr):
    if isVerbose(totally=totally):
        showMesg(fmt, fmtArgs=fmtArgs, out=out)

def showMesgIfNotQuiet(fmt, fmtArgs=(), totally=False, out=sys.stderr):
    if not isQuiet(totally=totally):
        showMesg(fmt, fmtArgs=fmtArgs, out=out)

def askYesNo(quest, default=False, exitWhenAnswerIs=None, exitOnInterrupt=True, exitCode=2, autoAnswer=None):
    if autoAnswer is None and isQuiet(totally=True):
        autoAnswer = (False if (default is None) else default)
    text = quest + ': yes/no?'
    if default is not None:
        text += ' [' + (default and 'y' or 'n') + ']'
    text += ': '
    result = None
    while result is None:
        if autoAnswer is not None:
            data = autoAnswer
            showMesg("%s%s (auto answer)", (text, autoAnswer))
        else:
            try:
                data = raw_input(text).lower()
            except KeyboardInterrupt:
                showMesg("no (interrupted by used)")
                if exitOnInterrupt:
                    doExit(exitCode)
                data = 'no'
        if not data and default is not None:
            result = default
        elif 'yes'.startswith(data):
            result = True
        elif 'no'.startswith(data):
            result = False
    if exitWhenAnswerIs is not None and exitWhenAnswerIs == result:
        doExit(exitCode)
    return result

def askContinue(prefix='', default=False, exitWhenAnswerIs=False, autoAnswer=None):
    if autoAnswer is None:
        autoAnswer = getContinueAutoanswer()
    return askYesNo(prefix + 'continue', default=default,
        exitWhenAnswerIs=exitWhenAnswerIs, exitCode=3,
        autoAnswer=autoAnswer)

def showWarnAsk(fmt, fmtArgs=(), continuePrefix='', continueDefault=True, exitWhenAnswerIs=False, autoAnswer=None):
    showWarn(fmt, fmtArgs=fmtArgs)
    return askContinue(prefix=continuePrefix,
        default=continueDefault,
        exitWhenAnswerIs=exitWhenAnswerIs,
        autoAnswer=autoAnswer)

def showErrAsk(fmt, fmtArgs=(), continuePrefix='', continueDefault=False, exitWhenAnswerIs=False, autoAnswer=None):
    showErr(fmt, fmtArgs=fmtArgs, exit=False)
    return askContinue(prefix=continuePrefix,
        default=continueDefault,
        exitWhenAnswerIs=exitWhenAnswerIs,
        autoAnswer=autoAnswer)
