#!/usr/bin/python2 -O
# coding: UTF-8
# dzheika © 2012


import os, sys, re, time, errno
import optparse
import shlex, subprocess, multiprocessing
import thread, threading

import mutagen

from dzh.utils import *
from dzh.inspectInfo import *
from dzh.interactive import *
from dzh.optParseExt import *

#
## create option parser
#
parser = optparse.OptionParser(
    usage="Usage: %prog [options] playlist-1 ... playlist-n",
    formatter=EpilogHelpFormatter(),
    epilog=["  ** Configuration example **",
            "",
            "verbose",
            "src-root=~/music",
            "dst-root=/media/MyDevice/music",
            "clone-policy=DELETE",
            "output-format=mp3",
            "replace-fat",
            "",
            "# decoders:",
            "decoder=flac: flac --decode --silent --stdout {src}",
            "decoder=mp3: lame --quiet --decode {src} -",
            "",
            "# encoders:",
            "encoder=mp3: lame --quiet --vbr-new -b 96 -B 160 - {dst}",
            "",
            "# keep lossy:",
            "keep-lossy=mp3:170",
    ]
)

#
## determine and define vars that used as defaults in options
#

try:
    proc_count = multiprocessing.cpu_count()
except:
    proc_count = 1
finally:
    proc_count += 1

#
# configure options
#
configFileOpts = []

setupConfigOptions(parser, '~/.lossync.rc')

configFileOpts.append(parser.add_option("-s", "--src-root", metavar='ROOT',
    action="store",
    help="specify the source root"))

configFileOpts.append(parser.add_option("-d", "--dst-root", metavar='ROOT',
    action="store",
    help="specify the destination root"))

configFileOpts.append(parser.add_option("-o", "--output-format", metavar='FMT',
    action="store",
    help="specify the output file format (must be defined as encoder/recoder)"))

configFileOpts.append(parser.add_option("--delete",
    action="store_true",
    help="delete outdated files from destination root"))

configFileOpts.append(parser.add_option("--protect", metavar='REGEX',
    action="append", default=[],
    help="protect files from deleting (python regex, will check full path)"))

configFileOpts.append(parser.add_option("--dry-run",
    action="store_true",
    help="perform a trial run with no changes made"))

configFileOpts.append(parser.add_option("--without-tags",
    action="store_true",
    help="don't copy tags"))

#
## group: coders
#
group = optparse.OptionGroup(parser, "Encoding customization Options",
    "Note: The PROG is the programm (with arguments), the {src} and {dst} keywords"
    " will be replaced by source/destination file path."
    " The EXT is the file extension."
    " Each of options can be defined multiple times with different extensions."
    " The Recoder have a prior.")

configFileOpts.append(group.add_option("--recoder", metavar='SRC-EXT:DST-EXT:PROG',
    action="append", default=[],
    help="associate the recoder(PROG) with the file extension(SRC-EXT/DST-EXT)"
        ", {src} and {dst} both are mandatory"))

configFileOpts.append(group.add_option("--decoder", metavar='EXT:PROG',
    action="append", default=[],
    help="associate the decoder(PROG) with the file extension(EXT), {src} is mandatory"))

configFileOpts.append(group.add_option("--encoder", metavar='EXT:PROG',
    action="append", default=[],
    help="associate the encoder(PROG) with the file extension(EXT), {dst} is mandatory"))

parser.add_option_group(group)


#
## group: sync-tune
#
group = optparse.OptionGroup(parser, "Synchronization tuning Options","")

configFileOpts.append(group.add_option("--keep-lossy", metavar='EXT:[[min_bitrate-]max_bitrate]',
    action="append", default=[],
    help="keep the source lossy files, multiple keep-lossy options are allowed"))

configFileOpts.append(group.add_option("--replace-from", metavar='KEY:REGEX',
    action="append", default=[],
    help="replace characters in destination names: specify the pattern (python regex)"
        " and associate with the KEY,"
        " multiple replace options with different KEYs are allowed"))

configFileOpts.append(group.add_option("--replace-to", metavar='KEY:TEXT',
    action="append", default=[],
    help="replace characters in destination names: specify the text for each KEY"
        " declared by --replace-from option"))

configFileOpts.append(group.add_option("--replace-fat",
    action="store_true", default=False,
    help="replace an illegal FAT filesystem characters by _ (underline) in destination names"))

configFileOpts.append(group.add_option("-t", "--tmp-suffix",  metavar='SUFFIX',
    action="store", default="~",
    help="suffix of temporaty (incompleted) files [default: %default]"))

configFileOpts.append(group.add_option("-T", "--threads", metavar='NUM',
    action="store", type="int", default=proc_count,
    help="threads count [default: %default]"))

configFileOpts.append(group.add_option("", "--clone-policy", metavar='POLICY',
    action="store", choices=['IGNORE', 'KEEP', 'DELETE'], default='IGNORE',
    help="specify the policy for resoloving clones "
        "(dest files with other extension) [default: %default]"))

parser.add_option_group(group)




configFileOpts.extend(setupVerboseOptions(parser, withTotally=True))
configFileOpts.extend(setupContinueOptions(parser))

##
## parse args
##
(options, args) = parser.parse_args()
options = loadConfigOptions(parser, configFileOpts, options)
setupOptions(options)

#
## check mandatory settings
#
if not args:
    showErr('no playlists passed')

if not options.output_format:
    showErr('no output format specified')

if not options.src_root:
    showErr('source root not specified')

options.src_root = os.path.expanduser(options.src_root)
if not os.path.exists(options.src_root):
    showErr('source root does not exist: %s', (options.src_root,))

if not options.dst_root:
    showErr('destination root not specified')

options.dst_root = os.path.expanduser(options.dst_root)
if not os.path.exists(options.dst_root):
    showErr('destination root does not exist: %s', (options.dst_root,))

#
## prepare (compile) settings
#
vals = options.protect
options.protect = []
for val in vals:
    try:
        val = re.compile(val)
        options.protect.append(val)
    except Exception, e:
        showErrAsk('Invalid protect regex: %s', (e,))

class KeepLossy:
    FMT = re.compile(r'^(?P<ext>.+?)(?::(?:(?P<from>\d+)-)?(?P<to>\d+))?$')
    def __init__(self, fmt):
        m = self.FMT.match(fmt)
        if not m:
            raise Exception("incorrect format: " + fmt)
        self.ext = m.group('ext')
        self.fr = m.group('from')
        self.to = m.group('to')
        try:
            if self.fr:
                self.fr = int(self.fr) * 1000
            if self.to:
                self.to = int(self.to) * 1000
        except ValueError, e:
            raise Exception("incorrect format: bad from/to: " + fmt)
    def isKeep(self, syncFile):
        if self.ext.lower() != syncFile.fmt:
            return False
        if self.fr and self.fr > syncFile.bitrate:
            return False
        if self.to and self.to < syncFile.bitrate:
            return False
        return True

vals = options.keep_lossy
options.keep_lossy = []
for val in vals:
    try:
        val = KeepLossy(val)
        options.keep_lossy.append(val)
    except Exception, e:
        showErrAsk('Invalid keep-lossy: %s', (e,))

fmt = re.compile('^(?P<ext>.+?):(?P<prog>.+)$')
for (opt, chk) in (('decoder', '{src}'), ('encoder', '{dst}')):
    dct = {}
    vals = getattr(options, opt)
    setattr(options, opt, dct)
    for val in vals:
        m = fmt.match(val)
        if not m:
            showErrAsk('Invalid %s: %s', (opt, val))
            continue
        ext, prog = m.group('ext', 'prog')
        if dct.has_key(ext):
            showErrAsk('Invalid %s: %s: such EXT already defined', (opt, val))
            continue
        prog_args = shlex.split(prog)
        if chk not in prog_args:
            showErrAsk('Invalid %s: %s: missing %s', (opt, val, chk))
            continue
        dct[ext] = prog_args

fmt = re.compile('^(?P<sext>.+?):(?P<dext>.+?):(?P<prog>.+)$')
dct = {}
vals = options.recoder
options.recoder = dct
for val in vals:
    m = fmt.match(val)
    if not m:
        showErrAsk('Invalid recoder: %s', (val,))
        continue
    src_ext, dst_ext, prog = m.group('sext', 'dext', 'prog')
    key = (src_ext, dst_ext)
    if dct.has_key(key):
        showErrAsk('Invalid recoder: %s: such SRC-EXT/DST-EXT pair already defined', (val,))
        continue
    prog_args = shlex.split(prog)
    fail = False
    for chk in ('{src}', '{dst}'):
        if chk not in prog_args:
            showErrAsk('Invalid %s: %s: missing %s', (opt, val, chk))
            fail = True
            break
    if not fail:
        dct[key] = prog_args

fmt = re.compile('^(?P<key>.+?):(?P<data>.+)$')
dct = {}
vals = options.replace_from
options.replace_from = dct
for val in vals:
    m = fmt.match(val)
    if not m:
        showErrAsk('Invalid replace-from: %s', (val,))
        continue
    key, regex = m.group('key', 'data')
    if dct.has_key(key):
        showErrAsk('Invalid replace-from: %s: such key already defined', (val,))
        continue
    try:
        regex = re.compile(regex)
    except:
        showErrAsk('Invalid replace-from regex: (key=%s) %s', (key, regex))
        continue
    dct[key] = regex

dct = {}
vals = options.replace_to
options.replace_to = dct
for val in vals:
    m = fmt.match(val)
    if not m:
        showErrAsk('Invalid replace-to: %s', (val,))
        continue
    key, text = m.group('key', 'data')
    if dct.has_key(key):
        showErrAsk('Invalid replace-to: %s: such key already defined', (val,))
        continue
    if not options.replace_from.has_key(key):
        showErrAsk('Illegal replace-to: %s: no such key exists (replace-from)', (val,))
        continue
    dct[key] = text

vals = options.replace_from.items()
for (k, v) in vals:
    if options.replace_to.has_key(k):
        continue
    showErrAsk('Incomplete replace-from: %s:%s: key not mapped (replace-to)', (k, v.pattern))
    del options.replace_from[k]

#
## collect files
#
class SyncFile:
    def __init__(self, path):
        self.path = path
        self.dir = os.path.dirname(path)
        self.name = os.path.basename(path)
        self.size = os.path.getsize(path)
        f = mutagen.File(path)
        if f and isinstance(f.info, mutagen.oggvorbis.OggVorbisInfo):
            self.fmt = 'ogg'
            self.bitrate = f.info.bitrate
        elif f and isinstance(f.info, mutagen.mp3.MPEGInfo):
            self.fmt = 'mp3'
            self.bitrate = f.info.bitrate
        elif f and isinstance(f.info, mutagen.flac.StreamInfo):
            self.fmt = 'flac'
            self.bitrate = -1
        else:
            idx = self.name.rfind('.')
            if idx > 0:
                self.fmt = self.name[idx+1:].lower()
            else:
                self.fmt = None
            if not self.fmt:
                raise Exception("Can't detect file type: " + path)
            try:
                self.bitrate = f.info.bitrate
            except:
                self.bitrate = -1
        if self.name.lower().endswith('.' + self.fmt):
            self.name = self.name[:-(len(self.fmt) + 1)]
    def isLossy(self):
        return self.bitrate >= 0

syncFiles = {}
totalSize = 0

for pl in args:
    try:
        with open(pl) as f:
            files = f.readlines()
    except Exception, e:
        showErrAsk("Can't read playlist: %s", (e,))
        continue
    for f in files:
        f = f.rstrip()
        try:
            sf = SyncFile(f)
            if not syncFiles.has_key(f):
                syncFiles[f] = sf
                totalSize += sf.size
            else:
                showWarn("already cached: %s", (f,))
        except Exception, e:
            showErrAsk("Bad mediafile: %s", (e,))
            continue
    # end for (f)
# end for (pl)

if isVerbose():
    showMesg(" ** total files: %d", (len(syncFiles),))
    showMesg(" ** total size (source): %s", (HumanReadableSize(totalSize),))

#
## do sync
#

class SubprocessFaieldError(Exception):
    def __init__(self, name, ecode, output=''):
        self.name = name
        self.ecode = ecode
        self.output = output
    def __str__(self):
        return "%s failed (exit code: %d)" % (self.name, self.ecode)

class SyncTask(object):
    def __init__(self, src, dst_path):
        self.src = src
        self.dst_path = dst_path
        self.do_stop = False
        self._tagger = None
    def doSync(self):
        if options.dry_run:
            return
        srcPath = self.src.path
        dstPath = self.dst_path
        tmp = self.dst_path + options.tmp_suffix
        try:
            try:
                os.makedirs(os.path.dirname(tmp))
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise e
            self.doSync0(srcPath, tmp)
            if tmp != dstPath:
                os.rename(tmp, dstPath)
        finally:
            if os.path.exists(tmp) and tmp != dstPath:
                os.remove(tmp)

        # tagging is necessary ?
        if os.path.exists(self.dst_path) and self.isNeedTagging() and not options.without_tags:
            done = False
            try:
                self.checkForceStop()

                # Tagger
                self._tagger = subprocess.Popen(
                    ['tagger',
                        '--no-config', '--never-continue', 
                        ('--totally-verbose' if isVerbose(totally=True) else '--quiet'),
                        '--from-file', srcPath, dstPath],
                    stdin=file(os.devnull, 'rb'),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT)
             
                # Validate tagger
                outTagger = self._tagger.communicate()[0]
                retTagger = self._tagger.wait()
                if retTagger != 0:
                    raise SubprocessFaieldError('tagger', retTagger, outTagger)

                done = True
            finally:
                exc = None
                if not done:
                    try:
                        os.remove(self.dst_path)
                    except Exception, e:
                        exc = e
                self.cleanupSubproc(done, self._tagger)
                if exc is not None:
                    raise exc
        return
    @classmethod
    def doSync0(self, srcPath, dstPath):
        raise NotImplementedError("%s.%s" % \
            (cls.__name__, currentMethodName()))
    @classmethod
    def isNeedTagging(self):
        raise NotImplementedError("%s.%s" % \
            (cls.__name__, currentMethodName()))
    def __str__(self):
        return "%s(src=%s, dst=%s)" % (self.__class__.__name__,
            self.src.path, self.dst_path)
    def stop(self):
        self.do_stop = True
        self.killSubproc(self._tagger)
        self.waitSubproc(self._tagger)
    @staticmethod
    def killSubproc(*procs):
        for p in filter(lambda x: x is not None, procs):
            try:
                p.kill()
            except:
                pass
        return
    @staticmethod
    def waitSubproc(*procs):
        for p in filter(lambda x: x is not None, procs):
            p.wait()
    def checkForceStop(self):
        if self.do_stop:
            raise Exception("force stopping")
    def cleanupSubproc(self, done, *procs):
        if not done:
            self.killSubproc(*procs)
        self.waitSubproc(*procs)
        self.checkForceStop()
    @staticmethod
    def replace(prog, srcPath, dstPath):
        return [x.replace('{src}', srcPath).replace('{dst}', dstPath)
                for x in prog]

class CopyTask(SyncTask):
    def doSync0(self, srcPath, dstPath):
        bs = 1024*16
        data = None
        with open(srcPath, 'rb') as src_f, \
             open(dstPath, 'wb') as dst_f:
            while (data is None) or (len(data) == bs):
                self.checkForceStop()
                data = src_f.read(bs)
                dst_f.write(data)
        return
    @staticmethod
    def isNeedTagging():
        return False

class RecodeTask(SyncTask):
    def __init__(self, src, dst_path, recoder):
        super(RecodeTask, self).__init__(src, dst_path)
        self.recoder = recoder
        self._recoder = None
    def doSync0(self, srcPath, dstPath):
        done = False
        try:
            self.checkForceStop()

            # Recoder
            progRecoder = self.replace(self.recoder, srcPath, dstPath)
            self._recoder = subprocess.Popen(progRecoder,
                stdin=file(os.devnull, 'rb'),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)
            
            # Validate tagger
            outRecoder = self._recoder.communicate()[0]
            retRecoder = self._recoder.wait()
            if retRecoder != 0:
                raise SubprocessFaieldError('recoder', retRecoder, outRecoder)

            done = True
        finally:
            self.cleanupSubproc(done, self._recoder)
    @staticmethod
    def isNeedTagging():
        return True
    def stop(self):
        super(RecodeTask, self).stop()
        self.killSubproc(self._recoder)
        self.waitSubproc(self._recoder)
        return

class EncodeTask(SyncTask):
    def __init__(self, src, dst_path, decoder, encoder):
        super(EncodeTask, self).__init__(src, dst_path)
        self.decoder = decoder
        self.encoder = encoder
        self._decoder = None
        self._encoder = None
    def doSync0(self, srcPath, dstPath):
        done = False
        try:
            self.checkForceStop()
            
            # Decoder
            progDecoder = self.replace(self.decoder, srcPath, dstPath)
            self._decoder = subprocess.Popen(progDecoder,
                stdin=file(os.devnull, 'rb'),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

            # Encoder
            progEncoder = self.replace(self.encoder, srcPath, dstPath)
            self._encoder = subprocess.Popen(progEncoder,
                stdin=self._decoder.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)

            # Allow Decoder to receive a SIGPIPE if Encoder exits
            self._decoder.stdout.close()

            # Validate decoder/encoder
            outDecoder = self._decoder.stderr.read()
            retDecoder = self._decoder.wait()
            if retDecoder != 0:
                raise SubprocessFaieldError('decoder', retDecoder, outDecoder)
            outEncoder = self._encoder.stdout.read()
            retEncoder = self._encoder.wait()
            if retEncoder != 0:
                raise SubprocessFaieldError('encoder', retEncoder, outEncoder)

            done = True
        finally:
            self.cleanupSubproc(done, self._decoder, self._encoder)
    @staticmethod
    def isNeedTagging():
        return True
    def stop(self):
        super(EncodeTask, self).stop()
        self.killSubproc(self._decoder, self._encoder)
        self.waitSubproc(self._decoder, self._encoder)
        return


def processClone(clonePath):
    if options.clone_policy == 'DELETE':
        showMesgIfNotQuiet(' ** deleting clone: %s', (clonePath,))
        try:
            if not options.dry_run:
                os.remove(clonePath)
        except Exception, e:
            showErrAsk('can not delete clone(%s): %s', (clonePath, e))
        return True
    if options.clone_policy == 'KEEP':
        showMesgIfNotQuiet(' ** keeping clone: %s', (clonePath,))
        return False
    showMesgIfNotQuiet(' ** ignoring clone: %s', (clonePath,))
    return True

tasks = {}

for src in syncFiles.values():
    try:
        showMesgIfVerbose("analyzing file: %s (%s)", (src.path, HumanReadableSize(src.size)), totally=True)
        xdir = src.dir
        if xdir.startswith(options.src_root):
            xdir = xdir[len(options.src_root):].lstrip(os.path.sep)
        dst_dir = os.path.join(options.dst_root, xdir)
        dst_base_path = os.path.join(dst_dir, src.name)
        dst_path = dst_base_path + '.' + options.output_format

        # fix name by replacing defined by user
        for key, regex in options.replace_from.items():
            text = options.replace_to[key]
            dst_path = regex.sub(text, dst_path)

        # fix fat illegal chars
        if options.replace_fat:
            dst_path = re.sub('[?<>\\\\:*|"\\x01-\\x1f]', '_', dst_path)

        del dst_base_path
        dst_dir = os.path.dirname(dst_path)
        dst_name = os.path.basename(dst_path)[:-(1+len(options.output_format))]

        do_ignore = False
        if os.path.exists(dst_path):
            showMesgIfVerbose('already exists: %s', (dst_path,), totally=True)
            do_ignore = True
        elif os.path.exists(dst_dir):
            check_name = dst_name + '.'
            for xname in os.listdir(dst_dir):
                if not xname.startswith(check_name):
                    continue
                xpath = os.path.join(dst_dir, xname)
                if not os.path.isfile(xpath):
                    continue
                if not processClone(xpath):
                    do_ignore = True
                    break
        if do_ignore:
            totalSize -= src.size
            # mark as task to protect from deleting
            if options.delete:
                tasks[dst_path] = None
            continue

        if tasks.has_key(dst_path):
            if not showWarnAsk("such destination path(%s) already registered for source(%s), "
                                "will be overrided for new source(%s)",
                                (dst_path, tasks[dst_path].src.path, src.path)):
                continue

        do_keep = False
        if src.isLossy():
            for keep in options.keep_lossy:
                if keep.isKeep(src):
                    do_keep = True
                    break
        if do_keep:
            showMesgIfVerbose("will copy as is (keep-lossy): %s", (src.path,), totally=True)
            tasks[dst_path] = CopyTask(src, dst_path)
            continue
        
        key = (src.fmt, options.output_format)
        recoder = options.recoder.get(key, None)
        if recoder is not None:
            showMesgIfVerbose("will use recoder (%s => %s): %s", key + (src.path,), totally=True)
            tasks[dst_path] = RecodeTask(src, dst_path, recoder)
            continue

        key = src.fmt
        decoder = options.decoder.get(key, None)
        if decoder is None:
            showErrAsk("unsupported source format (no decoder specified): %s", (src.path,))
            continue

        key = options.output_format
        encoder = options.encoder.get(key, None)
        if encoder is None:
            showErrAsk("unsupported output format (no encoder specified): %s", (src.path,))
            continue

        tasks[dst_path] = EncodeTask(src, dst_path, decoder, encoder)
    except Exception, e:
        if isVerbose(totally=True):
            import traceback
            traceback.print_exc()
        showErrAsk("analyzing file=%s: %s", (src.path, e))

toDelete = []
if options.delete:
    for dirname, dirnames, filenames in os.walk(options.dst_root):
        for fname in filenames:
            xpath = os.path.join(dirname, fname)
            if not tasks.has_key(xpath):
                doDelete = True
                if options.protect:
                    for regex in options.protect:
                        if regex.match(xpath):
                            doDelete = False
                            break
                if doDelete:
                    toDelete.append(xpath)
    # cleanup protected
    tasks = dict(filter(lambda (x, y): y is not None, tasks.items()))

showMesgIfVerbose(" ** total to delete: %d", (len(toDelete),))
for xpath in toDelete:
    showMesgIfNotQuiet("deleting: %s", (xpath,))
    if not options.dry_run:
        os.remove(xpath)

totalTasks = len(tasks)
showMesgIfVerbose(" ** total tasks: %d", (totalTasks,))

#
## do tasks in parallel threads
#

class TasksQueue:
    def __init__(self, tasks):
        self.tasks = tasks
        self.lock = thread.allocate_lock()
    def nextTask(self):
        with self.lock:
            if self.tasks:
                return self.tasks.pop(0)
        return None
    def __iter__(self):
        while True:
            task = self.nextTask()
            if task is None:
                break
            yield task
        return

tasks = TasksQueue(tasks.values())


class TaskExecutor(threading.Thread):

    outputLock = thread.allocate_lock()
    doneSize = 0
    doneCount = 0
    stopTasks = False
    activeTasks = {}

    def __init__(self, name, tasks):
        super(TaskExecutor, self).__init__()
        self.tasks = tasks
        self.setName(name)

    def run(self):
        outputLock = self.outputLock
        name = self.getName()
        for task in self.tasks:
            try:
                with outputLock:
                    if self.stopTasks:
                        showMesgIfNotQuiet("[%s] force stopping task executing", (name,))
                        break
                    showMesgIfVerbose("[%s] next task: %s", (name, task))
                    self.__class__.activeTasks[name] = task
                task.doSync()
            except Exception, e:
                with outputLock:
                    showErr("[%s] task %s failed: %s", (name, task, e), exit=False)
                    if isinstance(e, SubprocessFaieldError):
                        if e.output and isVerbose():
                            showMesg(" >> %s output <<\n%s\n >> end of output <<",
                                (e.name, e.output.rstrip('\n'),))
                    if isVerbose(totally=True):
                        import traceback
                        traceback.print_exc()
                    if not self.stopTasks and not askContinue(exitWhenAnswerIs=None):
                        self.__class__.stopTasks=True
                        for xname, xtask in self.activeTasks.items():
                            if xname != name:
                                xtask.stop()
                        break
            finally:
                with outputLock:
                    if self.__class__.activeTasks.has_key(name):
                        del self.__class__.activeTasks[name]
                    self.__class__.doneSize += task.src.size
                    self.__class__.doneCount += 1
                    if not self.stopTasks and isVerbose():
                        showMesg(" ** [%s] processed: %d of %d (%.2f%%)", (
                            time.strftime('%F %H:%M:%S', time.localtime()),
                            self.doneCount, totalTasks, 
                            (100. * self.doneSize/totalSize))
                        )
                # end with
            # end try
        return

if options.threads < 1:
    showWarn("bad threads count: %d (will autocorrect to 1 as min)", (options.threads,))
    options.threads = 1
if options.threads > totalTasks:
    options.threads = totalTasks

runners = [TaskExecutor("Thread-%d" % (idx+1), tasks) for idx in xrange(0, options.threads)]
for runner in runners:
    runner.start()
for runner in runners:
    runner.join()
