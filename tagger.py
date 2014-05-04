#!/usr/bin/python2 -O
# coding: UTF-8
# dzheika © 2012


import mutagen
import os, sys, re
import optparse

from mutagen._vorbis import VCommentDict 
from mutagen.id3 import ID3
from mutagen.mp4 import MP4Tags

from dzh.inspectInfo import *
from dzh.interactive import *
from dzh.optParseExt import *

all_srcs = ('SELF', 'FILE', 'PATH', 'CUE', 'TOC', 'ARG')

#
# create option parser
#
parser = optparse.OptionParser(
    usage="Usage: %prog [options] file-1 ... file-n",
    formatter=EpilogHelpFormatter(),
    epilog=["  ** CUE/TOC Tag formatting (TAG-FMT) **",
            "",
            "Format: [scope.]orig-tag[=dest-tag][, ...]",
            "  scope        DISC|TRACK [TRACK is default]",
            "  orig-tag     original tag name",
            "  dest-tag     final tag name [orig-tag is used by default]",
            "",
            "Examples:",
            "    DISC.TITLE=Album,TITLE,ISCR",
            "eq: DISC.TITLE=Album,TRACK.TITLE=Title,TRACK.ISRC=ISRC",
            "",
            "",
            "  ** PICTURE SPECIFICATION **",
            "",
            "Format: [mime=mime/type,][size=WxH,]file=filename",
    ]
)

#
# configure options
#
configFileOpts = []

setupConfigOptions(parser, '~/.tagger.rc')

for val in ('cue', 'toc'):
    configFileOpts.append(parser.add_option("", "--%s" % val, metavar='FILE',
        action="store",
        help="read meta from a %s file (ordered as %s)" % ((val.upper(),) * 2)))

for val in ('cue', 'toc'):
    configFileOpts.append(parser.add_option("", "--%s-tags" % val, metavar='TAG-FMT',
        action="append", default=[],
        help="accept only specified tags from a %s file" % val.upper()))

configFileOpts.append(parser.add_option("-t", "--tag", metavar='TAG=VALUE',
    action="append", default=[],
    help="specify the fixed tag"))

configFileOpts.append(parser.add_option("", "--omit-duplicate-romatags",
    action="store_true",
    help="do not write roma-tag & orig-tag with same content both (keep only orig)"))

configFileOpts.append(parser.add_option("", "--picture", metavar='PIC-SPEC',
    action="append", default=[],
    help="add picture (cover front)"))

configFileOpts.append(parser.add_option("", "--from-path", metavar='REGEX',
    action="store",
    help="detect meta from path to a media file (ordered as PATH)"))

configFileOpts.append(parser.add_option("", "--from-file", metavar='FILE',
    action="append", default=[],
    help="copy meta from another media file[s] (ordered as FILE)"))

configFileOpts.append(parser.add_option("", "--order", metavar='LIST',
    action="store", default=",".join(all_srcs),
    help="comma separated list of meta-sources: " + ", ".join(all_srcs)))

configFileOpts.append(parser.add_option("", "--tracknumber-format", metavar='FMT',
    action="store", default="%02d",
    help="tracknumber format [default: %default]"))

configFileOpts.append(parser.add_option("-k", "--keep-self",
    action="store_true",
    help="keep self meta-data (ordered as SELF)"))

configFileOpts.append(parser.add_option("-a", "--ask",
    action="store_true",
    help="ask before any meta-data modification"))

configFileOpts.append(parser.add_option("-A", "--no-ask",
    action="store_false", dest='ask',
    help="don't ask before any meta-data modification"))

configFileOpts.extend(setupVerboseOptions(parser, withTotally=True))
configFileOpts.extend(setupContinueOptions(parser))

##
## parse args
##
(options, args) = parser.parse_args()
options = loadConfigOptions(parser, configFileOpts, options)
setupOptions(options)

class DescMetaData:
    def __init__(self, orig, dest, data):
        self.orig = orig
        self.dest = dest or orig
        self.data = data
    def __str__(self):
        return "%s='%s'" % (self.dest, self.data)
    def __repr(self):
        return "DescMetaData(orig='%s', dest='%s': data='%s')" % (self.orig, self.dest, self.data)

class DescMetaDataSet:
    def __init__(self):
        self.dest2meta = {}
    def add(self, meta, update=True):
        ldest = meta.dest.lower()
        if not self.dest2meta.has_key(ldest) or update:
            self.dest2meta[ldest] = meta
    def has(self, name):
        return self.dest2meta.has_key(name.lower())
    def get(self, name):
        return self.dest2meta[name.lower()]
    def get_all_meta(self):
        return self.dest2meta.values()
    def update(self, other):
        self.dest2meta.update(other.dest2meta)
    def clear(self):
        self.dest2meta.clear()
    def __repr__(self):
        return "MetaSet(" + ", ".join(str(m) for m in self.dest2meta.values()) + ")"

class DescMetaTags:
    FMT = re.compile(r'^\s*(?:(?P<scope>.+?)\.)?(?P<orig>.+?)(?:=(?P<dest>.+?))?\s*$')
    def __init__(self, value):
        self.discTags = {}
        self.trackTags = {}
        for xval in value.split(","):
            xval = xval.strip()
            if not xval:
                continue
            m = self.FMT.match(xval)
            if not m:
                raise ValueError('Invalid format: %s' % xval)
            scope = m.group('scope') or 'track' # TRACK is default scope
            if scope.lower() not in ['disc', 'track']:
                raise ValueError('Invalid scope: %s: %s' % (scope, xval))
            orig = m.group('orig')
            dest = m.group('dest')
            tags = self.discTags if scope.lower() == 'disc' else self.trackTags
            tags[orig.lower()] = (orig, dest)

class DescMetaLoader:
    def __init__(self, filename, tags):
        self.filename = filename
        self.tags = tags
        self.reset()
    def reset(self):
        self.meta = {}
    @classmethod
    def is_next_track(cls, line):
        raise NotImplementedError("%s.%s" % \
            (cls.__name__, currentMethodName()))
    @classmethod
    def get_track_number(cls, line):
        raise NotImplementedError("%s.%s" % \
            (cls.__name__, currentMethodName()))
    def loadmeta(self):
        self.reset()
        last_track_number = 0
        disc_meta = DescMetaDataSet()
        track_meta = DescMetaDataSet()
        def submit_track_meta():
            if self.meta.has_key(last_track_number):
                raise ValueError('duplicate track number: %d' % last_track_number)
            self.meta[last_track_number] = DescMetaDataSet()
            self.meta[last_track_number].update(disc_meta)
            self.meta[last_track_number].update(track_meta)
            track_meta.clear()
        with open(self.filename, 'r') as f:
            for line in f:
                line = line.strip()
                if self.is_next_track(line):
                    if last_track_number:
                        submit_track_meta()
                    last_track_number += 1 # by default: increment
                tn = self.get_track_number(line)
                if tn > 0:
                    last_track_number = tn
                tags = self.tags.trackTags if last_track_number else self.tags.discTags
                meta = track_meta if last_track_number else disc_meta
                lline = line.lower()
                for (orig, dest) in tags.values():
                    lorig = orig.lower()
                    if lorig[-1] != ' ':
                        lorig += ' '
                    if lline.startswith(lorig):
                        val = line[len(lorig):].strip()
                        if val[0] == val[-1] == '"':
                            val = val[1:-1]
                        meta.add(DescMetaData(orig, dest, val)) 
                # end: for (...) in tags
            # end: for line in f
            if last_track_number:
                submit_track_meta()
        # end: with open(...) as f
        return self.meta
    # end: loadmeta(...)

class CueMetaLoader(DescMetaLoader):
    TRACK_FMT = re.compile(r'^\s*TRACK\s(?P<tn>\d+)\sAUDIO\s*$', re.I)
    @classmethod
    def is_next_track(cls, line):
        return cls.get_track_number(line) > 0
    @classmethod
    def get_track_number(cls, line):
        m = cls.TRACK_FMT.match(line)
        return m and int(m.group('tn')) or -1

class TocMetaLoader(DescMetaLoader):
    @staticmethod
    def is_next_track(line):
        return line.lower().startswith('track ')
    @staticmethod
    def get_track_number(line):
        return 0

#
## vorbis <==> [id3(tags), mp4(keys)]
#
vorbis_to_text_tags = {
    'album':            [mutagen.id3.TALB, '\xa9alb'],
    'grouping':         [mutagen.id3.TIT1, '\xa9grp'],
    'title':            [mutagen.id3.TIT2, '\xa9nam'],
    'subtitle':         [mutagen.id3.TIT3, None     ],
    'artist':           [mutagen.id3.TPE1, '\xa9ART'],
    'albumartist':      [mutagen.id3.TPE2, 'aART'   ],
    'conductor':        [mutagen.id3.TPE3, None     ],
    'remixer':          [mutagen.id3.TPE4, None     ],
    'composer':         [mutagen.id3.TCOM, '\xa9wrt'],
    'lyricist':         [mutagen.id3.TEXT, None     ],
    'discsubtitle':     [mutagen.id3.TSST, None     ],
    'genre':            [mutagen.id3.TCON, '\xa9gen'],
    'date':             [mutagen.id3.TDRC, '\xa9day'],
    'mood':             [mutagen.id3.TMOO, None     ],
    'isrc':             [mutagen.id3.TSRC, None     ],
    'copyright':        [mutagen.id3.TCOP, 'cprt'   ],
    'media':            [mutagen.id3.TMED, None     ],
    'label':            [mutagen.id3.TPUB, None     ],
    'encodedby':        [mutagen.id3.TENC, '\xa9too'],
    'albumsort':        [mutagen.id3.TSOA, 'soal'   ],
    'artistsort':       [mutagen.id3.TSOP, 'soar'   ],
    'titlesort':        [mutagen.id3.TSOT, 'sonm'   ],
}

vorbis_to_text_pair_tags = {
    'performer':        [mutagen.id3.TMCL, None,    re.compile(r'^(?P<v>.*?)(?:\s*\((?P<k>.*)\))?$')],
}

#
## supported tags (vorbis is basic and internal format)
#
(   TAGS_ID3,
    TAGS_MP4,
) = range(2)

def vorbis_to_tags(vorbis, tags_type):
    if tags_type == TAGS_ID3:
        out_tags = ID3()
    elif tags_type == TAGS_MP4:
        out_tags = MP4Tags()
    else:
        raise ValueError("unsupported tags type: 0x%x" % tags_type)
    track_number = None
    track_total = None
    disc_number = None
    disc_total = None
    def split_numeric_part(value, prev_number, prev_total):
        if '/' in value:
            return [int(x) for x in value.split('/', 1)]
        return [int(value), prev_total]
    for tkey, tvals in vorbis.items():
        lkey = tkey.lower()
        for tval in tvals:
            if lkey == 'tracknumber':
                track_number, track_total = split_numeric_part(tval, track_number, track_total)
            elif lkey == 'tracktotal' or lkey == 'totaltracks':
                track_total = int(tval)
            elif lkey == 'discnumber':
                disc_number, disc_total = split_numeric_part(tval, disc_number, disc_total)
            elif lkey == 'disctotal' or lkey == 'totaldiscs':
                disc_total = int(tval)
            else:
                xval = vorbis_to_text_tags.get(lkey, None)
                xtag = xval[tags_type] if xval is not None else None
                if xtag is not None:
                    if tags_type == TAGS_ID3:
                        if lkey == 'date':
                            tval = mutagen.id3.ID3TimeStamp(tval)
                        out_tags[xtag.__name__] = xtag(encoding=3, text=[tval])
                    elif tags_type == TAGS_MP4:
                        out_tags[xtag] = [tval]
                    continue

                xval = vorbis_to_text_pair_tags.get(lkey, None)
                xtag = xval[tags_type] if xval is not None else None
                if xtag is not None:
                    fmt = xval[-1]
                    def decode_tag_pair(val):
                        m = fmt.match(val)
                        if not m:
                            return ('', val)
                        k, v = m.group('k', 'v')
                        return (k or '', v or '')
                    final_tval = decode_tag_pair(tval)
                    if tags_type == TAGS_ID3:
                        out_tags[xtag.__name__] = xtag(encoding=3, people=[final_tval])
                    elif tags_type == TAGS_MP4:
                        out_tags[xtag] = [final_tval] # ? supported ?
                    continue

                tkey_up = tkey.upper()
                if tags_type == TAGS_ID3:
                    out_tags['TXXX:' + tkey_up] = \
                        mutagen.id3.TXXX(encoding=3, desc=tkey_up, text=[tval])
                elif tags_type == TAGS_MP4:
                    out_tags['----:com.apple.iTunes:' + tkey_up] = \
                        [tval]
    def to_numeric_part(number, total):
        if number is None:
            return None
        val = '%02d' % number
        if total is not None:
            val += '/%02d' % total
        return val
    if track_number:
        if tags_type == TAGS_ID3:
            track = to_numeric_part(track_number, track_total)
            out_tags['TRCK'] = mutagen.id3.TRCK(encoding=3, text=[track])
        elif tags_type == TAGS_MP4:
            out_tags['trkn'] = [(track_number, track_total or 0)]
    if disc_number:
        if tags_type == TAGS_ID3:
            disc = to_numeric_part(disc_number, disc_total)
            out_tags['TPOS'] = mutagen.id3.TPOS(encoding=3, text=[disc])
        elif tags_type == TAGS_MP4:
            out_tags['disk'] = [(disc_number, disc_total or 0)]
    return out_tags

def reversed_text_tags_map(tags_type):
    return dict(zip(*(zip(*[
                (k, l[tags_type]) for (k, l) in vorbis_to_text_tags.items()
            ]).__reversed__())))
id3_to_vorbis_text_frame = reversed_text_tags_map(TAGS_ID3)
mp4_to_vorbis_text_frame = reversed_text_tags_map(TAGS_MP4)

def tags_to_vorbis(tags, tags_type):
    out_tags = VCommentDict()
    for tkey, tval in tags.items():
        key = None
        if tags_type == TAGS_ID3:
            tf = id3_to_vorbis_text_frame.get(tval.__class__, None)
            to_text = lambda x: x
            if tf:
                key = tf
            elif isinstance(tval, mutagen.id3.TXXX):
                key = tval.desc
            elif isinstance(tval, mutagen.id3.TRCK):
                key = 'tracknumber'
            elif isinstance(tval, mutagen.id3.TPOS):
                key = 'discnumber'
            elif isinstance(tval, mutagen.id3.TDRC):
                key = 'date'
                to_text = lambda x: x.text
            if key:
                for txt in tval.text:
                    out_tags.append((key, to_text(txt)))
        elif tags_type == TAGS_MP4:
            def append_numtot_pairs(num_name, tot_name, pairs):
                for num_tot in pairs:
                    (num, tot) = num_tot if len(num_tot) > 1 else (num_tot[0], None)
                    out_tags.append((num_name, str(num)))
                    if tot:
                        out_tags.append((tot_name, str(tot)))
            tf = mp4_to_vorbis_text_frame.get(tkey, None)
            if tf:
                key = tf
            elif tkey.startswith('----:com.apple.iTunes:'):
                key = tkey.split(':', 2)[2]
            elif tkey == 'trkn':
                append_numtot_pairs('tracknumber', 'tracktotal', tval)
            elif tkey == 'disk':
                append_numtot_pairs('discnumber', 'disctotal', tval)
            if key:
                for txt in tval:
                    out_tags.append((key, txt))
        else:
            raise ValueError("unsupported tags type: 0x%x" % tags_type)
    return out_tags

#
## prepare settings
#
from_path_fmt = None
if options.from_path:
    try:
        from_path_fmt = re.compile(options.from_path)
    except Exception, e:
        showErrAsk('Invalid from-path-regex: %s', (e,))

fixed_tags = {}
if options.tag:
    try:
        for pair in options.tag:
            k, v = pair.split('=', 1)
            if fixed_tags.has_key(k):
                if not isinstance(fixed_tags[k], list):
                    fixed_tags[k] = [fixed_tags[k]]
                fixed_tags[k].append(v)
            else:
                fixed_tags[k] = v
    except Exception, e:
        showErrAsk('Invalid tag pair: %s: %s', (pair, e))

#
## process files
#



src_file_cache = {}
def load_meta_from_src_file(vorbis, path, save=False):
    tags = src_file_cache.get(path, None) if save else None
    if tags is None:
        f = mutagen.File(path)
        if isinstance(f.tags, VCommentDict):
            tags = f.tags
        elif isinstance(f.tags, ID3):
            tags = tags_to_vorbis(f.tags, TAGS_ID3)
        elif isinstance(f.tags, MP4Tags):
            tags = tags_to_vorbis(f.tags, TAGS_MP4)
        else:
            raise ValueError('current release supports only vorbis comment or mp3 (id3),'
                ' but file(%s) contains: %s' % (path, f.tags.__class__.__name__))
        if save:
            src_file_cache[path] = tags
    vorbis.update(tags)

src_desc_cue = None
src_desc_toc = None
def load_meta_from_src_desc(vorbis, desc, number):
    global src_desc_cue
    global src_desc_toc
    src = None
    if desc == 'CUE':
        if not src_desc_cue and options.cue:
            cue_tags = ','.join(options.cue_tags)
            showMesgIfVerbose('CUE tags are: %s', (cue_tags,))
            src_desc_cue = CueMetaLoader(options.cue, DescMetaTags(cue_tags)).loadmeta()
        src = src_desc_cue
    elif desc == 'TOC' and options.toc:
        if not src_desc_toc and options.toc:
            toc_tags = ','.join(options.toc_tags)
            showMesgIfVerbose('TOC tags are: %s', (toc_tags,))
            src_desc_toc = TocMetaLoader(options.toc, DescMetaTags(toc_tags)).loadmeta()
        src = src_desc_toc
    else:
        showErr('%s: unknown desc: %s', (currentMethodName(), desc))
    if src:
        tn = number
        if vorbis.has_key('tracknumber'):
            try:
                tn = int(vorbis['tracknumber'][0].split('/')[0])
            except ValueError, e:
                showWarn('Illegal tracknumber: %s', (vorbis['tracknumber'],))
                askContinue('default tracknumber is %d, ' % number)
        metaset = src.get(tn, None)
        if metaset:
            for meta in metaset.get_all_meta():
                vorbis[meta.dest] = meta.data
        else:
            showWarn('meta-source(%s): no such tracknumber: %s', (desc, tn))
    return

def load_meta_from_src(src, vorbis, arg, number):
    if src == 'ARG':
        if fixed_tags:
            showMesgIfVerbose('adding fixed tags (from ARG)')
            vorbis.update(fixed_tags)
    elif src == 'SELF':
        if options.keep_self:
            showMesgIfVerbose('loading from meta-source(SELF): %s', (arg,))
            load_meta_from_src_file(vorbis, arg, save=False)
    elif src == 'FILE':
        for path in options.from_file:
            showMesgIfVerbose('loading from meta-source(FILE): %s', (path,))
            load_meta_from_src_file(vorbis, path, save=True)
    elif src == 'PATH':
        if from_path_fmt:
            abspath = os.path.abspath(arg)
            showMesgIfVerbose('loading from meta-source(PATH): %s', (abspath,))
            m = from_path_fmt.match(abspath)
            if m:
                for k in from_path_fmt.groupindex.keys():
                    val = m.group(k)
                    if val is not None:
                        vorbis[k] = val
            else:
                showWarnAsk('no matches for meta-source(PATH):'
                    '\n  regex: %s'
                    '\n  path: %s', (from_path_fmt.pattern, abspath))
    elif src == 'CUE':
        if options.cue:
            showMesgIfVerbose('loading from meta-source(CUE): %s', (options.cue,))
            load_meta_from_src_desc(vorbis, src, number)
    elif src == 'TOC':
        if options.toc:
            showMesgIfVerbose('loading from meta-source(TOC): %s', (options.toc,))
            load_meta_from_src_desc(vorbis, src, number)
    else:
        showWarn('unknown meta-source: %s', (src,))

number = 0
for arg in args:
    number += 1
    vorbis = mutagen._vorbis.VCommentDict()
    else_srcs = set(all_srcs)
    showMesgIfNotQuiet(' >> next file: %s', (arg,))
    try:
        for src in options.order.split(','):
            src = src.strip().upper()
            load_meta_from_src(src, vorbis, arg, number)
            if src in all_srcs:
                else_srcs.remove(src)
        for src in else_srcs:
            load_meta_from_src(src, vorbis, arg, number)
        keys = vorbis.keys()
        if options.omit_duplicate_romatags:
            for k in keys:
                if not k.lower().startswith('roma'):
                    continue
                orig_key = k[4:].lower()
                if orig_key not in ('artist', 'album', 'albumartist', 'title', 'subtitle', 'discsubtitle'):
                    continue
                roma_val = vorbis[k]
                if not vorbis.has_key(orig_key):
                    vorbis[orig_key] = roma_val
                    del vorbis[k]
                else:
                    orig_val = vorbis[orig_key]
                    if roma_val == orig_val:
                        del vorbis[k]
        keys = vorbis.keys()
        for k in keys:
            vorbis[k] = [x if isinstance(x, unicode) else unicode(x, 'UTF-8')
                for x in vorbis[k]
            ]
        if options.ask or isVerbose():
            showMesg('   *** final tags ***')
            for k, vv in vorbis.items():
                for v in vv:
                    showMesg(' >> %s: %s',  (k, v))
            showMesg('      ************')
        if options.ask:
            if not askYesNo(' << write meta-data', default=True):
                continue
        f = mutagen.File(arg)
        if f.tags is None:
            if isinstance(f, mutagen.id3.ID3FileType):
                f.tags = ID3()
            else:
                f.tags = VCommentDict()
            f.tags.filename = f.filename
        else:
            f.tags.clear()
        if isinstance(f.tags, VCommentDict):
            f.tags.update(vorbis)
        elif isinstance(f.tags, ID3):
            f.tags.update(vorbis_to_tags(vorbis, TAGS_ID3))
        elif isinstance(f.tags, MP4Tags):
            f.tags.update(vorbis_to_tags(vorbis, TAGS_MP4))
        else:
            raise ValueError("not supported meta-type: %s" % f.tags.__class__.__name__)
        f.save()
    except KeyboardInterrupt, e:
        print "exiting"
        sys.exit(2)
    except Exception, e:
        if isVerbose(totally=True):
            import traceback
            traceback.print_exc()
        showErrAsk('can not apply tags: %s', (e,))
