# coding: UTF-8
# dzheika © 2012

def HumanReadableSize(size, precision=2):
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    suffixIndex = 0
    while size > 1024 and (suffixIndex+1) < len(suffixes):
        suffixIndex += 1
        size = size/1024.0
    return "%.*f %s" % (precision, size, suffixes[suffixIndex])
