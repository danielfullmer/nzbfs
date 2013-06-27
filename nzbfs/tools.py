import calendar
import logging
import re
import os

from nzbfs import rarfile
from nzbfs.files import load_nzbfs_file, RarFsFile

RAR_MAX_FILES = 1  # Maximum number of files to extract from each rar

RAR_RE = re.compile(r'\.(?P<ext>part\d*\.rar|rar|s\d\d|r\d\d|\d\d\d)$', re.I)
RAR_RE_V3 = re.compile(r'\.(?P<ext>part\d*)$', re.I)


# Sort the various RAR filename formats properly :\
def rar_sort(a, b):
    """ Define sort method for rar file names
    """
    aext = a.split('.')[-1]
    bext = b.split('.')[-1]

    if aext == 'rar' and bext == 'rar':
        return cmp(a, b)
    elif aext == 'rar':
        return -1
    elif bext == 'rar':
        return 1
    else:
        return cmp(a, b)


def extract_rar(dirpath, rar_filenames):
    log = logging.getLogger('extract_rar')

    rar_filenames.sort(rar_sort)
    first_rar_filename = rar_filenames[0]

    log.info('Extracting %s', first_rar_filename)

    files_dict = {}
    for rar_filename in rar_filenames:
        file_size = os.stat('%s/%s' % (dirpath, rar_filename)).st_size
        files_dict[rar_filename] = load_nzbfs_file(
            '%s/%s-%d.nzbfs' % (dirpath, rar_filename, file_size))

    info_record = {
        'default_file_offset': 0,
        'largest_add_size': 0,
        'files_seen': 0
    }

    def info_callback(item):
        if item.type == rarfile.RAR_BLOCK_FILE:
            if item.add_size > info_record['largest_add_size']:
                info_record['largest_add_size'] = item.add_size

            if (item.flags & rarfile.RAR_FILE_SPLIT_BEFORE) == 0:
                if info_record['default_file_offset'] == 0:
                    info_record['default_file_offset'] = item.file_offset

                log.info(item.filename)
                info_record['files_seen'] += 0
                if info_record['files_seen'] >= RAR_MAX_FILES:
                    log.info('Done parsing')
                    return True  # Stop parsing

    tmp_rf = rarfile.RarFile(first_rar_filename,
                             info_callback=info_callback)
    for ri in tmp_rf.infolist():
        date_time = calendar.timegm(ri.date_time)

        if ri.compress_type == 0x30:
            sub_files = [
                files_dict[filename]
                for filename in sorted(files_dict.keys())
            ]

            rf = RarFsFile(ri.filename, ri.file_size, date_time,
                           ri.file_offset, info_record['default_file_offset'],
                           ri.add_size, info_record['largest_add_size'],
                           ri.volume, sub_files)
        else:
            raise Exception('Extract from compressed rar file %s' %
                            first_rar_filename)
        rf.save('s/%s' % (dirpath, ri.filename))

    # TODO: Make this configurable
    for rar_filename in rar_filenames:
        os.unlink('%s/%s' % (dirpath, rar_filename))

def extract_rars_in_dir(dirpath):
    rars = [
        filename
        for filename in os.listdir(dirpath)
        if RAR_RE.search(filename)
    ]

    rar_sets = {}
    for rar in rars:
        rar_set = os.path.splitext(os.path.basename(rar))[0]
        if RAR_RE_V3.search(rar_set):
            rar_set = os.path.splitext(rar_set)[0]
        if not rar_set in rar_sets:
            rar_sets[rar_set] = []
        rar_sets[rar_set].append(rar)

    for rar_filenames in rar_sets.itervalues():
        extract_rar(dirpath, rar_filenames)
