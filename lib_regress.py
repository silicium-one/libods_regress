import logging
import ods as lib_old
import libods as lib_new

logging.basicConfig(format='%(asctime)s|%(levelname)8s|%(message)s', level=logging.DEBUG)
logger = logging.getLogger("logger")


def log(level, message):
    logger.log(level, message)

def new_lib():
    source = {
        'type': u'files',
        'file_names': (u'data.csv.gz',),
        'fetch_buffer_size': 128L * 1024L,
        'delimiter': u'|',
        'quote': u'"'
    }

    target = {
        "type": u"file",
        "file_name": u'out_new.csv.gz',
        "compression": 6L,
        "quote": '"',
        "delimiter": "|",
        'filter_mask': u'vvssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss'
                       u'ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss'
                       u'ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss',
        'filter_values': ()
    }
    res = lib_new.transfer(source, (target,), log)
    logger.info(res)

def old_lib():
    source = {
        'type': 'files',
        'file_names': ('data.csv.gz',),
        'fetch_buffer_size': 128 * 1024,
        'delimiter': '|', 'quote': '"'
    }

    processor = {
        'type': 'ignore_skip_columns_processor',
        'mask': 'vvssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss'
                'ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss'
                'ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss',
    }

    target = {
        "type": "file",
        "file_name": "out_old.csv.gz",
        "compression": 6
    }
    res = lib_old.process(source, ((target, processor),), log)
    logger.info(res)

new_lib()
old_lib()