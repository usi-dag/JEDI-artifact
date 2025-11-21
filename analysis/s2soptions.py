
from collections import namedtuple

paper_renaming = {
    # 'PARALLEL_UNORDERED': 'PU',
    # 'CONCURRENT': 'CG',
    # 'CONCURRENT_COLLECTOR': 'CGCC',
    # 'CONCURRENT_COLLECTOR_ADDER': 'CGCC',
}

paper_reverse_renaming = {v: k for k, v in paper_renaming.items()}

def rename(name):
    return paper_renaming.get(name, name)

def reverse_rename(name):
    return paper_reverse_renaming.get(name, name)


StreamOptionFields = {
    'join_converter': ['FLATMAP', 'MAPMULTI'],
    'multi_threading': [
        'SEQ',
        'PU',
        'CG',
        'CGCC'
    ],
    'fuseFilters': ['notFuseFilters', 'fuseFilters'],
}

StreamOptionTuple = namedtuple('StreamOptionTuple', StreamOptionFields)


def index_of(member, iterable):
    idx = next((i for i, element in enumerate(iterable) if element == member), None)
    if idx is not None:
        return idx
    # Error here
    error = f'Cannot find the element {member} in the given iterable {iterable}'
    raise ValueError(error)


class StreamOption(StreamOptionTuple):
    @staticmethod
    def from_str(s):
        fields = s.split('_')
        if fields[0] == 'Opt': 
            fields = fields[1:]

        return StreamOption(*fields)
    
    def with_option(self, option, value):
        self_options = list(self)
        self_options[index_of(option, StreamOptionFields)] = value
        return StreamOption(*self_options)

    def compact_str(self):
        return ('Opt'
                + '_' + self.join_converter
                + '_' + self.multi_threading
                + '_' + self.fuseFilters
        )
    
    def compact_str_paper(self):
        return ('Opt'
                + '_' + self.join_converter
                + '_' + self.multi_threading
                + '_' + self.fuseFilters
        )
    
    def variant_name(self):
        return 'Stream_' + self.compact_str()


stream_baseline = StreamOption('FLATMAP', 'SEQ', "notFuseFilters")
stream_best_sequential = StreamOption('MAPMULTI', 'SEQ', 'fuseFilters')



class ImperativeOption:
    def compact_str(self):
        return 'Opt'

    def variant_name(self):
        return 'Imperative_' + self.compact_str()



def from_variant_name(variant):
    if variant.startswith('Imperative_'):
        return ImperativeOption()
    if variant.startswith('Stream_'):
        return StreamOption.from_str(variant.replace('Stream_', ''))
    raise ValueError('Not a valid variant prefix: ' + variant)

