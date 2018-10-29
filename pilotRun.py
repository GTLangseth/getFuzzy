import pandas as pd
import random
from fuzzywuzzy import fuzz
from getFuzzy import PeachFuzz


def get_rand_chr():
    chr_num = random.randint(65, 65+26-1)
    return chr(chr_num)


def get_rand_str(str_len=5):
    str_val = ''
    for i in range(str_len):
        str_val += get_rand_chr()
    return str_val


df1 = pd.DataFrame({
    'A':[get_rand_str() for i in range(30)],
    'B':[get_rand_str() for i in range(30)],
    'C':[get_rand_str() for i in range(30)],
})

df2 = pd.DataFrame({
    'A':[get_rand_str() for i in range(30)],
    'B':[get_rand_str() for i in range(30)],
    'C':[get_rand_str() for i in range(30)],
})

match_tuple = [
    ('A','A', fuzz.partial_ratio, 1),
    ('B','C', fuzz.partial_ratio, 1),
]

match_set = PeachFuzz(df1, df2, match_tuple)
