# clean_lookups.py

lookup_list = [
    '14-Nov-22',
    '2.37E+16',
    '5.41E+16',
    '8.21E+16',
    '9.40E+16',
    '-- year--',
    '--none--',
    '-select-',
    '&quotdavid northway &quot',
    '&quoteliteach works  &quot',
    '&quotr&quot delivery',
    '#NAME?',
    '5k',
    '3r ag',
    '3bs',
    '1975',
    '2k',
    '1 deep',
    '785',
    '8325200286',
    '3r ag',
    '10-2-4 ranch',
    '107186',
    '3r ag',
    '5k',
    '10-2-4 ranch',
    '3bs'
]

def drop_lookup_values(df, column):
    for lookup_value in lookup_list:
        df = df[~df[column].str.contains(lookup_value, case=False, na=False)]
    return df
