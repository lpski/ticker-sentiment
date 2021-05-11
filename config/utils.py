import pandas as pd


def initialze_symbol_data():
    df = pd.read_csv('symbol_data/symbols.csv')

    # We build enhanced symbol data by simplifying company names for better relevance
    def strip_lingo(name: str) -> str:
        name = name.lower()

        remove_strings = [
            'Ordinary Shares', 'New Common Stock', 'Common Stock', 'Common Shares',
            'Common Units Representing Limited Partner Interests',
            'representing Limited Partner Interests',
            'Common Units',
            'Class A Voting', 'Class B Voting',
            'Class A', 'Class B', 'Class C',
            'voting shares', 'american depositary shares', 'depositary shares',
            'Group Holdings',
            # 'Holdings',
            'Corporation', 'Stock', ' Corp ', ' Corp.',
            ', Inc', ', Inc.', ' Inc.', ' Inc ',
            ' Ltd ', ' Ltd.', ' limited partner', ' Limited', 'l.p.',
            ' plc ', 'p.l.c.', ' lp ', 's.a.', 'llc',
            '(The)', '(New)'
        ]
        # remove_strings = remove_strings + [s.lower() for s in remove_strings] + [s.upper() for s in remove_strings]
        remove_strings = [s.lower() for s in remove_strings]
        
        if 'corporation' in name: name = name[:name.index('corporation')]
        if '(' in name: name = name[:name.index('(')]
        for target in remove_strings: name = name.replace(target, '')

        name = ' '.join(name.title().strip().split())
        if name.split(' ')[-1] in remove_strings: name = ' '.join(name.split(' ')[:-1])

        return name

    df['Reduced Name'] = df['Name'].apply(strip_lingo)
    df.to_csv('symbol_data/enhanced_symbols.csv')


