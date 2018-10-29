"""
The purpose of this class object is to provide for quick and efficient fuzzy matching between two pandas data-frames.
The strategy will be to write to a sqlite instance for memory-efficient operations as string similarity sequences
are performed.
"""

import pandas as pd
import logging
from sqlalchemy import create_engine


class PeachFuzz(object):

    def __init__(self, df1, df2, match_tuple=[]):
        self.df1 = df1
        self.df2 = df2
        self.match_tuple = match_tuple
        self.eng = create_engine('sqlite:///:memory:')
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())
        self.logger.info("class initialized")

    def score_column_pairs(self):
        """
        Creates a table in the sqlite database for each column used to make a comparison
        """
        self.logger.info("scoring column similarities")
        table_names = []
        for tup in self.match_tuple:

            col_a, col_b = tup[0], tup[1]
            col_pair = '_'.join([col_a, col_b])
            self.logger.info("scoring col pair: {}".format(col_pair))
            ser_a = self.df1[col_a].dropna().astype(str)
            ser_b = self.df2[col_b].dropna().astype(str)
            iter_len = len(ser_a) * len(ser_b)
            scorer = tup[2]
            weight = tup[3]
            self.logger.info("using scorer: {}".format(scorer))
            self.logger.info("col weight set to: {}".format(scorer))

            score_results = []
            div = 0
            p_div = int(iter_len * 0.1)
            for ix, ie in ser_a.iteritems():
                for jx, je in ser_b.iteritems():
                    score = scorer(ie, je)
                    df_row = (ix, jx, score)
                    score_results.append(df_row)
                    if div % p_div == 0:
                        _p = round(div/iter_len, 4) * 100
                        self.logger.info("{}% complete".format(_p))
                    div += 1
            del ser_a
            del ser_b

            col_names = [col_a + '_df1',
                         col_b + '_df2',
                         'score']

            col_pair_df = pd.DataFrame(score_results,
                                       columns=col_names)
            col_pair_df.to_sql(col_pair, self.eng,
                               index=False, if_exists='replace')
            del col_pair_df

            table_names.append((col_pair, weight))
            table_name_df = pd.DataFrame(table_names,
                                         columns=['table_name', 'score_weight'])
            table_name_df.to_sql('table_index', self.eng,
                                 index=False, if_exists='replace')
            self.logger.info("all tables created")

    def get_aggregate_scores(self):
        """
        creates index of all row pairs and sorts by aggregate score
        """
        self.logger.info("generating aggregate scores")
        score_dict = dict()
        tables = pd.read_sql_table('table_index', self.eng)
        for i, r in tables.iterrows():
            table = r['table_name']
            w = r['score_weight']
            self.logger.info("aggregating scores for {}".format(table))
            scores = pd.read_sql_table(table, self.eng)
            for j, s in scores.iterrows():
                score = s['score'] * w
                pair_ix = (s[0], s[1])
                if pair_ix in score_dict:
                    score_dict[pair_ix] += score
                else:
                    score_dict[pair_ix] = score
        score_series = pd.Series(score_dict, name='aggregate_scores')
        del score_dict
        score_series = pd.DataFrame(score_series).sort_values('aggregate_scores',
                                                              ascending=False)
        score_series.to_sql('aggregate_scores', self.eng, if_exists='replace')
        del score_series

    def run_scorer(self):
        self.logger.info("running scoring module")
        self.score_column_pairs()
        self.get_aggregate_scores()
        self.logger.info("scoring module complete!")

    def get_scores(self):
        self.run_scorer()
        df = pd.read_sql_table('aggregate_scores', self.eng)
        return df
