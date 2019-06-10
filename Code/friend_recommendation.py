'''
Recommend friend using user-to-user algorithm
'''
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
import random
import collections
import math

WEIGHT = 0.7
# this is the the weight we give for rating_diff and freq_sim
# we can adjust this parameter to gain a higher prediction accuracy


class yelp_recom(MRJob):
    '''
    This friend recommendation is based on user_to_user algorithm
    '''

    def mapper(self, _, line):
        '''
        Compute the tuple (user_id, rating) for each user
        '''
        try:
            user_id, bus_id, rating = line.split(',')[:3]       
            yield bus_id, (user_id, float(rating))
        except:
            pass


    def reducer(self, bus_id, values):
        '''
        Compute all the users for each business id, for each user
        we record the average ratings for this business and visiting times
        eg: ['FL2j0euK58x-JDwdolhIKg': (4.0, 1), 'U0WU9d6gpMNkO_tPXK58Sg': (3.0, 1)]
        '''
        values_ls = list(values)
        values_dic = {}
        for user_id, rating in values_ls:
            values_dic[user_id] = values_dic.get(user_id, []) + [rating]
        avg_rating_dic = {}
        for user_id, val_lst in values_dic.items():
            l = len(val_lst)
            avg_rating_dic[user_id] = (sum(val_lst)/l, l)
        final_ls=[]
        for key in avg_rating_dic:
            final_ls.append([key, avg_rating_dic[key]])
        yield bus_id, final_ls


    def topk_mapper(self, bus_id, ur_list):
        '''
        Compute the similarity score for each user pair and 
        split the dataset into training set and testing set
        '''
        for ur_1, ur_2 in combinations(ur_list, 2):
            user_1, user_2 = ur_1[0], ur_2[0]
            rating_1, rating_2 = ur_1[1][0], ur_2[1][0]
            visits_1, visits_2 = ur_1[1][1], ur_2[1][1]
            freq_sim = min(visits_1, visits_2) / max(visits_1 , visits_2)
            rating_diff = abs((rating_1 - rating_2)) / 5
            def sim_func(freq, rat, a):
                return freq_sim*a + (a-1)*(rating_diff)
            sim=sim_func(freq_sim, rating_diff, 0.7) 
            yield (user_1, user_2), sim


    def topk_reducer(self, pair, similarity):
        '''
        Sum up the similarity score for each user pair based on 
        all the common businesses they have visited 
        '''
        all_lst = []
        for i in list(similarity):
            all_lst.append(i)
        yield pair, sum(all_lst)


    def most_sim_mapper(self, pair, similarity):
        '''
        Compute all other possible similar users with respect 
        to one user with their similarity scores
        '''
        name=pair[0]
        others=[pair[1],similarity]
        yield name, others


    def most_sim_reducer(self,name,others):
        '''
        Find the user id with the highest similarity score with respect 
        to every user
        '''
        allothers = list(others)
        sim_dict = dict(allothers)
        sorted_simi = sorted(sim_dict.items(),key=lambda x: x[1],reverse=True)
        yield name, sorted_simi[0][0]


    def steps(self):
        '''
        Multiple steps work use steps function
        '''
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.topk_mapper,
                     reducer=self.topk_reducer),
            MRStep(mapper=self.most_sim_mapper,
                reducer=self.most_sim_reducer),
            ]


if __name__ == '__main__':
    yelp_recom.run()
