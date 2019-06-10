'''
User-to-user algorithm
'''
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
import random
import collections
import math

# this is the the weight we give for rating_diff and freq_sim
# we can adjust this parameter to gain a higher prediction accuracy
WEIGHT = 0.7


class yelp_recom(MRJob):

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
        # values (user_id, rating)
        values_ls = list(values)
        values_dic = {}
        for user_id, rating in values_ls:
            values_dic[user_id] = values_dic.get(user_id, []) + [rating]
        avg_rating_dic = {}
        for user_id, val_lst in values_dic.items():
            l = len(val_lst)
            avg_rating_dic[user_id] = (sum(val_lst)/l, l)
        final_ls = []
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
            # train-test split
            r = random.random()
            freq_sim = min(visits_1, visits_2)/ max(visits_1 , visits_2)
            rating_diff =(abs(rating_1 - rating_2)) / 5
            def sim_func(freq, rat, a):
                return freq_sim*a + (a-1)*(rating_diff)
            sim =- 10000
            if r <= 0.7:
                sim = sim_func(freq_sim, rating_diff, WEIGHT) 
            else:
                # Here we assign -2 to indicate the two users are similar and \
                # -3 means not similar
                if ((freq_sim >= 0.5) & (rating_diff <= 0.2)):
                    sim = -2
                else:
                    sim = -3
            yield (user_1, user_2), sim


    def topk_reducer(self, pair, similarity):
        '''
        Sum up the similarity score for each user pair based on 
        all the common businesses they have visited 
        '''
        training=[]
        test=[]
        for i in list(similarity):
            if (i != -2) & (i != -3):
                training.append(i)
            else:
                if i == -2:
                    test.append(1)
                else:
                    test.append(0)
        yield pair, (sum(training),(sum(test), len(test)))


    def top3_mapper(self, pair, similarity):
        '''
        Compute all other possible similar users with respect 
        to one user with their similarity scores
        '''
        name = pair[0]
        others = [pair[1],(similarity[0],similarity[1])]
        yield name, others
    

    def top3_reducer(self,name,others):
        '''
        Sorted the list of similar users with the decreasing
        similarity scores
        '''
        allothers = list(others)
        sim_dict = dict(allothers)
        sorted_simi = sorted(sim_dict.items(), key=lambda x: x[1][0], reverse=True)
        yield name, sorted_simi


    def accuracy_mapper(self,name,similarity):
        '''
        Find the accuract prediction number based on all the users 
        and only topk users
        '''
        acc_total = 0
        num_total = 0
        acc_top3 = 0
        num_top3 = 0
        for item in similarity:
            acc_total += item[-1][-1][0]
            num_total += item[-1][-1][-1]
        for item in similarity[:3]:
            acc_top3 += item[-1][-1][0]
            num_top3 += item[-1][-1][-1]
        yield None, ((acc_total, num_total),(acc_top3,num_top3))
     

    def accuracy_reducer(self,_,similarity):
        '''
        Compute the accuracy score based on the all the users 
        and the topk users
        '''
        acc_total = 0
        num_total = 0
        acc_top3 = 0
        num_top3 = 0
        for item in similarity:
            acc_total += item[0][0]
            num_total += item[0][1]
            acc_top3 += item[1][0]
            num_top3 += item[1][1]
        acc_pre = -1 
        acc_post = -1 
        if num_total != 0:
            acc_pre = acc_total/num_total
        if num_top3 != 0:
            acc_post = acc_top3/num_top3
        yield None, (acc_pre, acc_post)


    def steps(self):
        '''
        Multiple steps work use steps function
        '''
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.topk_mapper,
                    reducer=self.topk_reducer),
            MRStep(mapper=self.top3_mapper,
                    reducer=self.top3_reducer),
            MRStep(mapper=self.accuracy_mapper,
                    reducer=self.accuracy_reducer)
            ]


if __name__ == '__main__':
    yelp_recom.run()
