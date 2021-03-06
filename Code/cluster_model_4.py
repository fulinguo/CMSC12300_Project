'''
Clustering algorithm
'''
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
import random
import collections
import math

# we can adjust these parameters to gain a higher prediction accuracy
FREQ_THE=1 #the thershold of visiting times
STAR_THE=4 # the thershold of average ratings
TIMES_THE=1 # the thershold of times that two users are in the same sub-clusters

class cluster(MRJob):
    '''
    The clustering algorithm aims to classify users and to make recommendations 
    for each user based on other users who are classified into the same cluster. 
    First, for every business, users who visited it more than certain times and 
    rated this restaurant higher than a certain rating score are grouped into a 
    sub-cluster. The sub-cluster is not the final cluster, but an intermediate 
    set that is used to create the final classification. Then, users who are 
    put into the same sub-cluster for more than certain times will be put into 
    the same final cluster. Finally, we recommend the businesses that were 
    visited by all the other users in the same final cluster to each user. 

    We used a test set to check whether users in the same final cluster are 
    actually similar to each other. The criteria to check user similarity is 
    the same as the criteria used in the user-to-user algorithm of calculating 
    the prediction accuracy in the test set. Besides, we also calculated the 
    baseline accuracy without using the current clustering model. That being 
    said, we assumed that everyone is similar to each other, no matter how 
    many times they have visited the same businesses or how many stars they 
    have given to them. For a given person, we recommended all businesses 
    visited by anyone else but himself/herself to him/her. In such a way, 
    we are able to know whether the prediction accuracy of the clustering 
    model has improved or not.

    '''

    def mapper(self, _, line):
        '''
        For each review record, get the user_id, bus_id, and the rating.
        '''
        try:
            user_id, bus_id, rating = line.split(',')[:3]     #   
            yield bus_id, (user_id, float(rating))
        except: # skip the rows that are not review records (e.g., the first row)
            pass

    def reducer(self, bus_id, values):
        '''
        The reducer calculates the times that a user went to a specific restaurant and
        the average score the user rated for the restaurant. The key here is a business_id 
        and the value is a list like: 
        ['FL2j0euK58x-JDwdolhIKg': (4.0, 1), 'U0WU9d6gpMNkO_tPXK58Sg': (3.0, 1)]. 
        This list is a record for one business in our code, it includes all 
        the user_ids who visited the business and a tuple to indicate the average 
        rating and visiting times for this user to this business.
        '''
        values_ls = list(values)
        values_dic = {}

        for user_id, rating in values_ls:
            values_dic[user_id] = values_dic.get(user_id, []) + [rating]

        avg_rating_dic = {} # dictionary: the key is user_id, the value is a tuple of average rating and visiting times
        for user_id, val_lst in values_dic.items():
            l = len(val_lst)
            # sum(val_lst)/l is the average rating score, and l is the times that the user visited the business
            avg_rating_dic[user_id] = (sum(val_lst)/l, l)
        final_ls=[]
        # final_ls records all the user_ids who visited the business and the average 
        # rating and visiting times for this user to this business
        for key in avg_rating_dic:
            final_ls.append([key, avg_rating_dic[key]])

        yield bus_id, final_ls

    def cluster_mapper(self, bus_id, ur_list):
        '''
        we first use the random variable generated by python to decide 
        whether the business should be put in the training set or the 
        test set. If the business_id is supposed to be put in the 
        training set, we decide which users who visited this business 
        should be classified in the same sub-cluster. If the business 
        set is in the test set, we calculate whether the two users are 
        truly similar in selecting this business.
        '''
        r=random.random() # generate a random number which is used to split the businesses into a training set and a test set
        cluster=[]
        if r<=0.7:
            # If r<0.7, the business be put into the training set
            for item in ur_list:
                if (item[-1][-1]>=FREQ_THE) & (item[-1][0]>=STAR_THE):
                # users who visited it more than certain times and rated this restaurant 
                # higher than a certain rating score are grouped into a sub-cluster
                    cluster.append(item[0])
            # yield the subcluster if the business is in the training set.
            yield None, cluster
        else: 
            # otherwise, the business should be put into the test set
            for ur_1, ur_2 in combinations(ur_list, 2):
                user_1, user_2 = ur_1[0], ur_2[0]
                rating_1, rating_2 = ur_1[1][0], ur_2[1][0]
                visits_1, visits_2 = ur_1[1][1], ur_2[1][1]

                freq_sim=min(visits_1, visits_2)/max(visits_1 , visits_2)
                rating_sim=abs((rating_1 - rating_2))/5
                sim=-100

                # Here, we defined that a prediction is correct (i.e. two users are
                # actually similar to each other) if freq_sim is greater than or equal 
                # to 0.5 and rating_diff is less than or equal to 0.2 between the two users
                if (freq_sim>=0.5) & (rating_sim<=0.2):
                    # we assign the similarity score 1 if the pairs are truly similar
                    sim=1
                else:
                    # assign it 0 if they are not truly similar
                    sim=0
                # yield user pairs as the key, and similarity score (i.e., 0-1 variable actually) as the value for the test set
                yield (user_1, user_2), sim

    def cluster_reducer(self,pair_cluster,similarity):
        '''
        This step yields the list of sub-classifications corresponded to each business in the training set
        and the number of successful predictions and the number of all predictions made for each business 
        if the business is in the test set.
        '''
        test=[]
        bigclusters=[] # list of sub-classifications corresponded to each the business in the training set
        if type(pair_cluster)==list:
            for sim in list(similarity):
                test.append(sim)
            yield  None, (pair_cluster, (sum(test), len(test)))
        else:
            try:
                for item in list(similarity):
                    bigclusters.append(item) 
            except:
                pass
            yield None, bigclusters



    def top5_mapper(self, _ ,similarity):  
        yield None, similarity
      
    def top5_reducer(self, _, others):
        '''
        For the test set, we first calculate the accuracy rate without utilizing 
        our model. Then, we used the training set to combine sub-classifications 
        to generate the final classification of users based on the times threshold. 
        Then, we use the test set to calculate the accuracy rate of our model and 
        compared it to the accuracy rate without using the model. which could be 
        further used to select the best parameters.
        '''
        test={}
        bigclusters=[]
        pairs_cou={} # calculate the times that two users are put into the same subclusters
        for item in list(others):
            if item!=[]:
                try:
                    if item[-1]!=[]:
                        if type(item[-1][0])==int:
                            test[item[0][0]+'-'+item[0][1]]=item[1]
                        else:
                            for li in item:
                                for ur_1, ur_2 in combinations(li, 2):
                                    if (ur_1+'-'+ur_2 not in pairs_cou) & (ur_2+'-'+ur_1 not in pairs_cou):
                                        pairs_cou[ur_1+'-'+ur_2]=1
                                    else:
                                        try:
                                            pairs_cou[ur_1+'-'+ur_2]+=1
                                        except:
                                            pairs_cou[ur_2+'-'+ur_1]+=1
                except:
                    pass

        for pairs in pairs_cou:
                if pairs_cou[pairs]>=TIMES_THE:
                    # users who are put into the same sub-cluster for more than TIMES_THE will be put into the same final cluster
                    bigclusters.append([pairs])

        acc_total=0 
        num_total=0
        acc_cluster=0
        num_cluster=0

        for item in test:
            acc_total+=test[item][0] # acc_total is the number of accurate predictions when we do NOT use the algorithm
            num_total+=test[item][1] # num_total is the number of predictions we made when we do NOT use the algorithm

        try:
            if bigclusters!=[]:
                for item in bigclusters:
                    for i in item:
                        if i in test:
                            acc_cluster+=test[i][0] # acc_cluster is the number of accurate predictions when we use the cluster algorithm
                            num_cluster+=test[i][1] # num_cluster is the number of predictions we made when we use the cluster algorithm

        except:
            pass
        acc_pre=-1 # if num_total=0
        acc_post=-1 # if num_top5=0
        if num_total!=0:
            acc_pre=acc_total/num_total 
        if num_cluster!=0:
            acc_post=acc_cluster/num_cluster
        
        yield None, (acc_pre, acc_post)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.cluster_mapper,
                   reducer=self.cluster_reducer),
            MRStep(mapper=self.top5_mapper,
                    reducer=self.top5_reducer)
            ]


if __name__ == '__main__':
    cluster.run()
