import pandas
import os
import sys
import requests
import json
from nltk import word_tokenize
import re
import random
import matplotlib.pyplot as plt
from liwc import *
from lsm import *

# given a comment url under a Reddit post, grab all
# of its child comment threads. 
def thread_builder(comment_urls):
    # save index of comment url
    idx = 0
    # store the threads that offshoot each comment in the dataset
    comment_threads_dict = dict.fromkeys(comment_urls)
    for link in comment_urls:
        try: 
            url = 'https://www.reddit.com' + str(link) + '.json'
            data = requests.get(url, headers={'User-Agent': 'ripperino/1.0'},timeout=10).json()
            post_data = data[0]
            comment_data = data[1]
            # iterate thru top-level comments and add replies to comment_container as you go
            # i.e. treat comment_container as a queue
            comment_container = []
            # initialize comment_container with top level comments
            for comment in comment_data['data']['children']:
                comment_container.append(comment)
            # add remaining replies
            for comment in comment_container:
                if 'replies' in comment['data']:
                    if comment['data']['replies']!="":
                        for reply in comment['data']['replies']['data']['children']:
                            comment_container.append(reply)
            # iterate thru all comments and build threads
            import copy
            thread_container = {}
            for comment in comment_container:
                # only include relevant metadata
                mod_comment = {'data':{}}
                mod_comment['data']['name'] = comment['data']['name']
                mod_comment['data']['parent_id'] = comment['data']['parent_id']
                mod_comment['data']['total_awards_received'] = comment['data']['total_awards_received']
                mod_comment['data']['likes'] = comment['data']['likes']
                mod_comment['data']['body'] = comment['data']['body']
                mod_comment['data']['created_utc'] = comment['data']['created_utc']
                mod_comment['data']['author_flair_text'] = comment['data']['author_flair_text']
                mod_comment['data']['permalink'] = comment['data']['permalink']
                mod_comment['data']['edited'] = comment['data']['edited']
                # check for the current comment's parent in the thread_container
                # (this works since the first k comments in comment_container are top level comments)
                if mod_comment['data']['parent_id'] in thread_container.keys():
                    # create a copy of the thread we want to expand
                    thread_copy = copy.deepcopy(thread_container[mod_comment['data']['parent_id']])
                    thread_copy.append(mod_comment)
                    # add expanded thread to thread_container with the latest comment's id as the key
                    thread_container[mod_comment['data']['name']] = thread_copy
                # if current comment doesn't have parent_id in thread_container, 
                # it's a top-level comment so just start a new thread
                else:
                    # the first comment in a thread should always be the one w/ target question
                    thread_container[mod_comment['data']['name']] = [mod_comment]
            # remove partial threads
            full_threads = []
            for out_key in thread_container.keys():
                partial_thread = False
                # don't compare thread w/ itself
                for in_key in thread_container.keys():
                    if in_key != out_key:
                        if (is_partial_thread(thread_container[out_key],thread_container[in_key])==True):
                            partial_thread = True
                if partial_thread == False:
                    # add the the thread to full_threads
                    full_threads.append(thread_container[out_key])
            # update index in dataset comment iteration
            idx += 1
            print('Progress: ',idx)
            # save all offshoot threads for the target comment
            comment_threads_dict[link] = full_threads
        except Exception as e:
            comment_threads_dict[link] = []
            print('Error msg: ',str(e))
            print('Error msg coming from: ',link)

    return comment_threads_dict

# given two threads, determine whether or not thread1 
# is a partial thread 'inside' of thread2
def is_partial_thread(thread1, thread2):
    if(all(x in thread2 for x in thread1)):
        return True
    return False

# get random sample of n rows from dataframe
# wrote this before finding about DataFrame.sample()
def collect_random_data(df, num_random):
    inds_list = [*range(0,len(df))]
    random.shuffle(inds_list)
    rand_df = df.iloc[inds_list[:num_random]]
    return rand_df

def get_thread_q_stats(threads_dict,comments_incl_qtypes_dict):
    # get stats
    thread_lengths = 0
    num_threads = 0
    num_comments = 0

    num_threads_IS = 0
    num_threads_R = 0
    num_threads_NA = 0

    num_comments_IS = 0
    num_comments_R = 0
    num_comments_NA = 0

    thread_lengths_IS = 0
    thread_lengths_R = 0
    thread_lengths_NA = 0

    thread_lengths_IS_vec = []
    thread_lengths_R_vec = []
    thread_lengths_NA_vec = []

    num_threads_IS_vec = []
    num_threads_R_vec = []
    num_threads_NA_vec = []

    for key in threads_dict.keys():
        print('STATS FOR ', key, ':')
        if threads_dict[key]!=None:
            if 'IS' in comments_incl_qtypes_dict[key]:
                num_threads_IS_vec.append(len(threads_dict[key]))
            if 'R' in comments_incl_qtypes_dict[key]:
                num_threads_R_vec.append(len(threads_dict[key]))
            if 'not_applicable' in comments_incl_qtypes_dict[key]:
                num_threads_NA_vec.append(len(threads_dict[key]))
            print('num threads: ',len(threads_dict[key]))
            for thread in threads_dict[key]:
                thread_lengths += len(thread)
                print('len thread: ',len(thread))
                num_threads += 1
                if 'IS' in comments_incl_qtypes_dict[key]:
                    num_threads_IS += 1
                    thread_lengths_IS += len(thread)
                    thread_lengths_IS_vec.append(len(thread))
                if 'R' in comments_incl_qtypes_dict[key]:
                    num_threads_R += 1
                    thread_lengths_R += len(thread)
                    thread_lengths_R_vec.append(len(thread))
                if 'not_applicable' in comments_incl_qtypes_dict[key]:
                    num_threads_NA += 1
                    thread_lengths_NA += len(thread)
                    thread_lengths_NA_vec.append(len(thread))
        num_comments += 1
        if 'IS' in comments_incl_qtypes_dict[key]:
            num_comments_IS += 1
        if 'R' in comments_incl_qtypes_dict[key]:
            num_comments_R += 1
        if 'not_applicable' in comments_incl_qtypes_dict[key]:
            num_comments_NA += 1

    print('Average thread length: ', thread_lengths/num_threads)
    print('Average num threads per comment: ', num_threads/num_comments)

    print('Average IS thread length: ', thread_lengths_IS/num_threads_IS)
    print('Average num IS threads per comment: ', num_threads_IS/num_comments_IS)

    print('Average R thread length: ', thread_lengths_R/num_threads_R)
    print('Average num R threads per comment: ', num_threads_R/num_comments_R)

    print('Average NA thread length: ', thread_lengths_NA/num_threads_NA)
    print('Average num NA threads per comment: ', num_threads_NA/num_comments_NA)

    # visualize the number of offshoot threads from comments of each type of q
    all_num_threads = [num_threads_IS_vec, num_threads_R_vec, num_threads_NA_vec]
    
    fig, ax = plt.subplots()
    ax.boxplot(all_num_threads,showfliers=True)
    ax.set_ylabel('Number offshoot threads')
    plt.xticks([1,2,3],['IS','R','NA'])
    plt.yticks
    plt.show()

    fig, ax = plt.subplots()
    ax.boxplot(all_num_threads,showfliers=False)
    ax.set_ylabel('Number offshoot threads')
    plt.xticks([1,2,3],['IS','R','NA'])
    plt.yticks
    plt.show()

    # visualize the offshoot thread lengths from comments of each type of q
    all_thread_lengths = [thread_lengths_IS_vec,thread_lengths_R_vec,thread_lengths_NA_vec]

    fig, ax = plt.subplots()
    ax.boxplot(all_thread_lengths,showfliers=True)
    ax.set_ylabel('Thread lengths for offshoot threads')
    plt.xticks([1,2,3],['IS','R','NA'])
    plt.show()

    fig, ax = plt.subplots()
    ax.boxplot(all_thread_lengths,showfliers=False)
    ax.set_ylabel('Thread lengths for offshoot threads')
    plt.xticks([1,2,3],['IS','R','NA'])
    plt.show()

def measure_lsm(threads_dict,comments_incl_qtypes_dict):
    all_depth_lsms = []
    IS_depth_lsms = []
    R_depth_lsms = []
    NA_depth_lsms = []

    all_breadth_lsms = []
    IS_breadth_lsms = []
    R_breadth_lsms = []
    NA_breadth_lsms = []
    # compute all depth LSMs
    word_categories = liwc_keys
    for key in threads_dict.keys():
        if (threads_dict[key]!=None):
            try: 
                first_comment = threads_dict[key][0][0]['data']['body']
                for thread in threads_dict[key]:
                    # depth threads:
                    thread_lsm = compute_thread_LSM(thread,word_categories)
                    all_depth_lsms.append(thread_lsm)
                    if 'IS' in comments_incl_qtypes_dict[key]:
                        IS_depth_lsms.append(thread_lsm)
                    if 'R' in comments_incl_qtypes_dict[key]:
                        R_depth_lsms.append(thread_lsm)
                    if 'not_applicable' in comments_incl_qtypes_dict[key]:
                        NA_depth_lsms.append(thread_lsm)
                        
                    # breadth threads:
                    tmp_all_breadth_lsms = []
                    tmp_IS_breadth_lsms = []
                    tmp_R_breadth_lsms = []
                    tmp_NA_breadth_lsms = []
                    comment_idx = 0
                    for comment in thread:
                        if comment_idx == 1:
                            second_comment = comment['data']['body']
                            pair_lsm = compute_pair_composite_LSM(first_comment, second_comment, word_categories)
                            tmp_all_breadth_lsms.append(pair_lsm)
                            if 'IS' in comments_incl_qtypes_dict[key]:
                                tmp_IS_breadth_lsms.append(pair_lsm)
                            if 'R' in comments_incl_qtypes_dict[key]:
                                tmp_R_breadth_lsms.append(pair_lsm)
                            if 'not_applicable' in comments_incl_qtypes_dict[key]:
                                tmp_NA_breadth_lsms.append(pair_lsm)
                        comment_idx += 1
                    all_breadth_lsms.append(compute_composite_LSM_from_list(tmp_all_breadth_lsms))
                    IS_breadth_lsms.append(compute_composite_LSM_from_list(tmp_IS_breadth_lsms))
                    R_breadth_lsms.append(compute_composite_LSM_from_list(tmp_R_breadth_lsms))
                    NA_breadth_lsms.append(compute_composite_LSM_from_list(tmp_NA_breadth_lsms))
            except Exception as e:
                print('oops error:',e)
                print('moving on!')

    all_depth_lsms = [x for x in all_depth_lsms if x==x]
    IS_depth_lsms = [x for x in IS_depth_lsms if x==x]
    R_depth_lsms = [x for x in R_depth_lsms if x==x]
    NA_depth_lsms = [x for x in NA_depth_lsms if x==x]

    all_breadth_lsms = [x for x in all_breadth_lsms if x==x]
    IS_breadth_lsms = [x for x in IS_breadth_lsms if x==x]
    R_breadth_lsms = [x for x in R_breadth_lsms if x==x]
    NA_breadth_lsms = [x for x in NA_breadth_lsms if x==x]


    # visualize the number of offshoot threads from comments of each type of q
    depth_lsms = [all_depth_lsms, IS_depth_lsms, R_depth_lsms, NA_depth_lsms]
    
    fig, ax = plt.subplots()
    ax.boxplot(depth_lsms,showfliers=False)
    ax.set_ylabel('Composite LSMs of Individual Threads')
    plt.xticks([1,2,3,4],['All','IS','R','NA'])
   # plt.yticks
    plt.show()

    # visualize the number of offshoot threads from comments of each type of q
    breadth_lsms = [all_breadth_lsms, IS_breadth_lsms, R_breadth_lsms, NA_breadth_lsms]
    
    fig, ax = plt.subplots()
    ax.boxplot(breadth_lsms,showfliers=False)
    ax.set_ylabel('Composite LSMs of Direct Responses to Question')
    plt.xticks([1,2,3,4],['All','IS','R','NA'])
   # plt.yticks
    plt.show()



            




def main():
    # read in data
    df = pandas.read_csv('../Data/annot_question_types.csv')
    # comment out when not testing
   # df = df.iloc[:20,]
    comment_urls = df['comment_url'].tolist()
    question_types = df['labeled_question_function'].tolist()
    # fix the n.a. issue
    for i in range(len(question_types)):
        if question_types[i]!=question_types[i]:
            question_types[i] = 'not_applicable'

    # make dictionary for comment_urls mapping to the questions in them 
    comments_incl_qtypes_dict = {}
    for i in range(len(comment_urls)):
        comment_url = comment_urls[i]
        if comment_url not in comments_incl_qtypes_dict.keys():
            comments_incl_qtypes_dict[comment_url] = [question_types[i]]
        else:
            if question_types[i] not in comments_incl_qtypes_dict[comment_url]:
                comments_incl_qtypes_dict[comment_url].append([question_types[i]])

    print('building threads...')
    threads_dict = thread_builder(list(set(comment_urls)))

    # get the LSM metrics
    measure_lsm(threads_dict,comments_incl_qtypes_dict)

    # dump threads to a file
    write_file = False
    if write_file:
        print('writing threads to file...')
        with open('dev_set_comment_threads.json','w+') as outf:
            try:
                json.dump(threads_dict,outf)
            except OSError as error:
                print('error msg: ',str(error))

if __name__ == '__main__':
    main()