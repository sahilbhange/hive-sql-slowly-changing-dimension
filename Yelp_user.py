#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
@author: sbhange
"""

import pandas as pd


# Read the yelp_user file ti dataframe
yelp_user_orig = pd.read_csv('/Users/sbhange/yelp_data/yelp_user.csv', keep_default_na=False)



# Create file for month 20180731
yelp_user_20180731 = yelp_user_orig[['user_id',
                                     'name',
                                     'review_count',
                                     'yelping_since',
                                     'useful',
                                     'funny',
                                     'cool',
                                     'fans',
                                     'elite',
                                     'average_stars',
                                     'compliment_hot',
                                     'compliment_more',
                                     'compliment_profile',
                                     'compliment_cute',
                                     'compliment_list',
                                     'compliment_note',
                                     'compliment_plain',
                                     'compliment_cool',
                                     'compliment_funny',
                                     'compliment_writer',
                                     'compliment_photos'
                                     ]]

# Add the date column and set the date
yelp_user_20180731['date']='2018-07-31'

yelp_user_20180731.to_csv('/Users/sbhange/yelp_data/yelp_user_20180731.csv')


# Create file for month 20180831

yelp_user_20180831 = yelp_user_orig[['user_id',
                                     'name',
                                     'review_count',
                                     'yelping_since',
                                     'useful',
                                     'funny',
                                     'cool',
                                     'fans',
                                     'elite',
                                     'average_stars',
                                     'compliment_hot',
                                     'compliment_more',
                                     'compliment_profile',
                                     'compliment_cute',
                                     'compliment_list',
                                     'compliment_note',
                                     'compliment_plain',
                                     'compliment_cool',
                                     'compliment_funny',
                                     'compliment_writer',
                                     'compliment_photos'
                                     ]]

# Add the date column and set the date
yelp_user_20180831['date']='2018-08-31'

# Divide the data frame into 2 data sets
yelp_user_20180831_1325600 = yelp_user_20180831[:1325600]

yelp_user_20180831_500 = yelp_user_20180831[1325600:]

# Dummy update the average_stars field 
yelp_user_20180831_500['average_stars'] = 4.5

# Dummy update the fans field 
yelp_user_20180831_500['fans'] = 10

# Dummy update the elite field 
yelp_user_20180831_500['elite'] = '2015 2016 2017'

# Dummy update the useful field 
yelp_user_20180831_500['useful'] = 2500

# Manually edit/add new records to datarframe for new user entry every month

yelp_user_20180831_updated = pd.concat([yelp_user_20180831_1325600, yelp_user_20180831_500], axis=0)

yelp_user_20180831_updated.to_csv('/Users/sbhange/yelp_data/yelp_user_20180831.csv')


# Create file for month 20180930

yelp_user_20180930 = yelp_user_orig[[  'user_id',
                         'name',
                         'review_count',
                         'yelping_since',
                         'useful',
                         'funny',
                         'cool',
                         'fans',
                         'elite',
                         'average_stars',
                         'compliment_hot',
                         'compliment_more',
                         'compliment_profile',
                         'compliment_cute',
                         'compliment_list',
                         'compliment_note',
                         'compliment_plain',
                         'compliment_cool',
                         'compliment_funny',
                         'compliment_writer',
                         'compliment_photos'
                         ]]

# Add the date column and set the date
yelp_user_20180930['date']='2018-09-30'

# Divide the data frame into 2 data sets
yelp_user_20180930_1325600 = yelp_user_20180930[:1325600]

 
yelp_user_20180930_500 = yelp_user_20180930[1325600:]


# Dummy update the average_stars field 
yelp_user_20180930_500['average_stars'] = 3.5

# Dummy update the fans field
yelp_user_20180930_500['fans'] = 20

# Dummy update the elite field 
yelp_user_20180930_500['elite'] = '2015 2016 2018'

# Dummy update the fans useful
yelp_user_20180930_500['useful'] = 3000


# Manually edit/add new records to datarframe

yelp_user_20180930_updated = pd.concat([yelp_user_20180930_1325600, yelp_user_20180930_500], axis=0)

yelp_user_20180930_updated.to_csv('/Users/sbhange/yelp_data/yelp_user_20180930.csv')


# Create file for month 20181031

yelp_user_20181031 = yelp_user_orig[[  'user_id',
                         'name',
                         'review_count',
                         'yelping_since',
                         'useful',
                         'funny',
                         'cool',
                         'fans',
                         'elite',
                         'average_stars',
                         'compliment_hot',
                         'compliment_more',
                         'compliment_profile',
                         'compliment_cute',
                         'compliment_list',
                         'compliment_note',
                         'compliment_plain',
                         'compliment_cool',
                         'compliment_funny',
                         'compliment_writer',
                         'compliment_photos'
                         ]]


yelp_user_20181031_500['useful'] = 3400
181031 = pd.read_csv('/Users/sbhange/yelp_data/yelp_user_20181031.csv', keep_default_na=False)

yelp_user_20181031['date']='2018-10-31'

list(yelp_user_20181031)

yelp_user_20181031_1325600 = yelp_user_20181031[:1325600]

yelp_user_20181031_500 = yelp_user_20181031[1325600:]

yelp_user_20181031_500['average_stars'] = 4

yelp_user_20181031_500['fans'] = 40

yelp_user_20181031_500['elite'] = '2015 2017 2018'

# Manually edit/add new records to datarframe

yelp_user_20181031_updated = pd.concat([yelp_user_20181031_1325600, yelp_user_20181031_500], axis=0)

yelp_user_20181031_updated.to_csv('/Users/sbhange/yelp_data/yelp_user_20181031.csv')




##  Add partition field in the hive table


len(yelp_user_20180731)

yelp_user_20180731_442000 = yelp_user_20180731[0:442000]


yelp_user_20180731_884100 = yelp_user_20180731[442000:884100]


yelp_user_20180731_1326100 = yelp_user_20180731[884100:]


yelp_user_20181031_updated = pd.concat([yelp_user_20181031_1325600, yelp_user_20181031_500], axis=0)
                                               
yelp_user_part_20180731



temp = yelp_user_20180731[0:2000]










