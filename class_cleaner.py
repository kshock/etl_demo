"""

@author: kshock
"""

import pandas as pd
import os
from db_connector import DataBaseConn

    

class_file = pd.read_csv(os.getcwd() + '/class.csv', sep=';')
test_file = pd.read_csv(os.getcwd() + '/test.csv', sep=';')
test_lvl = pd.read_csv(os.getcwd() + '/test_level.csv', sep=';')

test_data = test_file.copy()
test_data = test_data[['id', 'class_id', 'created_at', 
                                     'authorized_at', 'test_level_id']]
class_file.rename(columns={'id':'class_id', 'name':'class_name'}, inplace=True)
test_data = test_data.join(class_file[['class_name', 'teaching_hours']], 
                                         on='class_id')
test_data.rename(columns={'id':'test_id', 'created_at':'test_created_at',
                                 'authorized_at':'test_authorized_at', 
                                 'test_level_id':'test_level'}, inplace=True)
test_data[['test_created_at', 'test_authorized_at']] = test_data[['test_created_at', 
                                                                  'test_authorized_at']].apply(lambda x: 
                                                                                               pd.to_datetime(x, 
                                                                                                              format='%d.%m.%y %H:%M'))

test_data.dropna(subset=['test_authorized_at'], inplace=True)


test_data[['test_created_at', 'test_authorized_at']] = test_data[['test_created_at', 
                                                                  'test_authorized_at']].applymap(lambda day: 
                                                                                                  day.strftime('%Y-%m-%d'))



test_data_cleaned = test_data.copy()
test_data_cleaned.dropna(subset=['class_name'], inplace=True)
test_data_cleaned = test_data_cleaned[['class_id', 'class_name', 'teaching_hours', 'test_id', 
                                     'test_level', 'test_created_at', 'test_authorized_at']]
test_data_cleaned.reset_index(drop=True, inplace=True)
id_class = test_data_cleaned['class_id'].drop_duplicates()
id_class = id_class.tolist()


def get_test_class(value_of_class_id):
    df = test_data_cleaned[test_data_cleaned['class_id'] == value_of_class_id]
    df['class_test_number'] = list(range(1, len(df)+1))
    
    return df

tests_in_classes = [get_test_class(test) for test in id_class]
test_utilization = pd.concat(tests_in_classes, ignore_index=True)


test_avg = test_data.join(test_file[['overall_score', 'test_status']], on='class_id')
test_avg.dropna(subset=['test_authorized_at'], inplace=True)
test_avg = test_avg[test_avg['test_status'] == 'SCORING_SCORED']
test_avg.dropna(subset=['overall_score'], inplace=True)
test_avg.reset_index(drop=True, inplace=True)
test_average_scores = test_avg.groupby(['class_id', 'class_name',
                                        'teaching_hours','test_created_at',
                                        'test_authorized_at']).agg({'overall_score':'mean'})
test_average_scores.rename(columns={'overall_score':'avg_class_test_overall_score'},
                           inplace=True)


test_utilization.to_csv(os.getcwd() + '/test_utilization.csv', sep=';', index=False)
test_average_scores.to_csv(os.getcwd() + '/test_average_scores.csv', sep=';')



'''
db = DataBaseConn('user', 'passworld', 'host', 'port', 'db', test_utilization)
db.load_to_sqile('table_test_utilization')
'''







