from os import listdir
import pandas as pd
import logging
import time
import pymongo
from pymongo import MongoClient


logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler('app.log', mode='a'),
        logging.StreamHandler()
    ]
)

logging.info("start of an app")

def find_csv_filenames( path_to_dir, suffix=".csv" ):
    """
    This function returns list of names of csv files from give directory
    """
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]


def gather_column_names():
    """
    this function geathers names of columns from different csv files,
    transforms those column names, so they fall under some standart
    and return list of column names which will be added to the database
    """

    filename_list = find_csv_filenames("../zno")
    df_columns_list = list()

    for name in filename_list:
        temp_df = pd.read_csv(f"../zno/{name}", sep=';', error_bad_lines=False, nrows=10)
        #make columns names lower case, so it will be easier to intersect them
        column_list = [each_string.lower() for each_string in list(temp_df.columns)]
        df_columns_list.append(column_list)

    df0_df1_intesection = [x for x in df_columns_list[1] if x in df_columns_list[0]]

    return df0_df1_intesection


def transorm_chunk_columns(chunk, df_columns_list, zno_year):
    """
    This function takes chunk from dataframe, drops unnecessary columns,
    makes names of columns lowercase 
    """
    chunk.columns = chunk.columns.str.strip().str.lower()
    #find those columns that are in chunk columns but not in df column list
    chunk_excess_columns = [x for x in list(chunk.columns) if x not in df_columns_list]
    for column in chunk_excess_columns:
        if column in chunk.columns:
            chunk.drop(column, inplace=True, axis=1)

    chunk["year"] = zno_year
    #chunk.loc[:, ~chunk.columns.str.contains("adaptscale")]
    chunk.drop(list(chunk.filter(regex = 'adaptscale')), axis = 1, inplace = True)

    if zno_year == 2021:
        cols_to_convert_to_int = list(chunk.filter(regex = 'ball100'))
        chunk[cols_to_convert_to_int] = chunk[cols_to_convert_to_int].apply(lambda x: x.str.replace(',','.')).apply(pd.to_numeric)


    chunk.rename(columns = {'outid':'_id'}, inplace = True)

    return chunk.to_dict(orient='records')

def get_digits_from_string(string):
    temp_str = ""
    for char in string:
        if char.isdigit():
            temp_str = temp_str + char

    return temp_str


def get_chunk_and_file_counter_from_log(filename):

    '''
    this function checks if  insert chunk error substring in lines of log file
    if that is the case it returns tuple of numbers of line and chunk we should start our prog with, 
    if not it returns two zeroes. Also this function checks if the last run of the program was not a success.
    if it was, than the function will return (0,0) so the program can be started over without any problems
    '''
    last_line = ""
    with open(filename, 'r') as f:
        lines = f.readlines()
        if len(lines) < 2:
            return (0,0,0)
        if "success" in lines[-2]:
            return (0, 0, 0)
        for line in lines:
            print
            if "InsertChunkError" in line:
                last_line = line

    #if that file contains information about previous breakdown, than we can set variables as those values
    #if not we set them as zero
    if len(last_line) == 0:
        logged_file_counter = 0
        logged_chunk_counter = 0
        logged_line_counter = 0
    else:
        comprehension = [int(s) for s in last_line.split() if s.isdigit()]
        logged_file_counter = comprehension[0]
        logged_chunk_counter = comprehension[1]
        logged_line_counter = comprehension[2] 

    return (logged_file_counter, logged_chunk_counter, logged_line_counter) 


def run_mongo_command():
    '''
    this function runs needed mongo db sequence
    '''
    client = MongoClient("localhost", 27017)
    db_name = "fourth_lab_db"
    coll_name = "zno"

    db = client[db_name]
    coll = db[coll_name]

    result_dict = dict()
    result_dict["regname"] = list()
    result_dict["maxball2018"] =  list()
    result_dict["maxball2021"] =  list()



    agg_result_for_2018 = coll.aggregate(
    [
    {"$match": { "$or": [{"histteststatus": "Зараховано", "year": 2018}]}},
    {"$group": {"_id": "$regname", "maxball": {"$max" : "$histball100"}}}
    ])

    agg_result_for_2021 = coll.aggregate(
    [
    {"$match": { "$or": [{"histteststatus": "Зараховано", "year": 2021}]}},
    {"$group": {"_id": "$regname", "maxball": {"$max" : "$histball100"}}}
    ])

    for line in agg_result_for_2018:
        result_dict["regname"].append(line["_id"])
        result_dict["maxball2018"].append(line["maxball"])

    for line in agg_result_for_2021:
        result_dict["maxball2021"].append(line["maxball"])

    df = pd.DataFrame.from_dict(result_dict) 
    print(df)
    df.to_csv("result_query.csv", index=False)


def main():

    client = MongoClient("localhost", 27017)
    db_name = "fourth_lab_db"
    coll_name = "zno"
    dict_line_counter = 0

    db = client[db_name]
    coll = db[coll_name]

    filename_list = find_csv_filenames("../zno")
    df_columns_list = gather_column_names()
    logged_numbers = get_chunk_and_file_counter_from_log("app.log")

    chunk_size = 50000
    file_counter = 0
    chunk_counter = 0
    line_counter = 0
    dict_line_counter = 0

    #looping over files, getting chunks, from those files as dataframes and writing those dataframes into a db
    for name in filename_list:
        file_counter += 1

        zno_year = get_digits_from_string(name)

        if file_counter < logged_numbers[0]:
            print("skipped one file")
            continue

        else:
            try:
                start_time = time.time()
                for chunk in pd.read_csv(f"../zno/{name}", sep=';', error_bad_lines=False, chunksize=chunk_size, low_memory=False):
                    chunk_counter += 1
                    if chunk_counter < logged_numbers[1]:
                        print("skipped one chunk")
                        continue
                    else:
                        transformed_chunk = transorm_chunk_columns(chunk, df_columns_list, int(zno_year))
                        for line in transformed_chunk:
                            dict_line_counter += 1
                            if dict_line_counter < logged_numbers[2]:
                                print("skipped one line")
                                continue
                            else:
                                coll.insert_one(line)

                        dict_line_counter = 0


                        print("inserted one chunk")

                chunk_counter = 0
                logging.info(f'finished handling file: {name}; ' f'took time: {round(time.time() - start_time)} seconds')
                logging.info("success")


            except Exception as e:
                logging.error(f"InsertChunkError {file_counter} {chunk_counter} {dict_line_counter}")
                print(e)


if __name__ == '__main__':
    main()
    run_mongo_command()


