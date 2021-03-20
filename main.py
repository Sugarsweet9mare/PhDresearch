# Barebones Algorithm
"""
creates file for each data element
while (not end of message)
    for each message
        for each data element
            if (pattern match for corresponding data element)
                append data into the file representing the data element in question

#to show table
creates table
    for each data element file
        new column with all the test results in order

#query data
fetch from the row number/ID in the table

"""

# In-depth Algorithm
"""
Involves using a python library: hl7 to parse HL7 v2.x messages
the parse() command in the hl7 library is able to parse the string of messages into 
a hl7.Message object which can be accessed as a list 
each segments, ie: MSH, SFT, PID etc. is separated into different lists 
and the fields and components separated into further lists  

For example: 
message = 'MSH|^~\&|GHH LAB|ELAB-3|GHH OE|BLDG4|200202150930||ORU^R01|CNTRL-3456|P|2.4\r'
message += 'PID|||555-44-4444||EVERYWOMAN^EVE^E^^^^L|JONES|196203520|F|||153 FERNWOOD DR.^^STATESVILLE^OH^35292||(206)3345232|(206)752-121||||AC555444444||67-A4335^OH^20030520\r'
message += 'OBR|1|845439^GHH OE|1045813^GHH LAB|1554-5^GLUCOSE|||200202150730||||||||555-55-5555^PRIMARY^PATRICIA P^^^^MD^^LEVEL SEVEN HEALTHCARE, INC.|||||||||F||||||444-44-4444^HIPPOCRATES^HOWARD H^^^^MD\r'
message += 'OBX|1|SN|1554-5^GLUCOSE^POST 12H CFST:MCNC:PT:SER/PLAS:QN||^182|mg/dl|70_105|H|||F\r'

import hl7
h = hl7.parse(message)
h[2][0][4][0][1] ==
[['OBX'], ['1'], ['SN'], [[['1554-5'], ['GLUCOSE'], ['POST 12H CFST:MCNC:PT:SER/PLAS:QN']]], [''], [[[''], ['182']]], ['mg/dl'], ['70_105'], ['H'], [''], [''], ['F']]

#Storing data extracted from the hl7 messages 
creates file for each data element
while (not end of message)
    for each message
        parse the message with parse() command in hl7 library
        for each data element
            get the data by querying location in the list that matches the data's location in the hl7 message
            (for example: to get HOSPITAL ID we can query for parsed_message[2][0])
            append data into the file representing the data element in question

#to show table
creates table
    for each data element file
        new column with all the test results in order

#query data
fetch from the row number/ID in the table

"""

# Learning about scikit learn machine learning library in python
# One of the more encompassing libraries for machine learning as easier to learn
# Supports classification which I think is the kind of machine learning going to be used for this project the most

import hl7
import pandas as pd
import csv
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import OneHotEncoder
import numpy
import os


def read_by_chunk(filename):
    """
    Reads a HL7 encoded text file in chunks?


    :param filename: File path to HL7 file.
    """
    with open(filename, "r") as f:

        message = ""
        for line in f:
            # Each message is separated an empty line
            if line == "\n":
                yield message
                message = ""

            else:
                line = line.rstrip()
                line += "\r"
                message += line

        yield message

"""
def read_by_chunk(file):
    for line in file:
        if line is not newline:
            strip /r from the line and appends to a list
        
        if line is newline:
            yield the concatenated list
    returns list 
    
def init_dataframe(file) 
    for chunk in read_by_chunk(file): 
        runs chuck through hl7 parser
        
        extract data from each category
    
    create dataframe from the data extracted
    dataframe.to_csv()

"""


def init_dataframe(file):
    patient_med_rec_num = []
    patient_DOB = []
    patient_sex = []
    patient_race = []
    patient_lang = []
    marital = []
    test_name = []
    test_result = []
    unit_measure = []
    col_date = []
    set_ID = []
    dia_code = []
    dia_desc = []

    for chunk in read_by_chunk(file):
        parsed = hl7.parse(chunk)

        # PID3.1 Patient Medical Record Number
        try:
            patient_med_rec_num.append(parsed.segments("PID")[0][3])
        except:
            patient_med_rec_num.append("")

        # PID7 Patient's DOB
        try:
            patient_DOB.append(parsed.segments("PID")[0][7])
        except:
            patient_DOB.append("")

        # PID8 Patient's Sex
        try:
            patient_sex.append(parsed.segments("PID")[0][8])
        except:
            patient_DOB.append("")

        # PID10 Race
        try:
            patient_race.append(parsed.segments("PID")[0][10])
        except:
            patient_race.append("")

        # PID15 Primary Language
        try:
            patient_lang.append(parsed.segments("PID")[0][15])
        except:
            patient_lang.append("")

        # PID16 Marital Status
        try:
            marital.append(parsed.segments("PID")[0][16])
        except:
            marital.append("")

        # OBX3.2 Test Name
        try:
            test_name.append(parsed.segments("OBX")[0][3])
        except:
            test_name.append("")

        # OBX5.1 Test Result
        try:
            test_result.append(parsed.segments("OBX")[0][5])
        except:
            test_result.append("")

        # OBX6 Unit of Measure
        try:
            unit_measure.append(parsed.segments("OBX")[0][6])
        except:
            unit_measure.append("")

        # OBX19 Collection Date/Time
        try:
            col_date.append(parsed.segments("OBX")[0][14])
        except:
            col_date.append("")

        # DG1.1 Set ID
        try:
            set_ID.append(parsed.segments("DG1")[0][1])
        except:
            set_ID.append("")

        # DG1.3 Diagnosis Code
        try:
            dia_code.append(parsed.segments("DG1")[0][3])
        except:
            dia_code.append("")

        # DG1.4 Diagnosis Description
        try:
            dia_desc.append(parsed.segments("DG1")[0][4])
        except:
            dia_desc.append("")

    data = {'Patient Medical Record Number': patient_med_rec_num, "Patient's DOB": patient_DOB,
            "Patient's Sex": patient_sex, "Race": patient_race, "Primary Language": patient_lang,
            "Marital Status": marital, 'Test Name': test_name, 'Test Result': test_result,
            'Unit of Measure': unit_measure, 'Collection Date/Time': col_date, 'Set ID': set_ID,
            'Diagnosis Code': dia_code, 'Diagnosis Description': dia_desc}
    df = pd.DataFrame(data)
    df.to_csv('full.csv', index=False)


def read_to_lst(filename):
    with open(filename, errors="ignore", newline='') as f:
        reader = csv.reader(f)
        data = list(reader)

    return data


def cluster_split(clusterLabels, inputFileList):
    clusters = []
    header = inputFileList[0]
    inputFileList = inputFileList[1:]
    for i in clusterLabels:
        if i not in clusters:
            clusters.append(i)

    for i in range(len(clusters)):
        with open('cluster ' + str(i) + '.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            for j in range(len(clusterLabels)):
                if clusterLabels[j] == i:
                    writer.writerow(inputFileList[j])


def file_split(file, lines):
    with open(file, errors="ignore", newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
        header = data[0]
        data = data[1:]

    filename = 1
    for i in range(len(data)):
        if i % lines == 0:
            with open(str(filename) + '.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(header)
                writer.writerows(data[i:i + lines])
            filename += 1


def cluster(file, eps):
    """
    :param file: The file to be clustered
    :param eps: The maximum distance between two samples for one to be considered as in the neighborhood of the other.
    """
    # reads the file into a list
    listed_df = read_to_lst(file)
    # encodes the list using one hot encoding
    ohe = OneHotEncoder(categories='auto')
    feature_arr = ohe.fit_transform(listed_df[1:]).toarray()

    numpy.set_printoptions(threshold=numpy.inf)
    # clusters the encoded data
    cluster = DBSCAN(eps=eps).fit(feature_arr)
    # splits the input file according to the clusters
    cluster_split(cluster.labels_, listed_df)


def chart(file, total):
    total = 3517
    with open(file, errors="ignore", newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
        header = data[0]
        data = data[1:]


def piechart():
    number = []
    my_labels = []
    my_labels2 = []
    my_labels3 = []
    my_labels4 = []

    directory = "D:\\Codes\\Phd research\\Clusters\\Clustered clusters"
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                with open(os.path.join(directory, file)) as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter=',')
                    next(csv_reader, None)
                    for row in csv_reader:
                        temp = []
                        temp.append(row)

                        my_labels.append(temp[0][2])
                        my_labels2.append(temp[0][3])
                        my_labels3.append(temp[0][4])
                        my_labels4.append(temp[0][5])

                        break
                    number.append(len(list(csv_reader)) + 2)

    # labels = list(my_labels)
    # plt.pie(number, labels=labels, autopct='%1.1f%%')
    # plt.title(chart_name)
    # plt.axis('equal')
    # plt.savefig(file_name)

    df1 = {

        "Patient's Sex": my_labels,
        'Race': my_labels2,
        'Primary Language': my_labels3,
        'Marital Status': my_labels4,
        'Number': number

    }
    df1 = pd.DataFrame(df1, columns=["Patient's Sex", 'Race', 'Primary Language', 'Marital Status', 'Number'])

    df1['Percentage'] = (df1['Number'] / df1['Number'].sum()) * 100
    df1.to_csv('chart3.csv', index=False)


def percentage(index):
    list = []
    all = []
    total = 0
    with open("D:\\Codes\\Phd research\\Clusters\\Clustered clusters\\cluster 1.csv") as file:
        csv_reader = csv.reader(file, delimiter=',')

        # next(csv_reader,None)

        for row in csv_reader:
            total += 1
            if row[index] == "Diagnosis Description":
                pass
            elif row[index] in all:
                list[all.index(row[index])][2] += 1
            else:
                all.append(row[index])
                list.append([row[index], row[index - 1], 1])

    for i in list:
        print(i, (i[2] / total) * 100)


"""
def percentage(index):
    opens the directory of the clusters csv files
    loops through all .csv files in the directory:
        gets the column/columns that the files are clustered by 
        calculate the number of entries in each file 
        calculate the percentage that each cluster occupies 
    creates a dataframe of all the percentages and column entries
    outputs a .csv file with the dataframe

"""

percentage(12)

# pd.set_option("display.max_rows", None, "display.max_columns", None)
# file_split("cluster num 0.csv", 10000)
# cluster("1.csv", 2.6)


# combine all files in the list

# Finded_URL = ["cluster 1.csv","cluster 3.csv","cluster 4.csv", "cluster 5.csv"]
# combined_csv = pd.concat([pd.read_csv(f,header=None) for f in Finded_URL])
# combined_csv.head()
# combined_csv.to_csv( "newcluster.csv", quotechar='"', quoting=csv.QUOTE_ALL, index=False, encoding='utf-8')
