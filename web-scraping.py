import pandas as pd
import csv
import sys

from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime, date, timedelta

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def main(argv):
    r = urlopen("https://www.eia.gov/dnav/ng/hist/rngwhhdD.htm")
    # This line opens the webpage
    soup = BeautifulSoup(r, 'lxml')
    # uses Beautiful soup to get all page source

    table = soup.findAll("table", attrs={"cellpadding": "2"})[0]
    # used to find the table with the attribute cellpadding=2
    # TODO: confusion on use of [0]

    csvFile = open("extracted_data.csv", 'w')
    # opens a file "extracted_data.csv" in write mode.
    # If the file exists then the previous data is deleted.
    # If the file doesn't exist then new file is created.

    for tr in table.findAll("tr"):
    # find all the tr in the table object(variable) that was created
        csvRow = []
        for th in tr.findAll("th"):
        # find all the th in the tr object(variable) from the outer loop
            writer = csv.writer(csvFile)
            # prepare to write in 'csvFile'
            csvRow.append(th.get_text().strip().replace('-', ' ').replace('  ', ' '))
            # th.get_text() === get the data contained in the th tag.
            # .strip() === The strip() method returns a copy of the string with both leading and trailing characters removed (based on the string argument passed).
            # here using .strip() unnecessary leading and trailing spaces are removed
            # .replace('-', ' ') === replaces any '-' found with ' '(single space).
            # .replace('  ', ' ') === replaces any '  '(double space) found with ' '(single space).
        for td in tr.findAll("td"):
        # find all the td in the tr object(variable) from the outer loop
            writer = csv.writer(csvFile)
            csvRow.append(td.get_text().strip().replace('-', ' ').replace('  ', ' '))
            # td.get_text() === get the data contained in the td tag.
            # .strip() === The strip() method returns a copy of the string with both leading and trailing characters removed (based on the string argument passed).
            # here using .strip() unnecessary leading and trailing spaces are removed
            # .replace('-', ' ') === replaces any '-' found with ' '(single space).
            # .replace('  ', ' ') === replaces any '  '(double space) found with ' '(single space).
        print(csvRow)
        writer.writerow(csvRow)
        # use writer to write in the data in csvRow to the csvFile
    csvFile.close()
    # closes the file open in csvFile object.


    df = pd.read_csv('extracted_data.csv')
    # read the csv file created above steps with pandas dataframe.
    df.dropna(axis=0, how='all', inplace=True)
    # since there were multiple empty lines in the extracted data.
    # removing the rows if all the data cell are empty.
    # axis=0 === says that we are selecting rows whereas axis=1 means columns.
    # how='all' === says to remove the row only if all data are missing.
    # inplace=True === says to make and save edits to the same dataframe.
    df.reset_index(inplace=True, drop=True)
    # resets the index to serialize the index order where some values were missing due to deleted rows from previous lines.
    dd = df.iloc[:,1:7]
    # .iloc[] is primarily integer position based (from 0 to length-1 of the axis), but may also be used with a boolean array.
    # from the df DataFrame selecting all the rows and columns 1,2,3,4,5,6( since in 1:7, 7 is exclusive).
    # and storing the result in a new DataFrame dd

    vals=[]
    days=[]
    # variables to store the values of columns for required dataframe format.

    for x in dd.index:
        vals.append(df['Mon'][x])
        vals.append(df['Tue'][x])
        vals.append(df['Wed'][x])
        vals.append(df['Thu'][x])
        vals.append(df['Fri'][x])
        # To append data extracted from Columns Mon-Fri serrially into the variable "vals"

        review_date = df['Week Of'][x]
        # Reads the date range given in the column "Week Of"
        review_date = review_date.split()
        # ".split()" makes it so that when we iterate through the data in the "review_date" variable, we read one word at a time rather than 1 character at a time.

        y = []
        for date_list in review_date:
            if (date_list == "to"):
                continue
            else:
                y.append(date_list)
        # for every element(word) in "review_date", appending the word into a list and if we get a word "to" then we skip it.
        
        min_date = y[0] + " " + y[1] + " " + y[2]
        # the extracted data of "Week of" is like "1997 Jan 6 to Jan 10"
        # so taking the first 3 strings(words) from the data we make "min_date" which is the staring office day for office

        begin_object = datetime.strptime(min_date, "%Y %b %d")
        # print(begin_object)
        # With the "min_date" variable, making it a datetime object with " datetime.strptime(min_date, "%Y %b %d") "
        #  "%Y %b %d" this part is to specify the format of datetime object values stored in the variable "min_date"
        
        end_object = begin_object + timedelta(days=5)
        # print(end_object)
        # since from Monday to Friday its 5 days, adding 5 days with "timedelta(days=5)" to begin_object, which is the start date
        # Here we get the end date of the week.

        for single_date in daterange(begin_object, end_object):
            date_oneval = single_date.strftime("%Y %m %d %a")
            days.append(date_oneval)
        # This block of code is to automatically generate dates between the "begin_object" and "end_object" and append/store it into the list "days"

    data = [list(x) for x in zip(days, vals)]
    # print(data)
    # This line of code takes the two lists "days" and "vals", then zips it together into a single list.
    # For examp[le if we have two lists a=[1,2,3,4] and b=[1,4,9,16]
    # This line of code gives the output c=[[1,1],[2,4],[3,9],[4,16]]

    daa = pd.DataFrame(data, columns = ['Date', 'Dollars per Million Btu'])
    # Defining the columns for the new dataframe.
    # and inserting the data.
    daa.to_csv('new_req.csv', index=True)
    # Creating a new csv file with the dataframe created.

if (__name__ == "__main__"):
    main(sys.argv)