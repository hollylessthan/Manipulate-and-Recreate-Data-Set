# Re-create the BBB data (individual assignment)
#
# A dataset like BBB doesn't exist in companies in its raw form
# Someone has to create it first ... likely from different data sources!
#
# The goal of this assignment is to re-create the pandas data frame in bbb.pkl
# data EXACTLY from its components. Follow the steps outlined below:
#
# 1. Determine how to load the different file types (use pd.read_pickel, pd.read_csv,
#    pd.read_excel, and sqlite3.connect)
# 2. Determine what data transformations are needed and how the data should be
#    combined into a data frame. You MUST name your re-created data frame 'bbb_rec'
# 3. Your work should be completely reproducible (i.e., generate the same results on
#    another computer). Think about the 'paths' you are using to load the data. Will
#    I or the TA have access to those same directories? Of course you cannot 'copy'
#    any data from bbb into bbb_rec. You can copy the data description however
# 4. The final step will be to check that your code produces a data frame
#    identical to the pandas data frame in the bbb.pkl file, using pandas' "equals"
#    method shown below. If the test passes, write bbb_rec to "data/bbb_rec.pkl". Do
#    NOT change the test as this will be used in grading/evaluation
# 5. Make sure to style your python code appropriately for easy readable
# 6. When you are done, save your, code and commit and push your work to GitLab.
#    Of course you can commit and push as often as you like, but only before the
#    due date. Late assignments will not be accepted
# 7. When testing your (final) code make sure to restart the kernel regularly.
#    Restarting the kernel ensures that all modules and variables your code needs
#    are actually generated and loaded in your code
# 8. You can use modules other than the ones mentioned below but do NOT use
#    modules that are not part of the rsm-msba-spark docker container by default

import pandas as pd
import sqlite3
from datetime import date, timedelta
import pyrsm as rsm
import urllib.request
from tempfile import NamedTemporaryFile as tmpfile
import os

# load the original bbb.pkl data frame from a Dropbox link
bbb_file = tmpfile().name
urllib.request.urlretrieve(
    "https://www.dropbox.com/s/6bulog0ij4pr52o/bbb.pkl?dl=1", bbb_file
)
bbb = pd.read_pickle(bbb_file)


# view the data description of the original data to determine
# what needs to be re-created
rsm.describe(bbb)
bbb.head()

# set the working directory to the location of this script
os.getcwd()
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# load demographics data from bbb_demographics.tsv
demographics = pd.read_csv("data/bbb_demographics.tsv", sep='\t')
demographics.head()
demographics['zip'] = demographics['zip'].astype(str)
demographics['zip'] = demographics['zip'].apply(lambda x: x.zfill(5))

# load nonbook aggregate spending from bbb_nonbook.xls
nonbook = pd.read_excel("data/bbb_nonbook.xls")
nonbook.head()

# load purchase and buy-no-buy information from bbb.sqlite
conn = sqlite3.connect("data/bbb.sqlite")
c = conn.cursor()
c.execute("SELECT name FROM sqlite_master WHERE type='table';")
c.fetchall()
#conn.commit()

buyer = pd.read_sql('SELECT * FROM buyer', conn)
purchase = pd.read_sql('SELECT * FROM purchase', conn)

conn.close()

# hint: what data type is "date" in the database?
# hint: most systems record dates internally as the number
# of days since some origin. You can use the pd.to_datetime
# method to convert the number to a date with argument: origin = "1-1-1970"

start = date(1970, 1, 1) 
purchase["date"] = [(start + timedelta(purchase.loc[i, "date"])) for i in range(len(purchase))]


def db_list_tables(con):
    """Return all table names"""
    cursor = con.cursor()
    cursor.execute("select name from sqlite_master where type='table';")
    return [x[0] for x in cursor.fetchall()]


def db_list_fields(con, tabel):
    """Return all column names for a specified table"""
    cursor = con.cursor()
    cursor.execute(f"select * from {tabel} limit 1;")
    return [name[0] for name in cursor.description]

#db_list_tables(conn)
#db_list_fields(conn, purchase)
bbb_rec = demographics

# add the zip3 variable
bbb_rec["zip3"] =  [str(bbb_rec.loc[i, "zip"])[:3] for i in range(len(bbb_rec))]

bbb_rec["acctnum"] = bbb_rec["acctnum"].astype("int")


# use the following reference date (i.e., "today" for the analysis)
start_date = date(2010, 3, 8)

def diff_months(date1, date2):
    """
    This function calculates the difference in months between
    date1 and date2 when a customer purchased a product
    """
    y = date1.year - date2.year
    m = date1.month - date2.month
    return y * 12 + m

purchase_group = purchase.sort_values(["date"],ascending=True).groupby(["acctnum"])
first = purchase_group["date"].min().reset_index().rename(columns={'date':'first'})
last = purchase_group["date"].max().reset_index().rename(columns={'date':'last'})

first["first"] = [diff_months(start_date, first.loc[i, "first"]) for i in range(len(first))]
first["last"] = [diff_months(start_date, last.loc[i, "last"]) for i in range(len(last))]

first['acctnum'] = first['acctnum'].astype('int') 
bbb_rec = bbb_rec.merge(first, how = 'left', on='acctnum')

col_name = {'price':'book', 'purchase_art':'art', 'purchase_child':'child', 'purchase_cook':'cook', 'purchase_do_it':'do_it', 'purchase_geog':'geog', 'purchase_reference':'reference', 'purchase_youth':'youth'}

purchase_dummies = pd.get_dummies(purchase, columns = ["purchase"]).groupby(["acctnum"]).sum().reset_index().rename(columns=col_name)

#cols = purchase_dummies.columns.tolist()

purchase_dummies['acctnum'] = purchase_dummies['acctnum'].astype('int')
bbb_rec = bbb_rec.merge(purchase_dummies , how = 'left', on='acctnum')
bbb_rec['purch'] = bbb_rec["child"]+ bbb_rec["youth"] + bbb_rec["cook"] + bbb_rec["do_it"] + bbb_rec["reference"] + bbb_rec["art"] + bbb_rec["geog"]

bbb_rec = bbb_rec.merge(nonbook, how = 'left', on='acctnum')
bbb_rec['total'] = bbb_rec["book"]+ bbb_rec["nonbook"]

buyer['acctnum'] = buyer['acctnum'].astype('int') 
bbb_rec = bbb_rec.merge(buyer, how = 'left', on='acctnum')
bbb_rec[['book', 'total']] = bbb_rec[['book', 'total']].astype('int')

cols = bbb_rec.columns.tolist()
cols = cols[:8] + cols[-4:-2] + [cols[-5], cols[9], cols[14]] + cols[10:12] + [cols[13], cols[8], cols[12]] + cols[-2:]
bbb_rec = bbb_rec[cols]
    
# generate the required code below for `first`, `last`, `book`, and `purch`,
# and add the purchase frequencies for the different book types
# hint: you can use pandas "value_counts" method here
# hint: check the help for pandas' `first` and `last` methods

# you may find the discussion below of interest at this point
# https://stackoverflow.com/questions/65067042/pandas-frequency-of-a-specific-value-per-group


# combine the different data frames using pandas' "merge" method
bbb.info()
bbb_rec.info()

bbb_rec["acctnum"] = bbb_rec["acctnum"].astype("str")
bbb_rec["gender"] = bbb_rec["gender"].astype("category")
bbb_rec["state"] = bbb_rec["state"].astype("category")  
bbb_rec["first"] = bbb_rec["first"].astype("int32") 
bbb_rec["last"] = bbb_rec["last"].astype("int32") 
bbb_rec["book"] = bbb_rec["book"].astype("int32") 
bbb_rec["nonbook"] = bbb_rec["nonbook"].astype("int32") 
bbb_rec["total"] = bbb_rec["total"].astype("int32") 
bbb_rec["purch"] = bbb_rec["purch"].astype("int32") 
bbb_rec["child"] = bbb_rec["child"].astype("int32") 
bbb_rec["youth"] = bbb_rec["youth"].astype("int32") 
bbb_rec["cook"] = bbb_rec["cook"].astype("int32") 
bbb_rec["do_it"] = bbb_rec["do_it"].astype("int32") 
bbb_rec["reference"] = bbb_rec["reference"].astype("int32") 
bbb_rec["art"] = bbb_rec["art"].astype("int32") 
bbb_rec["geog"] = bbb_rec["geog"].astype("int32") 
bbb_rec["buyer"] = bbb_rec["buyer"].astype("category")  
bbb_rec["training"] = bbb_rec["training"].astype("int32") 

# check if the columns in bbb and bbb_rec are in the same order
# and are of the same type - fix as needed
pd.DataFrame(
    {
        "bbb_rec": bbb_rec.dtypes.astype(str),
        "bbb": bbb.dtypes.astype(str),
        "check": bbb_rec.dtypes == bbb.dtypes,
    }
)

#(bbb_rec["training"] == bbb["training"]).sum()

# add the description as metadata to bbb_rec (see data/bbb_description.txt)
# see https://stackoverflow.com/a/40514650/1974918 for more information

df = pd.read_csv("data/bbb_description.txt")


bbb_rec.description = bbb.description
bbb_rec._metadata.append('description')

# check that you get the same output for both bbb and bbb_rec
rsm.describe(bbb_rec)
rsm.describe(bbb)

#############################################
# DO NOT EDIT CODE BELOW THIS LINE
# YOUR CODE MUST PASS THE TEST BELOW
#############################################
test1 = bbb_rec.equals(bbb)
if hasattr(bbb_rec, "description"):
    test2 = bbb_rec.description == bbb.description
else:
    test2 = False

if test1 is True and test2 is True:
    print("Well done! Both tests passed!")
    print("bbb_rec will now be written to the data directory")
    bbb_rec.to_pickle("data/bbb_rec.pkl")
else:
    test = False
    if test1 is False:
        raise Exception(
            """Test of equality of data frames failed.
            Use bbb.dtypes and bbb_rec.dtypes to check
            for differences in types. Check the number
            of mistakes per colmun using, for example,
            (bbb_rec["book"] == bbb["book"]).sum()"""
        )
    if test2 is False:
        raise Exception(
            """Add a description to the bbb_rec data frame.
            Read the description from the txt file in the
            data directory. See
            https://stackoverflow.com/a/40514650/1974918
            for more information"""
        )
