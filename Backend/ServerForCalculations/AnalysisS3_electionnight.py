#As not a mere text is saved but actually a machine learning algorithm
#for some reason boto3 alone could not do the job, this is why both boto libraries are loaded
#to connect to s3
import s3io
import joblib
import boto3
import _credentials
from io import StringIO
import boto
import csv, glob, re, io, json, time

import pandas as pd

from boto.s3.key import Key

# setting up the credentials for accessing the AWS s3 buckets
LOCAL_PATH = _credentials.LOCAL_PATH
AWS_ACCESS_KEY_ID = _credentials.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = _credentials.AWS_SECRET_ACCESS_KEY

bucket_name = 'jsontocsv2'

#the code for accessing s3 stems from https://stackoverflow.com/questions/15085864/how-to-upload-a-file-to-directory-in-s3-bucket-using-boto
#and https://stackoverflow.com/questions/30818341/how-to-read-a-csv-file-from-an-s3-bucket-using-pandas-in-python
#connecting to s3 and loading the csv with the tweets from the last 24 hours
conn = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
bucket = s3.Bucket('jsontocsv2')
s3 = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
from boto.s3.key import Key
k = Key('jsontocsv2')
k.key = 'Last24.csv'
data = pd.read_csv('https://s3.amazonaws.com/jsontocsv2/Last24.csv', sep=',' , skipinitialspace = True, quotechar = "'")


#deleting rows where the user_id is a NaN
data=data.dropna(subset = ['user_id'])
#sorting rows for user_id
data=data.sort_values(by='user_id')


IDbot=0
counter=0
botlist=[]

k = Key('sampletweets2')
k.key = 'lastbotlist.csv'
yesterdaybots = pd.read_csv('https://s3.amazonaws.com/sampletweets2/lastbotlist.csv', sep=',' , skipinitialspace = True, quotechar = "'")




#loading the list of yesterday's bots (accounts flagged as bot-like from 24to48hours) to start as a base
#for the botlist
yesterdaybotslist=yesterdaybots["bot_ids"].values.tolist()
for i in yesterdaybotslist:
      if i not in botlist:
            botlist.append(i)

#We are creating two lists. One with ids of likely bot accounts and one with ids of "normal-accounts"

#this loop iterates through the csv and records which account-ids have tweeted more than 24 times
#and appends them to a list. Our bot-list
for cell in data["user_id"]:
    #if the counter is over 24, the id of the account is added to the botlist,
    #unless the id is already in there
    if counter>24:
        if IDbot not in botlist and IDbot != "user_id":
            botlist.append(IDbot)
        else:
            pass
    #if the current cell is not the same as the previously stored IDnor
    #and the counter is at 1, the ID is appended to the list of normal accounts
    if cell != IDbot:

            #the stored ID is changed to the cell-ID and counter resetted to 1
            IDbot=cell
            counter=1
    #otherwise the ID stays the same but the counter is increased by 1
    else:
        counter+=1

#reduces the csv to only text, the id of the account and the id of the tweet
reduced_data=pd.concat([data["text"], data["user_id"], data["tweet_id"]], axis=1)

botsampleid = []
normalsampleid = []
botsampletext=[]
normalsampletext=[]
bottweetid = []
normaltweetid = []

progcounter=0
#goes through the entire dataset and splits it up into two
for index, row in reduced_data.iterrows():
    progcounter+=1
    #if the id was in the botlist, the row goes to the bot-dataframe
    if(row['user_id'])in botlist:
        botsampleid.append(row['user_id'])
        botsampletext.append(row['text'])
        bottweetid.append(row['tweet_id'])

    #if the id was not in the botlist, the row goes to the normal-dataframe

    if(row['user_id'])not in botlist:
        normalsampleid.append(row['user_id'])
        normalsampletext.append(row['text'])
        normaltweetid.append(row['tweet_id'])


    #to see the progress, we use a counter
    if (progcounter%2000==0):
        print(progcounter)



#we build a "bot" dataframe from these three lists and give it a fourth column
#with a default value of 1 for being a bot
botdf=pd.DataFrame()
botdf["user_id"]=botsampleid
botdf["text"]=botsampletext
botdf["tweet_id"]=  bottweetid
botdf["botornot"]=1

#we build a "normal" dataframe from these three lists and give it a fourth column
#with a default value of 1 for being a bot
normaldf=pd.DataFrame()
normaldf["user_id"]=normalsampleid
normaldf["text"]=normalsampletext
normaldf["tweet_id"]= normaltweetid
normaldf["botornot"]=0

#getting rid of the index column
normaldf.reset_index(drop=True, inplace=True)
botdf.reset_index(drop=True, inplace=True)

#removing all rows where the text is just 'text'
normaldf = normaldf[normaldf.text != 'text']
botdf = botdf[botdf.text != 'text']

#saving today's tweets that were classified as normal-tweets in a S3bucket
csv_buffer = StringIO()
normaldf.to_csv(csv_buffer, quotechar='"')
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("sampletweets2", 'normaltweets_today.csv').put(Body=csv_buffer.getvalue())

#saving today's tweets that were classified as bot-tweets in a S3bucket
csv_buffer = StringIO()
botdf.to_csv(csv_buffer, quotechar='"')
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("sampletweets2", 'bottweets_today.csv').put(Body=csv_buffer.getvalue())


#creating a dataset consisting of all tweets
test=pd.concat([normaldf,botdf], ignore_index=True)
print("AllTweets dumped")

#And dumping them into an S3 bucket
csv_buffer = StringIO()
test.to_csv(csv_buffer, quotechar='"')
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("sampletweets2", 'alltweets_today.csv').put(Body=csv_buffer.getvalue())

#list of hashtags or accounts associated with each party
CDU=["cdu","csu","merkel", "#cdu","#csu","#merkel", "@cdu"]
SPD=["@martinschulz","spd","schulz", "#spd","#schulz",'@spdde']
FDP = ['#fdp', '@c_lindner', '@fdp' ] #track list
GRUENE = ['#gruene','@die_gruenen','@cem_oezdemir', '@goeringeckardt'] #track list
AFD = ['#afd','@afd_bund','#gauland',"@alice_weidel"]
LINKE= ['#linke', '@dietmarbartsch', '@swagenknecht', '@dielinke'] #track list

#now we load in the data to the script that creates a stylometric fingerprint
#for each tweet
data = test
print(len(data))

#the principle is as follows, first we create a list for each aspect that the script catches
# and then we puzzle together an array/dataframe from these lists
dotcountlist=[]
commacountlist=[]
exclamationcountlist=[]
wordcountlist=[]
uppercasecountlist=[]
lowercasecountlist=[]
charcountlist=[]
specialcharlist=[]
numbercountlist=[]
uniquewordlist=[]
qmarkcountlist=[]
apicescountlist=[]
quotescountlist=[]
openparlist=[]
closeparlist=[]
operatorlist=[]
hashtagcountlist=[]
dottycountlist=[]
linkcountlist=[]

#and separately a list for how often a party's hashtags or accounts are mentioned
CDUlist=[]
SPDlist=[]
GRUENElist=[]
FDPlist=[]
AFDlist=[]
LINKElist=[]
a=0
#for each cell in the text column of data
for cell in (data['text']):

    a+=1
    dotcount=0
    commacount=0
    qmarkcount=0
    exclamationcount=0
    wordcount=0
    uppercasecount=0
    lowercasecount=0
    charcount=0
    lr_classifier=0
    specialchar=0
    numbercount=0
    cellwordlist=[]
    uniqueword=0
    qmarkcount=0
    apicescount=0
    quotescount=0
    openparcount=0
    closeparcount=0
    operatorcount=0
    hashtagcount=0
    dottycount=0
    linkcount=0

    CDUcount=0
    SPDcount=0
    FDPcount=0
    AFDcount=0
    LINKEcount=0
    GRUENEcount=0

    #we now count how often certain characters appear in each text cell
    dotcount=cell.count(".")
    commacount=cell.count(",")
    exclamationcount=cell.count("!")
    qmarkcount=cell.count("?")
    apicescount=cell.count("'")
    quotescount=cell.count('"')
    openparcount=cell.count('(')
    closeparcount=cell.count(')')
    hashtagcount=cell.count('#')

    #now we check for each word in the cell
    for word in (cell.split()):
        word=str(word)

        #how often are accounts or hashtags mentioned from the party's lists
        if word.lower() in CDU:
            CDUcount+=1
        if word.lower() in SPD:
            SPDcount+=1
        if word.lower() in GRUENE:
            GRUENEcount+=1
        if word.lower() in FDP:
            FDPcount+=1
        if word.lower() in AFD:
            AFDcount+=1
        if word.lower() in LINKE:
            LINKEcount+=1


        #how many unique words we find
        if word in cellwordlist:
            continue
        else:
            cellwordlist.append(word)
            uniqueword+=1
        #how many "..." do we find
        if re.search('\.\.\.',word):
            dottycount+=1
        #how many links can we find
        if re.search('http',word):
            linkcount+=1

        #how many words are in each cell
        wordcount+=1

        #checks how many letters, that are not at the start of a word,
        #are written in uppercase like "WHAT ARE THEY DOING???"
        uppercaseword=word[1:]
        for c in uppercaseword:
            if c.istitle():
                uppercasecount+=1
                #print(word)

        #checks how many characters and numbers are in each word
        for char in word:
            charcount+=1
            #checks how many characters are used
            if re.match("[^a-zA-Z0-9]", char):
                specialchar+=1
            #checks how many numbers are used
            if re.match("[0-9]", char):
                numbercount+=1
            #checks how many characters are lowercase
            if char.islower():
                lowercasecount+=1
            #checks how many characters are operators
            if re.match("[\+\/\-\*\%\=<>&]", char):
                operatorcount+=1

    #appending the results of the various counters to the list
    dotcountlist.append(dotcount)
    commacountlist.append(commacount)
    exclamationcountlist.append(exclamationcount)
    wordcountlist.append(wordcount)
    uppercasecountlist.append(uppercasecount)
    lowercasecountlist.append(lowercasecount)
    charcountlist.append(charcount)
    specialcharlist.append(specialchar)
    numbercountlist.append(numbercount)
    uniquewordlist.append(uniqueword)
    qmarkcountlist.append(qmarkcount)
    apicescountlist.append(apicescount)
    quotescountlist.append(quotescount)
    openparlist.append(openparcount)
    closeparlist.append(closeparcount)
    operatorlist.append(operatorcount)
    hashtagcountlist.append(hashtagcount)
    dottycountlist.append(dottycount)
    linkcountlist.append(linkcount)

    CDUlist.append(CDUcount)
    SPDlist.append(SPDcount)
    FDPlist.append(FDPcount)
    GRUENElist.append(GRUENEcount)
    AFDlist.append(AFDcount)
    LINKElist.append(LINKEcount)

#checking length of dataframe
print("length of list", len(LINKElist))
#now building pandas series from these lists
row1=pd.DataFrame()
row1["Dotcount"]=dotcountlist
row2=pd.DataFrame()
row2["Commacount"]=commacountlist
row3=pd.DataFrame()
row3["Exclcount"]=exclamationcountlist
row4=pd.DataFrame()
row4["Wordcount"]=wordcountlist
row5=pd.DataFrame()
row5["Uppercasecount"]=uppercasecountlist
row6=pd.DataFrame()
row6["Lowercasecount"]=lowercasecountlist
row7=pd.DataFrame()
row7["Specialchar"]=specialcharlist
row8=pd.DataFrame()
row8["Numbercount"]=numbercountlist
row9=pd.DataFrame()
row9["Charcount"]=charcountlist
row10=pd.DataFrame()
row10["Uniquewords"]=uniquewordlist
row11=pd.DataFrame()
row11["Qmarkcount"]=qmarkcountlist
row11=pd.DataFrame()
row11["Apicescount"]=apicescountlist
row12=pd.DataFrame()
row12["Quotescount"]=quotescountlist
row13=pd.DataFrame()
row13["Openpar"]=openparlist
row14=pd.DataFrame()
row14["Closepar"]=closeparlist
row15=pd.DataFrame()
row15["Operatorcount"]=operatorlist
row16=pd.DataFrame()
row16["Hashtagcount"]=hashtagcountlist
row17=pd.DataFrame()
row17["Dottycount"]=dottycountlist
row18=pd.DataFrame()
row18["Linkcount"]=linkcountlist

#buidling dataframes from these serieses
output=pd.concat([data, row1, row2, row3, row4, row5, row6, row7, row8, row9, row10, row11, row12, row13, row14, row15, row16, row17, row18],axis=1)

#storing this stylometric fingerprint csv to the S3 bucket
csv_buffer = StringIO()
output.to_csv(csv_buffer, quotechar='"', index=False)
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("sampletweets2", 'todayABSraw.csv').put(Body=csv_buffer.getvalue())

#only taking the user_id, the tweet_id and the botornot column without further processing
new_datareduced=output.ix[:,[0,2,3]]
#taking the dataframe from the third column onwards as the datareduced dataframe
datareduced=output.ix[:,3:]
print(datareduced.head(10))
datareduced.describe()
c=0
limit=0
#counting the number of columns of this
for column in datareduced:
    limit+=1
print(limit)
a=-1
#now we iterate through each column and calculate the average for each column
#divided by the mean number of characters
for column in datareduced:
    lista=[]
    a+=1
    columnmean=((datareduced.ix[:, a]).mean())/datareduced['Charcount'].mean()
    print(column,":" ,columnmean)
    b=-1
    #now we weight each absolute value we calculated earlier against the mean
    #so for instance, if 2 dots were counted, but the mean number of dots was 1.5
    # the relative weighted value is then 2/1.5=2.33
    for index, row in (datareduced).iterrows():
        b+=1
        columnval=(datareduced.ix[b, a])
        columnratio=columnval/datareduced.ix[b,'Charcount']
        columnREL=(columnratio/columnmean)
        lista.append(columnREL)
    c+=1
    print(c)
    if c>limit:
        print("Limit!")
        break
    else:
        column = pd.DataFrame({(column+"REL") : lista})
        #putting together the new weighted columns with the not processed values
        new_datareduced=pd.concat([new_datareduced, column], axis=1)
print("STOP!")

#dropping two columns that created problems when using for the machine learning model
new_datareduced=new_datareduced.drop("ApicescountREL", axis=1)
new_datareduced=new_datareduced.drop("botornotREL", axis=1)
#now storing again the dataframe with the now weighted relative counts
csv_buffer = StringIO()
new_datareduced.to_csv(csv_buffer, quotechar='"', index=False)
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("sampletweets2", 'todayRELraw.csv').put(Body=csv_buffer.getvalue())


#creating serieses for each partylist
rown1=pd.DataFrame()
rown1["CDU"]=CDUlist
rown2=pd.DataFrame()
rown2["SPD"]=SPDlist
rown3=pd.DataFrame()
rown3["FDP"]=FDPlist
rown4=pd.DataFrame()
rown4["GRUENE"]=GRUENElist
rown5=pd.DataFrame()
rown5["AFD"]=AFDlist
rown6=pd.DataFrame()
rown6["LINKE"]=LINKElist

#creating a dataframe out of these serieses
parties=pd.concat([rown1, rown2,rown3,rown4,rown5,rown6], axis=1)
print(len(new_datareduced))
print(len(parties))


#concating the stylometric fingerprint dataframe together with
#the partylist dataframe
test=pd.concat([new_datareduced, parties], axis=1)
#shuffling the dataframe
test = test.sample(frac=1).reset_index(drop=True)
test.head()

#now we load into the script for the machine learning based classification

import numpy as np
test.replace('', np.nan, inplace=True)
test.dropna(inplace=True)

testdata = test
#we save these columns to lists to save them for now
#the botornot list because it is the dependent variable that should be predicted
#all other lists because they were not included in the model that we built
#from the tweets from yesterday
botornotcount=testdata['botornot'].tolist()
CDUlist=testdata['CDU'].tolist()
SPDlist=testdata['SPD'].tolist()
FDPlist=testdata['FDP'].tolist()
GRUENElist=testdata['GRUENE'].tolist()
LINKElist=testdata['LINKE'].tolist()
AFDlist=testdata['AFD'].tolist()

#now we delete all these columns from the dataframe to avoid confusion for the
#machine learning model
del testdata['botornot']
del testdata['user_id']
del testdata['SPD']
del testdata['CDU']
del testdata['FDP']
del testdata['GRUENE']
del testdata['LINKE']
del testdata['AFD']

#now we define the remaining dataframe as the array of the independent variables
X_test=testdata
from sklearn.cross_validation import StratifiedKFold
from sklearn import metrics
import pickle

#now loading the previously stored ML model
bucket = "sampletweets2"
key = "trained_model.sav"
credentials = dict(aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
import numpy as np
with s3io.open('s3://{0}/{1}'.format(bucket, key), mode='r',**credentials) as s3_file:
    clr =  joblib.load(s3_file)
#X_test.replace('', np.nan, inplace=True)
##X_test.to_csv("X_test", index=False)
#print(X_test.head())

#now predicting which tweets are bots or not based on the stored model
pred_test = clr.predict(X_test)
#storing the predictions to a list
pred_test=pred_test.tolist()

#creating a series from the predicted bot or not count-list
rownew=pd.DataFrame()
rownew["MLbotornot"]=pred_test
#creating a series from the count-based bot or not count-list
rownew2=pd.DataFrame()
rownew2["Countbotornot"]=botornotcount

#buidling a dataframe from these serieses and the stored testdata-dataframe
output=pd.concat([testdata, rownew, rownew2], axis=1)

#creating serieses from the parties-lists
rownew=pd.DataFrame()
rownew["CDU"]=CDUlist
rownew2=pd.DataFrame()
rownew2["SPD"]=SPDlist
rownew3=pd.DataFrame()
rownew3["FDP"]=FDPlist
rownew4=pd.DataFrame()
rownew4["GRUENE"]=GRUENElist
rownew5=pd.DataFrame()
rownew5["LINKE"]=LINKElist
rownew6=pd.DataFrame()
rownew6["AFD"]=AFDlist

#adding these serieses to this master-dataframe

output=pd.concat([output, rownew, rownew2,rownew3,rownew4,rownew5,rownew6], axis=1)


#now we do the last analysis, we count how many tweets for each party were counted
#and whether they were bot tweets or not
botnumber=0
MLdetectbot=0
Dualdetectbot=0
CDUnumber=0
SPDnumber=0
FDPnumber=0
GRUENEnumber=0
AFDnumber=0
LINKEnumber=0

CDUbotnumber=0
SPDbotnumber=0
FDPbotnumber=0
GRUENEbotnumber=0
LINKEbotnumber=0
AFDbotnumber=0

CDUMLbotnumber=0
SPDMLbotnumber=0
FDPMLbotnumber=0
GRUENEMLbotnumber=0
AFDMLbotnumber=0
LINKEMLbotnumber=0

CDUDualbotnumber=0
SPDDualbotnumber=0
FDPDualbotnumber=0
GRUENEDualbotnumber=0
AFDDualbotnumber=0
LINKEDualbotnumber=0

#we iterate through the master-dataframe, row by row and count
for index, row in output.iterrows():
    #was this tweet as a bot-tweet because more than 24 tweets were sent out from this account
    if row["Countbotornot"]==1:
        botnumber+=1
    #was this tweet classified as a bot-tweet based on the machine learning classification
    if row["MLbotornot"]==1:
        MLdetectbot+=1
    #was this either classified as bot by counts or by the machine learning algorithm
    if (row["MLbotornot"]==1) or (row["Countbotornot"]==1):
        Dualdetectbot+=1
    #was there a menitoning of the party ...
    if row["CDU"]>0:
        CDUnumber+=1
    if row["SPD"]>0:
        SPDnumber+=1
    if row["FDP"]>0:
        FDPnumber+=1
    if row["GRUENE"]>0:
        GRUENEnumber+=1
    if row["AFD"]>0:
        AFDnumber+=1
    if row["LINKE"]>0:
        LINKEnumber+=1

    #now we count the tweets by party that were classified as bots according to counts
    if row["CDU"]>0 and row["Countbotornot"]==1:
        CDUbotnumber+=1
    if row["SPD"]>0 and row["Countbotornot"]==1:
        SPDbotnumber+=1
    if row["FDP"]>0 and row["Countbotornot"]==1:
        FDPbotnumber+=1
    if row["AFD"]>0 and row["Countbotornot"]==1:
        AFDbotnumber+=1
    if row["LINKE"]>0 and row["Countbotornot"]==1:
        LINKEbotnumber+=1
        #print("LINKE COUNT BOT")
    if row["GRUENE"]>0 and row["Countbotornot"]==1:
        GRUENEbotnumber+=1

    #now we count the tweets by party that were classified as bots according to the machine learning algorithm
    if row["CDU"]>0 and row["MLbotornot"]==1:
        CDUMLbotnumber+=1
    if row["SPD"]>0 and row["MLbotornot"]==1:
        SPDMLbotnumber+=1
    if row["GRUENE"]>0 and row["MLbotornot"]==1:
        GRUENEMLbotnumber+=1
    if row["LINKE"]>0 and row["MLbotornot"]==1:
        LINKEMLbotnumber+=1
    if row["FDP"]>0 and row["MLbotornot"]==1:
        FDPMLbotnumber+=1
    if row["AFD"]>0 and row["MLbotornot"]==1:
        AFDMLbotnumber+=1

    #now we count the tweets by party that were classified as bots either according to the machine learning algorithm
    #or according to the counting method
    if row["CDU"]>0 and (row["Countbotornot"]==1 or row["MLbotornot"]==1):
        CDUDualbotnumber+=1
    if row["SPD"]>0 and (row["Countbotornot"]==1 or row["MLbotornot"]==1):
        SPDDualbotnumber+=1
    if row["FDP"]>0 and (row["Countbotornot"]==1 or row["MLbotornot"]==1):
        FDPDualbotnumber+=1
    if row["AFD"]>0 and (row["Countbotornot"]==1 or row["MLbotornot"]==1):
        AFDDualbotnumber+=1
    if row["LINKE"]>0 and (row["Countbotornot"]==1 or row["MLbotornot"]==1):
        LINKEDualbotnumber+=1
        #print("LINKE DUAL COUNT BOT")
    if row["GRUENE"]>0 and (row["Countbotornot"]==1 or row["MLbotornot"]==1):
        GRUENEDualbotnumber+=1

overall=len(output)
#we calculate the number of normaltweets by subtration
normalnumber=overall-botnumber
normalMLnumber=overall-MLdetectbot
normalDualnumber=overall-Dualdetectbot

#we give a current estimate
print("tweets by bots:", botnumber,"//","normal tweets:", normalnumber)



#we calculate the ratio of bots given the different counting methods
botquota=botnumber/overall
botquota

botquotaML=MLdetectbot/overall
botquotaML

botquotaDual=Dualdetectbot/overall
botquotaDual

#now we calculate the number of normal tweets per party again by subtraction
#and for each counting method

CDUpure=CDUnumber-CDUbotnumber
CDUpureML=CDUnumber-CDUMLbotnumber
CDUpureDual=CDUnumber-CDUDualbotnumber

SPDpure=SPDnumber-SPDbotnumber
SPDpureML=SPDnumber-SPDMLbotnumber
SPDpureDual=SPDnumber-SPDDualbotnumber

FDPpure=FDPnumber-FDPbotnumber
FDPpureML=FDPnumber-FDPMLbotnumber
FDPpureDual=FDPnumber-FDPDualbotnumber

GRUENEpure=GRUENEnumber-GRUENEbotnumber
GRUENEpureML=GRUENEnumber-GRUENEMLbotnumber
GRUENEpureDual=GRUENEnumber-GRUENEDualbotnumber

LINKEpure=LINKEnumber-LINKEbotnumber
LINKEpureML=LINKEnumber-LINKEMLbotnumber
LINKEpureDual=LINKEnumber-LINKEDualbotnumber

AFDpure=AFDnumber-AFDbotnumber
AFDpureML=AFDnumber-AFDMLbotnumber
AFDpureDual=AFDnumber-AFDDualbotnumber

#we retrieve the current time
from datetime import datetime, timedelta
Germantime = datetime.now()+ timedelta(hours=2)
Germantime=Germantime.strftime("%Y-%b-%d--%H-%M-%S")
print(Germantime)

#and store all the overall statistics in one csv
csv_out1 = io.StringIO()

#with a headerrow
data = u"localtime,overalltweets,botquotaraw,botquotaML,botquotaDual,normalnumber,normalMLnumber,botnumber,MLbotnumber";
csv_out1.write(data)
csv_out1.write(u'\n')
row = [Germantime,overall,botquota,botquotaML,botquotaDual,normalnumber,normalMLnumber,botnumber,MLdetectbot];
print(row)
row_joined = (str(row).strip('[').strip(']'))
csv_out1.write(row_joined)
csv_out1.write(u'\n')
#and dump it into a s3 bucket
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("hashtagswahlpost", 'BOTStats.csv').put(Body=csv_out1.getvalue())

#and then we create the csv that records for each party the statistics of bots and normal tweets
csv_out2 = io.StringIO()
data = u"party,tweetsoverall,normaltweetscount,bottweetscount,normaltweetsML,bottweetsML,normaltweetsdual,bottweetsdual,color";
csv_out2.write(data)

#we include the partyname and the partycolour
partyname="CDU"
colorname="#000000"

csv_out2.write(u'\n')
row = [partyname,CDUnumber,CDUpure,CDUbotnumber,CDUpureML,CDUMLbotnumber,CDUpureDual,CDUDualbotnumber,colorname];
row_joined = (str(row).strip('[').strip(']'))
csv_out2.write(row_joined)
csv_out2.write(u'\n')

partyname="SPD"
colorname="#E2001A"

row = [partyname,SPDnumber,SPDpure,SPDbotnumber,SPDpureML,SPDMLbotnumber,SPDpureDual,SPDDualbotnumber,colorname];
row_joined = (str(row).strip('[').strip(']'))
csv_out2.write(row_joined)
csv_out2.write(u'\n')

partyname="FDP"
colorname="#FFD600"

row = [partyname,FDPnumber,FDPpure,FDPbotnumber,FDPpureML,FDPMLbotnumber,FDPpureDual,FDPDualbotnumber,colorname];
row_joined = (str(row).strip('[').strip(']'))
csv_out2.write(row_joined)
csv_out2.write(u'\n')

partyname="GRUENE"
colorname="#64A12D"

row = [partyname,GRUENEnumber,GRUENEpure,GRUENEbotnumber,GRUENEpureML,GRUENEMLbotnumber,GRUENEpureDual,GRUENEDualbotnumber,colorname];
row_joined = (str(row).strip('[').strip(']'))
csv_out2.write(row_joined)
csv_out2.write(u'\n')

partyname="AFD"
colorname="#009DE0"

row = [partyname,AFDnumber,AFDpure,AFDbotnumber,AFDpureML,AFDMLbotnumber,AFDpureDual,AFDDualbotnumber,colorname];
row_joined = (str(row).strip('[').strip(']'))
csv_out2.write(row_joined)
csv_out2.write(u'\n')

partyname="LINKE"
colorname="#81007F"

row = [partyname,LINKEnumber,LINKEpure,LINKEbotnumber,LINKEpureML,LINKEMLbotnumber,LINKEpureDual,LINKEDualbotnumber,colorname];
row_joined = (str(row).strip('[').strip(']'))
csv_out2.write(row_joined)
csv_out2.write(u'\n')

#finally dumping the whole result to an S3 bucket from where the frontend
# can retrieve the numbers for buidling its graphics
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("hashtagswahlpost", 'partystats.csv').put(Body=csv_out2.getvalue())
