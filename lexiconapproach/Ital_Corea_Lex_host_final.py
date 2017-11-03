#I also need the multiprocessing library to use all cores
from multiprocessing import Pool
from joblib import Parallel, delayed
import multiprocessing
#And pandas for data wrangling and re for regex
import pandas as pd
import re

#loading in the CorEa data
data = pd.read_csv('CorEA-message-level_clean2.csv', sep='\t',quotechar="'",
                  engine='python')
#just needed for test-runs against a portion of the dataset
data=data.head(10)

#loading in the lexicon that rates each word
lex = pd.read_csv('ITALconvertcsv.csv', delimiter=",", quotechar='"', header=0)
lex.head(5)

#reducing the lexicon into two columns, one for the word and one for the sentimentrating
new_lex=pd.concat([lex['Lemma/_writtenForm'], lex['Sense/Sentiment/_polarity'], ],axis=1)

#now I start creating a binary tree out of the lexicon by splitting it into half
halfindex=int((len(new_lex.index))/2)
halfword=(new_lex['Lemma/_writtenForm'][halfindex])
halfplusword=(new_lex['Lemma/_writtenForm'][halfindex+1])
#but I adjust it so that the last word of the upper half and the first word of the lower half
#have different starting letters
while (halfword[0]==halfplusword[0]):
    halfindex+=1
    halfplusindex=halfindex+1
    halfword=(new_lex['Lemma/_writtenForm'][halfindex])
    halfplusword=(new_lex['Lemma/_writtenForm'][halfplusindex])

#then we define the two halves of an index
uphalfLex=new_lex.ix[:halfindex]
lowhalfLex=new_lex.ix[halfplusindex:]
uphalfLex.reset_index(drop=True, inplace=True)
lowhalfLex.reset_index(drop=True, inplace=True)

#now we do the exact same process to create four quarters of a lexicon

upquarterindex=int((len(uphalfLex.index))/2)
upquarterword=(uphalfLex['Lemma/_writtenForm'][upquarterindex])
upquarterplusword=(uphalfLex['Lemma/_writtenForm'][upquarterindex+1])
while (upquarterword[0]==upquarterplusword[0]):
    upquarterindex+=1
    upquarterplusindex=upquarterindex+1
    upquarterword=(uphalfLex['Lemma/_writtenForm'][upquarterindex])
    upquarterplusword=(uphalfLex['Lemma/_writtenForm'][upquarterplusindex])


upupquarterLex=uphalfLex.ix[:upquarterindex]
uplowquarterLex=uphalfLex.ix[upquarterplusindex:]

upupquarterLex.reset_index(drop=True, inplace=True)
uplowquarterLex.reset_index(drop=True, inplace=True)


lowquarterindex=int((len(lowhalfLex.index))/2)

lowquarterword=(lowhalfLex['Lemma/_writtenForm'][lowquarterindex])
lowquarterplusword=(lowhalfLex['Lemma/_writtenForm'][lowquarterindex+1])
while (lowquarterword[0]==lowquarterplusword[0]):
    lowquarterindex+=1
    lowquarterplusindex=lowquarterindex+1
    lowquarterword=(lowhalfLex['Lemma/_writtenForm'][lowquarterindex])
    lowquarterplusword=(lowhalfLex['Lemma/_writtenForm'][lowquarterplusindex])


lowupquarterLex=lowhalfLex.ix[:lowquarterindex]
lowlowquarterLex=lowhalfLex.ix[lowquarterplusindex:]

lowupquarterLex.reset_index(drop=True, inplace=True)
lowlowquarterLex.reset_index(drop=True, inplace=True)



cellvavlist=[]
leftrightlist=[]
midlist=[]



#the lexicon classifier is defined as a function for multiprocessing
def lexiconclassifier(line):
    cell=(line['6text'])
    mid=(line['#0Mid'])
    new_lex=pd.DataFrame()
    cellrating=0
    wordcount=0
    lr_classifier=0
    LeftOrRight="None"

    #each word from the cell is now checked
    for word in (cell.split()):
        word=re.sub("[^A-Za-z|-]","", word)
        #print(word)

        ratingfactor=0
        #now for each word we take the first letter
        #and check which quarter of the lexicon might have the word in it
        try:
            if word[0]<=halfplusword[0]:
                if word[0]<=upquarterplusword[0]:
                    new_lex=upupquarterLex
                else:
                    new_lex=uplowquarterLex
            else:
                if word[0]<=lowquarterplusword[0]:
                    new_lex=lowupquarterLex
                else:
                    new_lex=lowlowquarterLex
        except LookupError:
            pass
        #then this loop iterates through the lexicon
        for index, row in (new_lex).iterrows():
            #and if the word from the cell is found in the lexicon

            if word.startswith(row["Lemma/_writtenForm"]):
                wordcount+=1
                #it adds the ratingfactor depending on what is in the
                #Sense/Sentiment/_polarity"-column
                if row["Sense/Sentiment/_polarity"]=="neutral":
                    ratingfactor=0
                if row["Sense/Sentiment/_polarity"]=="positive":
                    ratingfactor=1
                if row["Sense/Sentiment/_polarity"]=="negative":
                    ratingfactor=-1
                #and then adds the rating of the word to the rating of the cell
                cellrating=cellrating+(ratingfactor)
    if wordcount==0:
        cellaverage=0
    #finally it calculates the average rating of the cell, unless there were
    #no words matchin in the cell
    else:
        cellaverage=(cellrating/wordcount)
    print(cellaverage)
    print(mid)
    element={}
    element['mid']=mid
    element['cellaverage']=cellaverage
    #and returns the calculated elements as dictionary elements
    return(element)

#this counts the available cores
num_cores = multiprocessing.cpu_count()

print(num_cores)
#this defines that multiprocessing should be used to rate each cell in parallel
result=Parallel(n_jobs=num_cores)(delayed(lexiconclassifier)(line) for index,line in (data[['#0Mid','6text']]).iterrows())


print(result)
#this converts the results dictionary into a dataframe
test=pd.DataFrame.from_dict(result, orient='columns', dtype=None)

test.head(10)


#and stores it as a csv

test.to_csv("ItaloLexRate.csv")
