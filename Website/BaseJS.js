//loads from a csv how accurate the algorithm works
function d3CSVload() {

  d3.csv("botdetectSTATS.csv", function(error1, databot){
    if (error1) throw error;
    console.log(databot)
    accuracy =+ databot["0"].ACC

    console.log(accuracy)

    d3.csv(" BOTStats.csv", function(error, data) {
            if (error) throw error;
            console.log(data)
            //loads from the other csv the number of tweets and the last time
            overalltweets= +data["0"].overalltweets
            lastruntimemin= data["0"].localtime.slice(-6, -4)
            lastruntimehour= data["0"].localtime.slice(-9, -7)
            lastruntime=(lastruntimehour+":"+lastruntimemin)
            console.log(lastruntime)

            console.log(overalltweets)
            //if the accuracy is below 0.8, it uses the bot percentage for the count-based method
            //so number of tweets from accounts that tweeted more than 24 times a day
            // about the tracked hashtags
            if(accuracy<0.8){
            console.log("count")
            botquota = +data["0"].botquotaraw;
            botquota=botquota*100;
            }
            //otherwise the numbers derieved from the machine learning algorithm are used
            else{
            console.log("ML")

            botquota = +data["0"].botquotaML;
            botquota=botquota*100;
            }
            // the retrieved numbers are now inserted into pre-defined div elements

            document.getElementById("TweetsoverallGer").innerHTML = overalltweets.toFixed(0)+" "
            document.getElementById("TweetsoverallEng").innerHTML = overalltweets.toFixed(0)+" "

            botquotaGer=botquota.toFixed(1)+" "
            botquotaGer=botquotaGer.replace('.',",")
            document.getElementById("botquotaGer").innerHTML = botquotaGer
            document.getElementById("botquotaEng").innerHTML = botquota.toFixed(1)+" "

            document.getElementById("runtimeGer").innerHTML = lastruntime
            document.getElementById("runtimeEng").innerHTML = lastruntime


      });
    });
    }
    d3CSVload()
