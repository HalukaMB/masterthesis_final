# Conclusion of my MSc-thesis

# Simple Twitter election predictions based on mentions do not work
The thesis shows that the concept of election predictions based on Twitter mentions does not work currently. Tweet-based predictions would have wrongly projected the AfD to be the winner of the election, when in reality it was just the party in third place. Furthermore, the offset between predicted vote share (based on mention of parties in tweets) and actual result was in double figures with a mean error of 10.7 per cent points.

# Bot filtering can improve predictions but only on a small scale
Using methods that are trying to filter out bots, the offset could be reduced from 10.7 down to 10.3. This is an improvement but on a very small scale.
However, the results show that bot filtering has become necessary as bots can create the impression that a certain party is supported by more people than it actually is. Using more sophisticated methods than the ones used in this master’s thesis might help to estimate the support for a party more correctly.

# The far-right seems to have been heavily supported by bots
In the case of the German election, it seems that content related to the right-wing party AfD was especially pushed by highly automated accounts. This is shown by the different bot statistics retrieved from my application and also by the fact that the prediction model improves its accuracy significantly once it excludes the AfD.

# Sentiment analysis of text is much more complicated than expected
The poor results of sentiment analysis based on a lexicon-approach and on a stylometric/machine learning approach show how complicated it is to detect sentiment in text. Several iterations and changes could not improve any of the approaches so that the algorithm would detect correctly the sentiment of a text at least in 60 per cent of the cases.

# Agile concepts and modular organization of code are helpful
As much as the outcome of the sentiment analysis was unsuccessful, the minimum viable product concept of the application meant that the overall project was never threatened - merely the aim for the application had to be modified slightly. Also, the set up of sprints and partial deadlines stopped me from spending too much time on sentiment analysis, although setting up all the modules on servers was not done until I was halfway into my internship at the Financial Times, which means the overall delivery was delayed by four weeks.
Furthermore, the idea to code in separate modules allowed me to quickly adjust the project depending on the problems encountered and it also made the handling of bugs much easier.

# Amazon instances and S3 buckets are a lean and efficient hosting solution
Using Amazon’s web services proved to be a helpful decision. Hosting the different Python scripts on the server instances was a solution that I could handle easily and the scripts ran mostly stable on the instances. Also, it allowed me to scale up from a smaller sized instance to a bigger one, once it became clear that more capacity was needed for data handling. Combining the AWS instances with the idea of hosting the website from a S3 bucket was also fitting for the purpose. It allowed me to host the website in a very simple manner and ensured that the graphics on the website would update every 15 minutes when new CSVs were pushed into the bucket.

