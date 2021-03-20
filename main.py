#Imports and setting up the reddit API
from flask import Flask, jsonify, request
import pandas as pd
import os
import mysql.connector
import csv

app = Flask(__name__)

@app.route("/")
def hello_world():

	#Loads in my db credentials
	t = csv.reader(open('./dbcredentials.csv','r'),delimiter=',')
	credentials = []
	for credential in t:
		credentials.append(credential)
	credentials = credentials[0]
	
	WSBDB = mysql.connector.connect(
		host=credentials[0],
		user=credentials[1],
		password=credentials[2],
		database='WSB_Posts'
	)
	
	WSBCursor = WSBDB.cursor()
	
	
	#The argument passed here is a query code representing which query should be sent to the database
	args = pd.DataFrame([request.args.get('queryCode')])[0][0]
    #Handles the TopPosts graph, just showing upvotes over time for the 1000 most upvoted posts
	if (args=='TopPosts'):
		WSBCursor.execute('''SELECT votes, date_posted 
							FROM Posts
							ORDER BY votes DESC
							LIMIT 1000''')
		dbreturns = []
		for x in WSBCursor:
			dbreturns.append(x)

		outputs = []
		for item in dbreturns:	  
			newRow = []
			newRow.append(item[0])
			newRow.append(str(item[1])[0:10])
			outputs.append(newRow)
		#Returns the data
		return(jsonify(outputs),200,{'Access-Control-Allow-Origin': '*'}) #Allow-Origin: '*' is just for testing purposes
		
    #Shows the default stock mention counts over the last 24 hours
	elif (args=='RecentDataTable'):
		import pickle
		#Loads some pickles containing data on the stocks listed on three major exchanges (NYSE, NASDAQ, TSX)
		words = pickle.load(open('stopwords.p','rb'))
		NYSEDict = pickle.load(open('NYSEDict.p','rb'))
		NASDAQDict = pickle.load(open('NASDAQDict.p','rb'))
		TSXDict = pickle.load(open('TSXDict.p','rb'))
		tickerToName = pickle.load(open('tickerToName.p','rb'))
		nameToTicker = pickle.load(open('nameToTicker.p','rb'))
		
		#This creates a set of stock names for comparison
		stockNames = set(tickerToName.values())
		
		#Creates a dictionary to hold the count for each stock ticker
		tickerCount = {}
		for ticker in tickerToName.keys():
			tickerCount[ticker] = 0

		
		posts = []
		comments = []
		#Executes the database query to pull the posts from the last 24 hours
		WSBCursor.execute('''SELECT post_title, self_text, date_pulled FROM Posts
							WHERE DATE_SUB(date_pulled, INTERVAL 24 HOUR) < date_posted
							ORDER BY date_posted DESC
							''')
		for x in WSBCursor:
			posts.append(x)
		#Pulls all comments from the last 24 hours
		WSBCursor.execute('''SELECT text, date_pulled FROM Comments
							WHERE DATE_SUB(date_pulled, INTERVAL 24 HOUR) < date_posted
							ORDER BY date_posted DESC
							''')
		for x in WSBCursor:
			comments.append(x)
		
		#Gets the titles and text from each post pulled from the database
		tmptitles = list(map(lambda x: x[0],posts))
		tmpself_text = list(map(lambda x: x[1],posts))
		titles = []
		self_text = []
		#Does a little text preprocessing to make sure that line returns are treated as spaces
		for title in tmptitles:
			titles.append(title.replace('\n',' '))
		for text in self_text:
			#A lot of posts have no self text (they might be images or links instead), so this removes the self text from those
			if (text != ''):
				self_text.append(text.replace('\n',' '))
			
		for title in titles:
			for word in title.split(' '):
                #Removes stop words and counts tickers
				if ((word in tickerCount) & (word.lower() not in words)):
						tickerCount[word] += 1
                #Counts up named mentions and associates them to the appropriate ticker
				elif (word in stockNames):
						tickerCount[nameToTicker[word]] += 1
						
						
		for text in self_text:
			for word in text.split(' '):
				if ((word in tickerCount) & (word.lower() not in words)):
						tickerCount[word] += 1
				elif (word in stockNames):
						tickerCount[nameToTicker[word]] += 1
		

		#Processes the comment text similarly to the post text
		commentText = list(map(lambda x: x[0],comments))
		for text in commentText:
			for word in text.split(' '):
				if ((word in tickerCount) & (word.lower() not in words)):
					tickerCount[word] += 1
				elif (word in stockNames):
					tickerCount[nameToTicker[word]] += 1
        
        #Sorts the stock tickers based on mentions
		items = list(tickerCount.items())
		items.sort(key=lambda item: item[1],reverse=True)
		sortedTickers = [ item[0] for item in items ]

		tickersWithNames = []
		for item in items:
			tickersWithNames.append([item[0],tickerToName[item[0]],item[1]])
		#Returns the twenty most mentioned tickers
		return(jsonify({'tickers':tickersWithNames[0:20]}),200,{'Access-Control-Allow-Origin': '*'})
		
		
    #This is probably not the most descriptive error message
	else:
		return(jsonify({'uh':'oh'}),200,{'Access-Control-Allow-Origin': '*'})


	
	
	
if __name__ == '__main__':
	app.run(debug=True,host="0.0.0.0",port=int(os.environ.get("PORT",8080)))

