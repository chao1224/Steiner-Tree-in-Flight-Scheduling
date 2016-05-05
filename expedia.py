import requests, json, time, math, threading, csv
from datetime import datetime, timedelta

def calDate(departureDate, day):
	findDay = datetime.strptime(departureDate, "%Y-%m-%d") + timedelta(days=1) * day
	return findDay.strftime('%Y-%m-%d')

def calculateComfort(info):
	score_numOfSegments = 1.0 / info["numOfSegments"]
	score_totalFlightHours = max(0, 1.0 - math.exp(-4 + 0.5 * info["totalFlightHours"]))
	info["flight_comfort"] = 50 * score_numOfSegments + 50 * score_totalFlightHours

def queryFlight(departureAirport, resultList, slot):
	global apikey, session, baseURL, departureDate, totalDay, airportList, maxFlightBetweenTwoCity, city_to_code_dict, code_to_city_dict
	
	offerInfoList = []

	for currDay in range(totalDay):
		for arrivalAirport in airportList:
			if (departureAirport == arrivalAirport):
				continue

			# API query
			url = "{0}departureDate={1}&departureAirport={2}&arrivalAirport={3}&apikey={4}".format(\
				baseURL, calDate(departureDate, currDay), departureAirport, arrivalAirport, apikey)
			# print url
			js = json.loads(session.get(url).text)
			# print js
			# if (js.has_key("offers") == False or js.has_key("legs") == False):
			#	continue

			try:
				# extract legs info
				legInfoDict = {}
				for leg in js["legs"]:
					currLegInfo = {}
					currLegInfo["numOfSegments"] = len(leg["segments"])
					currLegInfo["totalFlightSeconds"] = leg["segments"][-1]["arrivalTimeEpochSeconds"] - leg["segments"][0]["departureTimeEpochSeconds"]
					currLegInfo["totalFlightHours"] = currLegInfo["totalFlightSeconds"] / 3600.0
					currLegInfo["totalFlightDistance"] = sum(x["distance"] for x in leg["segments"])
					# print "currLegInfo =", currLegInfo
					legInfoDict[leg["legId"]] = currLegInfo

				# extract offer info
				count = 0
				for offer in js["offers"]:
					count += 1
					if (count > maxFlightBetweenTwoCity):
						break
					# append info to offerInfoList
					currOfferInfo = {}
					currOfferInfo["departure_city"] = code_to_city_dict[departureAirport]
					currOfferInfo["departure_airport"] = departureAirport
					currOfferInfo["arrival_city"] = code_to_city_dict[arrivalAirport]
					currOfferInfo["arrival_airport"] = arrivalAirport
					currOfferInfo["flight_date"] =	currDay + 1
					currOfferInfo["flight_price"] = offer["totalFarePrice"]["amount"]
					currlegId = offer["legIds"][0]
					currOfferInfo["numOfSegments"] = legInfoDict[currlegId]["numOfSegments"]
					currOfferInfo["totalFlightHours"] = legInfoDict[currlegId]["totalFlightHours"]
					currOfferInfo["totalFlightDistance"] = legInfoDict[currlegId]["totalFlightDistance"]
					calculateComfort(currOfferInfo)  	# calculate "flight_comfort"
					# print "currOfferInfo =", currOfferInfo
					offerInfoList.append(currOfferInfo)
			except:
				print "url =", url

	resultList[slot] = offerInfoList
		

def extractThreads():
	global airportList, attributes
	threadList, resultList = [], [0] * len(airportList)

	# Step1: start mutiple threads
	for i in range(len(airportList)):
		t = threading.Thread(target=queryFlight, args=(airportList[i], resultList, i))
		threadList.append(t)
		t.start()
		# time.sleep(3)

	# Step2: wait for all threads
	for t in threadList:
		t.join()

	# Step3: write the results into csv file
	with open("flights.csv", "wb") as outcsv:
		writer = csv.writer(outcsv)
		writer.writerow(attributes)
		for i in range(len(airportList)):
			if (resultList[i] == 0):
				continue
			for record in resultList[i]:
				tempList = []
				for attribute in attributes:
					tempList.append(record[attribute])
				writer.writerow(tempList)

def cityToCode():
	global city_to_code_dict, code_to_city_dict
	city_to_code_dict = {}
	code_to_city_dict = {}
	with open('code.txt') as openfile:
		for record in openfile:
			curr = record.decode("utf-8").split()
			if (code_to_city_dict.has_key(curr[-1]) == False):
				city_to_code_dict[" ".join(curr[:-2])] = curr[-1]
				code_to_city_dict[curr[-1]] = " ".join(curr[:-2])
	openfile.close()

def extractCityInfo():
	global cityList, airportList, city_to_code_dict, code_to_city_dict
	# city and airport information
	cityToCode()
	airportList = []
	for city in cityList:
		airportList.append(city_to_code_dict[city])
	print cityList
	print airportList

	citySocre = {}
	citySocre["New York"] = 89
	citySocre["Boston"] = 81
	citySocre["Washington D.C."] = 77
	citySocre["Chicago"] = 78
	citySocre["Minneapolis"] = 68
	citySocre["Atlanta"] = 48
	citySocre["Miami"] = 78
	citySocre["Orlando"] = 41
	citySocre["Los Angeles"] = 66
	citySocre["San Francisco"] = 86
	citySocre["Seattle"] = 73
	citySocre["Madison"] = 48

	'''
	with open("locations.csv", "wb") as outcsv:
		writer = csv.writer(outcsv)
		writer.writerow(["city", "airport", "latitude", "longitude", "score"])

		for i in range(len(cityList)):
			city = cityList[i]
			airport = airportList[i]
			# API query
			url = "http://maps.googleapis.com/maps/api/geocode/json?address=airport+" + airport
			print url
			js = json.loads(session.get(url).text)
			print js
			tempList = [city, airport]
			tempList.append(js["results"][0]["geometry"]["location"]["lat"])
			tempList.append(js["results"][0]["geometry"]["location"]["lng"])
			tempList.append(citySocre[city])
			writer.writerow(tempList)
	'''

def init():
    global apikey, session, baseURL, departureDate, totalDay, cityList, maxFlightBetweenTwoCity, attributes
    apikey = "8lUYqEjewV1DSoOKQUZ51TVdzNYkhJxw"
    session = requests.Session()
    baseURL = "http://terminal2.expedia.com/x/mflights/search?"
    departureDate = "2016-07-01"
    totalDay = 1

    cityList = ["New York", "Boston", "Washington D.C.", "Chicago", "Minneapolis", "Atlanta", "Miami", "Orlando", "Los Angeles", "San Francisco", "Seattle", "Madison"]
    # cityList = ["Miami", "Orlando"]
    maxFlightBetweenTwoCity = 3

    # extracted attributes
    attributes = ["departure_city", "departure_airport", "arrival_city", "arrival_airport", "flight_date", "flight_price", "flight_comfort", "numOfSegments", "totalFlightHours", "totalFlightDistance"]

init()
extractCityInfo()
extractThreads()