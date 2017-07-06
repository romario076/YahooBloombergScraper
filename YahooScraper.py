import urllib
import re
import pandas as pd
import json
from bs4 import BeautifulSoup

pd.set_option('display.width', 500)


class ParseYahooBbrg(object):

    def __init__(self, symbols):
        self.symbols = symbols
        print self.symbols

    def getYahooStatistic(self):
        data = []
        defaultUrl = "https://finance.yahoo.com/quote/{0}?p={0}"

        for i in range(0, len(self.symbols)):
            symbol = self.symbols[i]
            print "Getting from yahoo " + symbol
            url = defaultUrl.format(symbol)
            htmlFile = urllib.urlopen(url).read()
            regexPrice = re.compile(
                r'<span class="Trsdu\(0.3s\) Fw\(b\) Fz\(36px\) Mb\(-4px\) D\(ib\)" data-reactid="36">(.*?)</span>')
            regexYClose = re.compile(r'<span class="Trsdu\(0.3s\) " data-reactid="42">(.+?)</span>')
            regexOpen = re.compile(r'<span class="Trsdu\(0.3s\) " data-reactid="47">(.+?)</span>')
            regexVolume = re.compile(r'<span class="Trsdu\(0.3s\) " data-reactid="70">(.+?)</span>')
            regexBeta = re.compile(r'<span class="Trsdu\(0.3s\) " data-reactid="88">(.+?)</span>')
            price = re.findall(regexPrice, htmlFile)
            yClosePrice = re.findall(regexYClose, htmlFile)
            open = re.findall(regexOpen, htmlFile)
            volume = re.findall(regexVolume, htmlFile)
            beta = re.findall(regexBeta, htmlFile)
            data.extend([[symbol, price[0], yClosePrice[0], open[0], volume[0].replace(",", ""), beta[0]]])

        dataYahoo = pd.DataFrame(data, columns=["Symbol", "CurrentPrice", "YClose", "Open", "Volume", "Beta"])
        return dataYahoo

    def getBBRGStatistic(self):

        dataBBRG = pd.DataFrame(columns=["Symbol", "LowPrice52Week", "YClose", "LowPrice", "OpenPrice", "Volume", \
                                         "TotalReturn1Year", "HighPrice52Week", "PercentChange1Day", "PrimaryExchange"])
        defaultUrl = "https://www.bloomberg.com/markets/api/security/basic/{}%3AUS?locale=en"

        for i in range(0, len(self.symbols)):
            symbol = self.symbols[i].upper()
            try:
                print "Getting " + symbol + " from BBRG"
                url = defaultUrl.format(symbol)
                print url
                html = urllib.urlopen(url)
                data = json.load(html)
                dataBBRG = dataBBRG.append({"Symbol": symbol, "LowPrice52Week": data["lowPrice52Week"],
                                            "YClose": data["previousClosingPriceOneTradingDayAgo"], \
                                            "LowPrice": data["lowPrice"], "OpenPrice": data["openPrice"],
                                            "Volume": data["volume"], \
                                            "TotalReturn1Year": data["totalReturn1Year"],
                                            "HighPrice52Week": data["highPrice52Week"], \
                                            "PercentChange1Day": data["highPrice52Week"],
                                            "PrimaryExchange": data["primaryExchange"]}, ignore_index=True)
            except Exception:
                print symbol + " Something Wrong!"
                continue
        return dataBBRG

    def getHistoricalPricesBBRG(self, period):

        periods = ["1_DAY", "1_MONTH", "1_YEAR", "5_YEAR"]
        if period not in periods:
            print "Specified period not exist!"
            return 0
        defultUrl = "https://www.bloomberg.com/markets/api/bulk-time-series/price/{0}%3AUS?timeFrame={1}"
        histData = pd.DataFrame(columns=["Date", "Symbol", "Price"])
        for i in range(0, len(self.symbols)):
            symbol = self.symbols[i].upper()
            print "Getting "+symbol+" historical BBRG"

            try:
                url = defultUrl.format(symbol, period)

                html = urllib.urlopen(url)
                data = json.load(html)

                pattern = re.compile(r"'date':\s+u'(\d+-\d+-\d+)',\s+u'value':\s+(\d+)")
                prices = pattern.findall(str(data))
                histData = histData.append(pd.DataFrame({"Date": [x[0] for x in prices], \
                                                             "Symbol": [symbol]*len(prices), "Price": [x[1] for x in prices]}))
            except Exception:
                print "Something wrong with " + symbol
                continue
        return histData

    def getNewsBBRG(self):

        defaulUrl = "https://www.bloomberg.com/quote/{}:US"
        companyNews = pd.DataFrame(columns=["Time", "Symbol", "News"])
        for i in range(0, len(self.symbols)):
            symbol = self.symbols[i]
            print "Getting News " + symbol
            try:
                url = defaulUrl.format(symbol)
                html = urllib.urlopen(url).read()

                b = BeautifulSoup(html, "lxml")
                newsStory = b.findAll("article", {"class": "news-story"})
                newsList = []
                for item in newsStory:
                    newsList.append(str(item))

                pattern = re.compile(r"datetime=\"?(.+?)[Zz]\"?>.+</time>\s+<div\s+class=\".*\">\s+<a\s+class=.+?>(.+?)</a>\s+</div>\s+</article>$")
                for item in newsList:
                    regex = re.findall(pattern, item)
                    if len(regex) > 0:
                        companyNews = companyNews.append({"Symbol": symbol, "Time": regex[0][0].replace("T", " "), \
                                            "News": regex[0][1]}, ignore_index=True)
            except Exception:
                print "Something wrong with " + symbol
                continue
        return companyNews



symbols = ["AAPL", "GOOG", "AMZN"]

### Availbale periods for historical data: ["1_DAY", "1_MONTH", "1_YEAR", "5_YEAR"]


obj = ParseYahooBbrg(symbols)
dataYahoo = obj.getYahooStatistic()
dataBBRG = obj.getBBRGStatistic()
histData = obj.getHistoricalPricesBBRG(period="5_YEAR")
companyNews = obj.getNewsBBRG()


###Save Results
writer = pd.ExcelWriter("./results/Results.xlsx", engine="xlsxwriter")
dataYahoo.to_excel(writer, "Yahoo", index=False)
dataBBRG.to_excel(writer, "Bloomberg", index=False)
histData.to_excel(writer, "Historical", index=False)
writer.save()
companyNews.to_csv("results/News.txt", index=False)

