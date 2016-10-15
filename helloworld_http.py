#!/usr/bin/env python


import logging, requests, urllib2, json, re, operator
from spyne import Application, srpc, ServiceBase, Iterable, UnsignedInteger, \
    String, Decimal
from spyne.protocol.json import JsonDocument
from spyne.protocol.http import HttpRpc
from spyne.server.wsgi import WsgiApplication
from collections import Counter
from datetime import datetime, time

class HelloWorldService(ServiceBase):
    @srpc(Decimal, Decimal, _returns=Iterable(String))       
    def checkcrime(lat, lon):
        parameters = {'lat':lat,'lon':lon,'radius':'0.02','key':'.'}
        url = 'https://api.spotcrime.com/crimes.json'
        response = requests.get(url,params=parameters).json()
        
        #array for extracting the total number of crimes
        totalcrimes = [item['cdid'] for item in response['crimes']]
        
        #counter set for the type of crime 
        typeofcrime = Counter([item['type'] for item in response['crimes']])
        
        #time is extracted from the date field and converted to 24 hour format and compared with the specified time intervals
        times = [item['date'] for item in response['crimes']]
        out = [x.partition(" ") for x in times]
        out2 = sorted([(x[2]) for x in out])
        out_time = []
        for i in range(len(out2)):
            in_time = datetime.strptime(out2[i], "%I:%M %p")
            out_time.append(datetime.strftime(in_time, "%H:%M"))
        midto3 = threeto6 = sixto9 = nineto12 = twelveto15 = fifteento18 = eighteento21 = twentyoneto24 = 0
        for t in out_time:
            if str(time(00,01)) <= t <= str(time(03,00)):
                midto3 +=1
            elif str(time(03,01)) <= t <= str(time(06,00)):
                threeto6 +=1
            elif str(time(06,01)) <= t <= str(time(9,00)):
                sixto9 +=1
            elif str(time(9,01)) <= t <= str(time(12,00)):
                nineto12 +=1
            elif str(time(12,01)) <= t <= str(time(15,00)):
                twelveto15 +=1
            elif str(time(15,01)) <= t <= str(time(18,00)):
                fifteento18 +=1
            elif str(time(18,01)) <= t <= str(time(21,00)):
                eighteento21 +=1
            elif str(time(21,01)) <= t <= str(time(00,00)):
                twentyoneto24 +=1
 
        #address is validated to extract the street names and counter set to take the count of no of streets and the top 3 are returned
        address = [item['address'] for item in response['crimes']]
        streets = []
        for x in address:
            if 'OF' in x:
                outstreet = x.partition(" OF ")
                for o in outstreet:
                    if "ST" in o:
                        streets.append(o)
            if '&' in x:
                outstreet = x.partition(" & ")
                for o in outstreet:
                    if "ST" in o:
                        streets.append(o)
        streetcounter = Counter(streets)
        newstreets = sorted(streetcounter.iteritems(), key=operator.itemgetter(1), reverse=True)[:3]
        my_list = [l[0] for l in newstreets]
        
        #Display statements as the format specified
        somedict = { "total_crime" : len(totalcrimes) ,"event_time_count" :{ "12:01am-3am " :  midto3, "3:01am-6am " :  threeto6, "6:01am-9am " : sixto9, "9:01am-12noon " : nineto12,  "12:01pm-3pm " : twelveto15,  "3:01pm-6pm " : fifteento18, "6:01pm-9pm " : eighteento21, "9:01pm-12midnight" : twentyoneto24 }, "crime_type_count" : typeofcrime, "the_most_dangerous_streets " : my_list}
        yield somedict
        


if __name__ == '__main__':
    # Python daemon boilerplate
    from wsgiref.simple_server import make_server

    logging.basicConfig(level=logging.DEBUG)

    # Instantiate the application by giving it:
    #   * The list of services it should wrap,
    #   * A namespace string.
    #   * An input protocol.
    #   * An output protocol.
    application = Application([HelloWorldService], 'spyne.examples.hello.http',
        # The input protocol is set as HttpRpc to make our service easy to
        # call. Input validation via the 'soft' engine is enabled. (which is
        # actually the the only validation method for HttpRpc.)
        in_protocol=HttpRpc(validator='soft'),

        # The ignore_wrappers parameter to JsonDocument simplifies the reponse
        # dict by skipping outer response structures that are redundant when
        # the client knows what object to expect.
        out_protocol=JsonDocument(ignore_wrappers=True),
    )

    # Now that we have our application, we must wrap it inside a transport.
    # In this case, we use Spyne's standard Wsgi wrapper. Spyne supports
    # popular Http wrappers like Twisted, Django, Pyramid, etc. as well as
    # a ZeroMQ (REQ/REP) wrapper.
    wsgi_application = WsgiApplication(application)

    # More daemon boilerplate
    server = make_server('127.0.0.1', 8000, wsgi_application)

    logging.info("listening to http://127.0.0.1:8000")
    logging.info("wsdl is at: http://localhost:8000/?wsdl")

    server.serve_forever()