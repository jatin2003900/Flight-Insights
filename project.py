import datetime
from PIL import Image
from geopy import Nominatim
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, avg, round as rou, count, when, to_timestamp, to_date, max
from pyspark.sql.types import IntegerType
import streamlit
import pandas
import pydeck
import time

path1 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_1/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_1.csv"
path2 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_2/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_2.csv"
path3 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_3/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_3.csv"
path4 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_4/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_4.csv"
path5 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_5/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_5.csv"
path6 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_6/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_6.csv"
path7 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_7/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_7.csv"
path8 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_8/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_8.csv"
path9 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_9/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_9.csv"
path10 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_10/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_10.csv"
path11 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_11/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_11.csv"
path12 = "C:/Users/gaiab/Desktop/Big data/project/Dataset/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_12/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_12.csv"
pathCor = "C:/Users/gaiab/Desktop/Big data/project/Dataset/Coordinates.csv"
pathCoor = "C:/Users/gaiab/Desktop/Big data/project/coordinates.csv"
image = "C:/Users/gaiab/Desktop/Big data/project/sidebar.jpg"

# Create SparkSession
spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()
spark.sparkContext.setLogLevel("OFF")

# Reading dataset
df = ((spark.read.options(delimiter=",").csv(path1, header=True)) \
      .union(spark.read.options(delimiter=",").csv(path2, header=True)) \
      .union(spark.read.options(delimiter=",").csv(path3, header=True)) \
      .union(spark.read.options(delimiter=",").csv(path4, header=True)) \
      .union(spark.read.options(delimiter=",").csv(path5, header=True)) \
      .union(spark.read.options(delimiter=",").csv(path6, header=True)) \
      .union(spark.read.options(delimiter=",").csv(path7, header=True)) \
      .union(spark.read.options(delimiter=",").csv(path8, header=True)) \
      .union(spark.read.options(delimiter=",").csv(path9, header=True)) \
      .union(spark.read.options(delimiter=",").csv(path10, header=True)) \
      .union(spark.read.options(delimiter=",").csv(path11, header=True)) \
      .union(spark.read.options(delimiter=",").csv(path12, header=True)))\
      .na.drop(subset=["Flight_Number_Reporting_Airline"]).cache()


# Cast to integer
df = df.withColumn("ArrDelayMinutes", df["ArrDelayMinutes"].cast(IntegerType()))
df = df.withColumn("CarrierDelay", df["CarrierDelay"].cast(IntegerType()))
df = df.withColumn("WeatherDelay", df["WeatherDelay"].cast(IntegerType()))
df = df.withColumn("NASDelay", df["NASDelay"].cast(IntegerType()))
df = df.withColumn("SecurityDelay", df["SecurityDelay"].cast(IntegerType()))
df = df.withColumn("LateAircraftDelay", df["LateAircraftDelay"].cast(IntegerType()))
df = df.withColumn("Distance", df["Distance"].cast(IntegerType()))

# Cast to time
df = df.withColumn('DepTime', to_timestamp("DepTime", "HHmm"))
df = df.withColumn("ArrTime",  to_timestamp('ArrTime', 'HHmm'))
df = df.withColumn('CRSArrTime', to_timestamp("CRSArrTime", "HHmm"))
df = df.withColumn("CRSDepTime",  to_timestamp('CRSDepTime', 'HHmm'))
df = df.withColumn("DivArrDelay",  to_timestamp('DivArrDelay', 'HHmm'))
df = df.withColumn("FlightDate", to_date(col("FlightDate"), "yyy-MM-dd"))

# User interface
streamlit.set_page_config(page_title='Analisi dei voli', layout="wide")
streamlit.sidebar.image(image=Image.open(image), width=300)
streamlit.sidebar.header('Informazioni sui voli in America' + '\n' + ' Anno 2013')
menu = streamlit.sidebar.radio("Scegli una pagina da visualizzare", ('Homepage', 'Informazioni sui voli mensili',
            'Informazioni sui ritardi', 'Informazioni sui dirottamenti', 'Ricerca voli'))


# Monthly count of flights
def countFlightsMonth():
    return df.groupby("month").count()

# Number of rows and columns of dataset
def dimensionRows():
    return str(df.count())
def dimensionCol():
    return str(len(df.columns))

# Airplane which made most flights
def mostTravelsAirplane():
    return df.na.drop(subset=["Tail_Number"]).groupby("Tail_Number").count().orderBy(desc("count"))

# Routes and their average delay
def routesAverageDelay():
    return df.groupby("Origin", "Dest", "OriginCityName", "DestCityName").agg(avg("ArrDelay").alias("Delay")).orderBy(desc("Delay"))

# Routes and number of delay related to the total amount
def routesNumberDelay():
    return df.groupby("Origin", "Dest", "OriginCityName", "DestCityName").agg(count(when(col("ArrDelayMinutes") > 0, 1)).alias("Delay"),
    count(col("ArrDelayMinutes")).alias("Total"),
    rou((count(when(col("ArrDelayMinutes") > 0, 1)) / (count(col("ArrDelayMinutes"))) * 100), 2).alias("Percentage")).orderBy(desc("Percentage"))

# Routes and number of diverted flights
def routesNumberDiverted():
    return df.groupby("Origin", "Dest", "OriginCityName", "DestCityName").agg(count(when(col("Diverted") == "1.00", 1)).alias("Diverted"),
    count(col("Diverted")).alias("Total"), count(col("DivReachedDest") == "1.00").alias("DivReachedDest"),
    rou((count(when(col("Diverted") == "1.00", 1)) / (count(col("Diverted"))) * 100), 2).alias("Percentage"))

# Routes with deverted flights which reached their destination
def routesNumberReached():
    return df.agg(count(when(col("Diverted") == "1.00", 1)).alias("Diverted"),
                         count(col("Diverted")).alias("Total"),
                         count(when(col("DivReachedDest") == "1.00", 1)).alias("DivReachedDest"))

# Number of diverted airport landings
def deviatedLandingsNumber():
    return df.filter(col("DivAirportLandings") != 0).groupby("DivAirportLandings").count()

# Motive that caused more delay and the average time each time there was a delay
# NO DIVERTED FLIGHTS
def reasonsDelayAverage():
    total = df.where(col("CarrierDelay") >= 0).count()
    result = df.groupby().agg(rou(avg("CarrierDelay")).alias("Carrier delay"),
           rou((count(when(col("CarrierDelay") > 0, 1)) / total * 100), 2).alias("Percentage"),
         rou(avg("WeatherDelay")).alias("Weather delay"),
           rou((count(when(col("WeatherDelay") > 0, 1)) / total * 100), 2).alias("Percentage"),
         rou(avg("NASDelay")).alias("NAS delay"),
           rou((count(when(col("NASDelay") > 0, 1)) / total * 100),2).alias("Percentage"),
         rou(avg("SecurityDelay")).alias("Security delay"),
           rou((count(when(col("SecurityDelay") > 0, 1)) / total * 100), 2).alias("Percentage"),
         rou(avg("LateAircraftDelay")).alias("Late Aircraft delay"),
           rou((count(when(col("LateAircraftDelay") > 0, 1)) / total * 100),2).alias("Percentage"))
    return result

# Route with more flights
def mostTravels():
    return df.groupby("Origin", "Dest", "OriginCityName", "DestCityName").count().orderBy(desc("count"))

# Route with the biggest distance
def biggestDistance():
    return df.groupby("Origin", "Dest", "OriginCityName", "DestCityName").max("Distance").orderBy(desc("max(Distance)"))

# Prints the coordinates of all airports
def routes():
    locator = Nominatim(user_agent="myNewGeocoder")
    df2 = df.dropDuplicates(["Origin", "Dest"]).collect()

    for row in df2:
        # Coordinates for Origin
        time.sleep(1)
        found = locator.geocode(row["Origin"] + " Airport " + row["OriginCityName"], timeout=None)
        if found is None:
            time.sleep(1)
            found = locator.geocode(row["OriginCityName"], timeout=None)
        locationOr = found

        # Coordinates for destination
        time.sleep(1)
        found = locator.geocode(row["Dest"] + " Airport " + row["DestCityName"], timeout=None)
        if found is None:
            time.sleep(1)
            found = locator.geocode(row["DestCityName"], timeout=None)
        locationDes = found

        # Printing of coordinates
        if locationOr is not None and locationDes is not None:
            print(locationOr.latitude + "," + locationOr.longitude + "," + locationDes.latitude + "," + locationDes.longitude)

# Returns the coordinates for a given airport
def coordinates(origin, originCityName, destination, destinationCityName):
    locator = Nominatim(user_agent="myNewGeocoder")

    # Coordinates for Origin
    found = locator.geocode(origin + " Airport " + originCityName, timeout=None)
    if found is None:
        time.sleep(1)
        found = locator.geocode(originCityName, timeout=None)
    result = [found.latitude, found.longitude]

    # Coordinates for destination
    time.sleep(1)
    found = locator.geocode(destination + " Airport " + destinationCityName, timeout=None)
    if found is None:
        time.sleep(1)
        found = locator.geocode(destinationCityName, timeout=None)
    result.append(found.latitude)
    result.append(found.longitude)
    dfRes = pandas.DataFrame({'OriginLatitude': [result[0]], 'OriginLongitude': [result[1]],
                              'DesLatitude': [result[2]], 'DesLongitude': [result[3]]})
    return dfRes

# Returns the months associated with the number
def findMonth(number):
    if number == 0:
        return "Gennaio"
    elif number == 1:
        return "Febbraio"
    elif number == 2:
        return "Marzo"
    elif number == 3:
        return "Aprile"
    elif number == 4:
        return "Maggio"
    elif number == 5:
        return "Giugno"
    elif number == 6:
        return "Luglio"
    elif number == 7:
        return "Agosto"
    elif number == 8:
        return "Settembre"
    elif number == 9:
        return "Ottobre"
    elif number == 10:
        return "Novembre"
    else:
        return "Dicembre"

# First page
if menu == "Homepage":
    streamlit.title('Homepage')
    streamlit.markdown('Il dataset comprende i dati dei voli domestici americani eseguiti nell\'arco del 2013. '
                       'E\' riportata la mappa di tutti gli aeroporti.'
                       ' Tra i dati riportati sono presenti la tratta più trafficata e il veivolo che ha compiuto più voli. ')
    streamlit.markdown("""---""")

    # Data for the map
    dfTem = pandas.read_csv(pathCoor, delimiter=",")
    dfMap1 = pandas.DataFrame()
    dfMap1 = dfTem[['OriginLat', 'OriginLon']]
    dfMap1.rename(columns={'OriginLat': 'LAT', 'OriginLon': 'LON'}, inplace=True)
    dfMap2 = pandas.DataFrame()
    dfMap2 = dfTem[['DestinationLat', 'DestinationLon']]
    dfMap2.rename(columns={'DestinationLat': 'LAT', 'DestinationLon': 'LON'}, inplace=True)
    dfMap = pandas.concat([dfMap1, dfMap2])

    # First row - map
    streamlit.markdown(":red[Mappa degli aeroporti]")
    streamlit.markdown("Ogni punto sulla mappa rappresenta un aeroporto da cui è partito, atterrato o transitato un aereo")

    streamlit.map(data=dfMap, zoom=1)
    streamlit.markdown("""---""")

    # Second row - info
    col01, col02 = streamlit.columns(2)
    col01.markdown('_Informazioni sul dataset_')
    col02.markdown('_Aeroplano con più viaggi_')
    col1, col2, col3, col4 = streamlit.columns(4)
    col1.metric(label=":blue[Numero di righe] ", value=dimensionRows())
    col2.metric(label=":blue[Numero di colonne] ", value=dimensionCol())
    dfTailNumber = mostTravelsAirplane()
    col3.metric(label=":blue[Tail number] ", value=dfTailNumber.take(num=1)[0].__getitem__("Tail_Number"))
    col4.metric(label=":blue[Numero di Voli] ", value=dfTailNumber.take(num=1)[0].__getitem__("count"))
    streamlit.markdown("""---""")

    # Third row - maps
    col5, col6 = streamlit.columns(2)
    with col5:
        travelD = biggestDistance().take(num=1)[0]
        lab1 = 'Tratta con maggiore distanza:' + " From " + travelD.__getitem__(
            "OriginCityName") + " to " + travelD.__getitem__("DestCityName")
        streamlit.metric(label=lab1, value=str(travelD.__getitem__("max(Distance)")) + " miglia")
        coorD = coordinates(travelD.__getitem__("Origin"), travelD.__getitem__("OriginCityName"),
                            travelD.__getitem__("Dest"), travelD.__getitem__("DestCityName"))

        col5.pydeck_chart(
            pydeck.Deck(
                # map_style=None,
                initial_view_state=pydeck.ViewState(
                    latitude=coorD.iloc[0].__getitem__("OriginLatitude"),
                    longitude=coorD.iloc[0].__getitem__("OriginLongitude"),
                    zoom=5,
                    pitch=50,
                    height=250,
                ),
                layers=[
                    pydeck.Layer(
                        "ArcLayer",
                        data=coorD,
                        get_source_position=["OriginLongitude", "OriginLatitude"],
                        get_target_position=["DesLongitude", "DesLatitude"],
                        get_source_color=[200, 30, 0, 160],
                        get_target_color=[200, 30, 0, 160],
                        auto_highlight=True,
                        width_scale=0.0001,
                        get_width="outbound",
                        width_min_pixels=3,
                        width_max_pixels=30,

                        radius=200,
                        elevation_scale=4,
                        elevation_range=[0, 1000],
                        pickable=True,
                        extruded=True,
                    ),
                ],
            )
        )

    with col6:
        travel = mostTravels().orderBy(desc("count")).take(num=1)[0]
        lab2 = 'Tratta con più voli:' + " From " + travel.__getitem__(
            "OriginCityName") + " to " + travel.__getitem__("DestCityName")
        streamlit.metric(label=lab2, value=travel.__getitem__("count"))
        coorA = coordinates(travel.__getitem__("Origin"), travel.__getitem__("OriginCityName"),
                            travel.__getitem__("Dest"), travel.__getitem__("DestCityName"))

        col6.pydeck_chart(
            pydeck.Deck(
                initial_view_state=pydeck.ViewState(
                    latitude=coorA.iloc[0].__getitem__("OriginLatitude"),
                    longitude=coorA.iloc[0].__getitem__("OriginLongitude"),
                    zoom=5,
                    pitch=50,
                    height=250,
                ),
                layers=[
                    pydeck.Layer(
                        "ArcLayer",
                        data=coorA,
                        get_source_position=["OriginLongitude", "OriginLatitude"],
                        get_target_position=["DesLongitude", "DesLatitude"],
                        get_source_color=[200, 30, 0, 160],
                        get_target_color=[200, 30, 0, 160],
                        auto_highlight=True,
                        width_scale=0.0001,
                        get_width="outbound",
                        width_min_pixels=3,
                        width_max_pixels=30,

                        radius=200,
                        elevation_scale=4,
                        elevation_range=[0, 1000],
                        pickable=True,
                        extruded=True,
                    ),
                ],
            )
        )


if menu == "Informazioni sui voli mensili":
    streamlit.title('Informazioni sui voli mensili')
    streamlit.markdown('Per ciascun mese è riportato il numero di voli eseguiti con l\'incremento rispetto al precedente.'
                       ' In blu è evidenziato il mese con più voli di tutto l\'anno 2013 ')
    streamlit.markdown("""---""")

    # Data
    monthsFlightDF = countFlightsMonth()
    monthMostFlights = int(monthsFlightDF.orderBy(desc("count")).take(num=1)[0].__getitem__("month")) - 1
    monthsFlight = monthsFlightDF.collect()

    # Stream
    prev = 0
    for i in range(0, 12, 4):
        col1, col2, col3, col4 = streamlit.columns(4)

        val = monthsFlight[i].__getitem__("count")
        lab = findMonth(i)
        if prev == 0:
            delta = 0
        else:
            delta = val-prev
        if i == 0:
            deltaCol = "off"
        else:
            deltaCol = "normal"
        if i == monthMostFlights:
            col1.metric(label=lab + ":blue[  Mese con più voli] ", value=val, delta=delta, delta_color=deltaCol)
        else:
            col1.metric(label=lab, value=val, delta=delta, delta_color="normal")
        prev = val

        val = monthsFlight[i+1].__getitem__("count")
        lab = findMonth(i+1)
        delta = val - prev
        if (i+1) == monthMostFlights:
            col2.metric(label=lab + ":blue[  Mese con più voli] ", value=val, delta=delta, delta_color="normal")
        else:
            col2.metric(label=lab, value=val, delta=delta, delta_color="normal")
        prev = val

        val = monthsFlight[i+2].__getitem__("count")
        lab = findMonth(i+2)
        delta = val - prev
        if (i+2) == monthMostFlights:
            col3.metric(label=lab + ":blue[  Mese con più voli] ", value=val, delta=delta, delta_color="normal")
        else:
            col3.metric(label=lab, value=val, delta=delta, delta_color="normal")
        prev = val

        val = monthsFlight[i+3].__getitem__("count")
        lab = findMonth(i + 3)
        delta = val - prev
        if (i+3) == monthMostFlights:
            col4.metric(label=lab + ":blue[  Mese con più voli] ", value=val, delta=delta, delta_color="normal")
        else:
            col4.metric(label=lab, value=val, delta=delta, delta_color="normal")
        prev = val


if menu == "Informazioni sui ritardi":
    streamlit.title('Informazioni sui ritardi')
    streamlit.markdown('Sono riportate le informazioni inerenti la tratta con ritardo medio maggiore e le motivazioni che causano i  ritardi.'
                       ' Puoi visualizzare la tratta con la percentuale di ritardi maggiore e che compie un numero minimo di voli indicando il valore attraverso lo slider ')
    streamlit.markdown("""---""")

    col1, col2 = streamlit.columns(2)

    # Route with the most average delay
    dfAverageDelay = routesAverageDelay().take(num=1)[0]
    col1.markdown('_Tratta aerea con ritardo medio maggiore:_')
    col1.markdown(":red[Volo da **" + dfAverageDelay.__getitem__("OriginCityName") + "** a **" +
                  dfAverageDelay.__getitem__("DestCityName") + "**]")
    col1.metric(label="From " + dfAverageDelay.__getitem__("Origin") + " To " + dfAverageDelay.__getitem__("Dest"),
                value=str(dfAverageDelay.__getitem__("Delay")) + " Minutes")

    coor1 = coordinates(dfAverageDelay.__getitem__("Origin"), dfAverageDelay.__getitem__("OriginCityName"),
                              dfAverageDelay.__getitem__("Dest"), dfAverageDelay.__getitem__("DestCityName"))

    col2.pydeck_chart(
        pydeck.Deck(
            #map_style=None,
            initial_view_state=pydeck.ViewState(
                latitude=coor1.iloc[0].__getitem__("OriginLatitude"),
                longitude=coor1.iloc[0].__getitem__("OriginLongitude"),
                zoom=5,
                pitch=50,
                height=250,
            ),
            layers=[
                pydeck.Layer(
                    "ArcLayer",
                    data=coor1,
                    get_source_position=["OriginLongitude", "OriginLatitude"],
                    get_target_position=["DesLongitude", "DesLatitude"],
                    get_source_color=[200, 30, 0, 160],
                    get_target_color=[200, 30, 0, 160],
                    auto_highlight=True,
                    width_scale=0.0001,
                    get_width="outbound",
                    width_min_pixels=3,
                    width_max_pixels=30,

                    radius=200,
                    elevation_scale=4,
                    elevation_range=[0, 1000],
                    pickable=True,
                    extruded=True,
                ),
            ],
        )
    )
    streamlit.markdown("""---""")
    col3, col4 = streamlit.columns(2)

    # Route with the biggest number of delay
    dfNumberDelayTotal = routesNumberDelay()
    maxVal = dfNumberDelayTotal.select(max("Total")).take(num=1)[0].__getitem__("max(Total)")
    number = col3.slider('Seleziona un numero di voli minimo', min_value=0, max_value=maxVal, step=1)
    dfNumberDelay = dfNumberDelayTotal.filter(col("Total") >= number).take(num=1)[0]
    col3.markdown('_Tratta aerea con percentuale di voli in ritardo maggiore:_')
    col3.markdown(
        ":red[Volo da **" + dfNumberDelay.__getitem__("OriginCityName") + "** a **" + dfNumberDelay.__getitem__(
            "DestCityName") + "**]")

    col3.metric(label="Numero di voli in ritardo",
                value=str(str(dfNumberDelay.__getitem__("Delay")) + " su " + str(dfNumberDelay.__getitem__("Total"))))
    col3.metric(label="Percentuale di voli in ritardo sul totale",
                value=str(dfNumberDelay.__getitem__("Percentage")) + " %")

    coor2 = coordinates(dfNumberDelay.__getitem__("Origin"), dfNumberDelay.__getitem__("OriginCityName"),
                              dfNumberDelay.__getitem__("Dest"), dfNumberDelay.__getitem__("DestCityName"))

    col4.pydeck_chart(
        pydeck.Deck(
            # map_style=None,
            initial_view_state=pydeck.ViewState(
                latitude=coor2.iloc[0].__getitem__("OriginLatitude"),
                longitude=coor2.iloc[0].__getitem__("OriginLongitude"),
                zoom=5,
                pitch=50,
                height=400,
            ),
            layers=[
                pydeck.Layer(
                    "ArcLayer",
                    data=coor2,
                    get_source_position=["OriginLongitude", "OriginLatitude"],
                    get_target_position=["DesLongitude", "DesLatitude"],
                    get_source_color=[200, 30, 0, 160],
                    get_target_color=[200, 30, 0, 160],
                    auto_highlight=True,
                    width_scale=0.0001,
                    get_width="outbound",
                    width_min_pixels=3,
                    width_max_pixels=30,

                    radius=200,
                    elevation_scale=4,
                    elevation_range=[0, 1000],
                    pickable=True,
                    extruded=True,
                ),
            ],
        )
    )

    streamlit.markdown("""---""")
    streamlit.markdown('_Motivazioni delle cancellazioni_')
    streamlit.markdown('I seguenti chart mostrano la percentuale di incidenza per ciascuna motivazione di ritardo sul totale.'
                       ' Il calcolo tiene in considerazione i minuti di ritardo')

    col5, col6, col7, col8, col9 = streamlit.columns(5)
    dfDelay = reasonsDelayAverage().collect()

    # Carrier delay
    with col5:
        streamlit.metric(label=":blue[Carrier delay]", value= str(dfDelay[0][0]) + "%")
        value = dfDelay[0][1]
        sizes = [value, (100-value)]
        colors = ['#4169E1', '#DCDCDC']
        fig, ax = plt.subplots()
        ax.pie(sizes, shadow=True, startangle=90, labeldistance=1.15,
               wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white' }, colors=colors)
        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        p = plt.gcf()
        p.gca().add_artist(plt.Circle((0, 0), 0.8, color='white'))
        fig.patch.set_facecolor('#0e1117')
        streamlit.pyplot(fig)

    with col6:
        streamlit.metric(label=":blue[Weather delay]", value= str(dfDelay[0][3]) + "%")
        value = dfDelay[0][3]
        sizes = [value, (100-value)]
        colors = ['#4169E1', '#DCDCDC']
        fig, ax = plt.subplots()
        ax.pie(sizes, shadow=True, startangle=90, labeldistance=1.15, wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white' }, colors=colors)
        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        p = plt.gcf()
        p.gca().add_artist(plt.Circle((0, 0), 0.8, color='white'))
        fig.patch.set_facecolor('#0e1117')
        streamlit.pyplot(fig)
    with col7:
        streamlit.metric(label=":blue[NAS delay]", value= str(dfDelay[0][5]) + "%")
        value = dfDelay[0][5]
        sizes = [value, (100-value)]
        colors = ['#4169E1', '#DCDCDC']
        fig, ax = plt.subplots()
        ax.pie(sizes, shadow=True, startangle=90, labeldistance=1.15, wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white' }, colors=colors)
        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        p = plt.gcf()
        p.gca().add_artist(plt.Circle((0, 0), 0.8, color='white'))
        fig.patch.set_facecolor('#0e1117')
        streamlit.pyplot(fig)
    with col8:
        streamlit.metric(label=":blue[Security delay]", value= str(dfDelay[0][7]) + "%")
        value = dfDelay[0][7]
        sizes = [value, (100-value)]
        colors = ['#4169E1', '#DCDCDC']
        fig, ax = plt.subplots()
        ax.pie(sizes, shadow=True, startangle=90, labeldistance=1.15, wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white' }, colors=colors)
        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        p = plt.gcf()
        p.gca().add_artist(plt.Circle((0, 0), 0.8, color='white'))
        fig.patch.set_facecolor('#0e1117')
        streamlit.pyplot(fig)
    with col9:
        streamlit.metric(label=":blue[Late Aircraft delay]", value= str(dfDelay[0][9]) + "%")
        value = dfDelay[0][9]
        sizes = [value, (100-value)]
        colors = ['#4169E1', '#DCDCDC']
        fig, ax = plt.subplots()
        ax.pie(sizes, shadow=True, startangle=90, labeldistance=1.15, wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white' }, colors=colors)
        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        p = plt.gcf()
        p.gca().add_artist(plt.Circle((0, 0), 0.8, color='white'))
        fig.patch.set_facecolor('#0e1117')
        streamlit.pyplot(fig)


if menu == "Informazioni sui dirottamenti":
    streamlit.title('Dirottamenti')
    streamlit.markdown('Nella pagina seguente è possibile visualizzare alcune informazioni relativamente alle deviazioni '
                       'subite nelle tratte aeree. Inoltre, sono riportate le informazioni relative alla tratta aerea con '
                       'più dirottamenti in valore assoluto.')
    streamlit.markdown("""---""")

    col1, col2, col3 = streamlit.columns(3)

    # Route with more diverted flights
    dfNumberDiverted = routesNumberDiverted()
    dfNumberDivertedTot = dfNumberDiverted.orderBy(desc("Diverted")).take(num=1)[0]

    with col1:
        streamlit.markdown('_Tratta aerea con il maggior numero di voli dirottati:_')

        streamlit.metric(label="",value=str(dfNumberDivertedTot.__getitem__("Diverted")) + " voli su " +
                                        str(dfNumberDivertedTot.__getitem__("Total")))

        value = dfNumberDivertedTot.__getitem__("Percentage")
        sizes = [value, (100-value)]
        mylabels = ["Dirottati", "Non dirottati "]
        fig, ax = plt.subplots()
        ax.pie(sizes, labels=mylabels, shadow=True, startangle=90, labeldistance=1.15,
               wedgeprops={'linewidth': 3, 'edgecolor': 'white'})
        ax.axis('equal')
        p = plt.gcf()
        p.gca().add_artist(plt.Circle((0, 0), 0.8, color='white'))
        fig.patch.set_facecolor('#0e1117')
        ax.legend(title="Legenda")
        streamlit.pyplot(fig)

        streamlit.markdown(":red[Volo da **" + dfNumberDivertedTot.__getitem__("OriginCityName") + "** a **" +
                           dfNumberDivertedTot.__getitem__("DestCityName") + "**]")

    with col2:
        streamlit.markdown('_Numero di voli dirottati giunti a destinazione:_')
        reached = routesNumberReached().take(num=1)[0]
        value2 = reached.__getitem__("DivReachedDest")
        value3 = reached.__getitem__("Total")
        streamlit.metric(label="", value=str(value2) + " su " + str(value3))

        sizes2 = [value2, (value3 - value2)]
        mylabels2 = ["Giunti a destinazione", "Non giunti a destinazione "]
        fig, ax = plt.subplots()
        ax.pie(sizes2, labels=mylabels2, shadow=True, startangle=90, labeldistance=1.15,
               wedgeprops={'linewidth': 3, 'edgecolor': 'white'})
        ax.axis('equal')
        p = plt.gcf()
        p.gca().add_artist(plt.Circle((0, 0), 0.8, color='white'))
        fig.patch.set_facecolor('#0e1117')
        ax.legend(title="Legenda")
        streamlit.pyplot(fig)

    with col3:
        streamlit.markdown('_Numero di atterraggi deviati_:')
        numberLandings = deviatedLandingsNumber().collect()
        n = len(numberLandings)
        sizes3 = []
        mylabels3 = []
        for i in range(n):
            sizes3 = sizes3 + [numberLandings[i].__getitem__("count")]
            mylabels3 = mylabels3 + [i+1]
        fig, ax = plt.subplots()
        ax.pie(sizes3, labels=mylabels3, shadow=True, startangle=90, labeldistance=1.15,
               wedgeprops={'linewidth': 3, 'edgecolor': 'white'})
        ax.axis('equal')
        p = plt.gcf()
        p.gca().add_artist(plt.Circle((0, 0), 0.8, color='white'))
        fig.patch.set_facecolor('#0e1117')
        ax.legend(title="Legenda")
        streamlit.pyplot(fig)


if menu == "Ricerca voli":
    streamlit.title('Ricerca voli')
    streamlit.markdown('Puoi ricercare un volo inserendo la data, l\'ora di partenza, '
                       'l\'aeroporto di partenza e quello di arrivo. '
                       'Ti verranno visualizzate le informazioni relative all\'orario di partenza ed arrivo effettivo, la distanza e '
                       'relativamente alla tratta anche il ritardo medio e la percentuale di voli cancellati')
    streamlit.markdown("""---""")

    col1, col2 = streamlit.columns(2)
    with col1:
        giorno = streamlit.date_input("Digita o scegli una data", datetime.date(2013, 1, 1))
        streamlit.markdown("""---""")
        partenza = streamlit.time_input('Digita o scegli un orario di partenza', datetime.time(9, 00))

        p = datetime.datetime.strptime("1970-01-01 " + partenza.strftime("%H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        flightsP = df.filter(col("FlightDate") == giorno).filter(col("CRSDepTime") == p).cache()
        labelsP = flightsP.select(col("OriginCityName")).dropDuplicates().sort(col("OriginCityName"))
        if labelsP.count() == 0:
            streamlit.markdown('Non sono presenti voli per i dati insereriti')
            streamlit.markdown('Attenzione: Scegli un volo del 2013')
        else:
            origin = streamlit.selectbox('Aeroporto di partenza', (labelsP))
            flightsA = flightsP.filter(col("OriginCityName") == origin)
            labelsA = flightsA.select(col("DestCityName")).sort(col("DestCityName"))
            destination = streamlit.selectbox('Aeroporto di arrivo', (labelsA))
            flight = flightsA.filter(col("DestCityName") == destination).take(num=1)[0]
            coor = coordinates(flight.__getitem__("Origin"), flight.__getitem__("OriginCityName"),
                               flight.__getitem__("Dest"), flight.__getitem__("DestCityName"))

    with col2:
        if labelsP.count() != 0:
            streamlit.pydeck_chart(
                pydeck.Deck(
                    initial_view_state=pydeck.ViewState(
                        latitude=coor.iloc[0].__getitem__("OriginLatitude"),
                        longitude=coor.iloc[0].__getitem__("OriginLongitude"),
                        zoom=5,
                        pitch=50,
                        height=400,
                    ),
                    layers=[
                        pydeck.Layer(
                            "ArcLayer",
                            data=coor,
                            get_source_position=["OriginLongitude", "OriginLatitude"],
                            get_target_position=["DesLongitude", "DesLatitude"],
                            get_source_color=[200, 30, 0, 160],
                            get_target_color=[200, 30, 0, 160],
                            auto_highlight=True,
                            width_scale=0.0001,
                            get_width="outbound",
                            width_min_pixels=3,
                            width_max_pixels=30,

                            radius=200,
                            elevation_scale=4,
                            elevation_range=[0, 1000],
                            pickable=True,
                            extruded=True,
                        ),
                    ],
                )
            )

    if labelsP.count() != 0:
        streamlit.markdown("""---""")

        cd = flight.__getitem__("CarrierDelay")
        wd = flight.__getitem__("WeatherDelay")
        nd = flight.__getitem__("NASDelay")
        sd = flight.__getitem__("SecurityDelay")
        lad = flight.__getitem__("LateAircraftDelay")

        # Code for cancelled flights
        if flight.__getitem__("Cancelled") == "1.00":
            streamlit.markdown(':red[Il volo è stato cancellato]')
            streamlit.markdown("""---""")
            col3, col4, col5, col6, col7 = streamlit.columns(5)
            col3.metric(label="Orario di partenza previsto", value=flight.__getitem__("CRSDepTime").strftime("%H:%M"))
            col4.metric(label="Orario di arrivo previsto", value=flight.__getitem__("CRSArrTime").strftime("%H:%M"))
            col5.metric(label="Distanza prevista", value=str(flight.__getitem__("Distance")) + " miles")
            ritardoMedio = flightsA.agg(avg("ArrDelay")).take(num=1)[0].__getitem__("avg(ArrDelay)")
            col6.metric(label="Ritardo medio", value=str(ritardoMedio) + " minuti")
            dfMedia = df.filter(col("OriginCityName") == origin).filter(col("DestCityName") == destination)
            canc = dfMedia.filter(col("Cancelled") == "1.00").count()
            total = dfMedia.count()
            col7.metric(label="Percentuale voli cancellati", value=str(round(canc / total * 100, 2)) + " %")

        # Code for delayed flights
        elif cd is not None and wd is not None and nd is not None and sd is not None and lad is not None and (cd>0 or wd>0 or nd>0 or sd>0 or lad>0):
            col3, col4, col5 = streamlit.columns(3)
            with col3:
                streamlit.metric(label="Orario di partenza previsto", value=flight.__getitem__("CRSDepTime").strftime("%H:%M"))
                streamlit.markdown("""---""")
                if flight.__getitem__("DepDelay") == "0.00":
                    deltaCol = "off"
                else:
                    deltaCol = "inverse"
                streamlit.metric(label="Orario di partenza effettivo", value=flight.__getitem__("DepTime").strftime("%H:%M"),
                                delta=flight.__getitem__("DepDelay"), delta_color=deltaCol)
                streamlit.markdown("""---""")
                streamlit.metric(label="Tail Number", value=flight.__getitem__("Tail_Number"))
                streamlit.markdown("""---""")
                ritardoMedio = flightsA.agg(avg("ArrDelay")).take(num=1)[0].__getitem__("avg(ArrDelay)")
                streamlit.metric(label="Ritardo medio", value=str(ritardoMedio) + " minuti")

            with col4:
                streamlit.metric(label="Orario di arrivo previsto",value=flight.__getitem__("CRSArrTime").strftime("%H:%M"))
                streamlit.markdown("""---""")
                if flight.__getitem__("ArrDelay") == "0.00":
                    deltaCol = "off"
                else:
                    deltaCol = "inverse"
                streamlit.metric(label="Orario di arrivo effettivo",
                                     value=flight.__getitem__("ArrTime").strftime("%H:%M"),
                                     delta=flight.__getitem__("ArrDelay"), delta_color=deltaCol)
                streamlit.markdown("""---""")
                streamlit.metric(label="Distanza percorsa", value=str(flight.__getitem__("Distance")) + " miles")
                dfMedia = df.filter(col("OriginCityName") == origin).filter(col("DestCityName") == destination)
                canc = dfMedia.filter(col("Cancelled") == "1.00").count()
                total = dfMedia.count()
                streamlit.markdown("""---""")
                streamlit.metric(label="Percentuale voli cancellati", value=str(round(canc / total * 100, 2)) + " %")

            with col5:
                streamlit.markdown(":blue[Cause del ritardo]")
                if cd > 0:
                    streamlit.markdown("Carrier delay: " + str(cd) + " minuti")
                if wd > 0:
                    streamlit.markdown("Weather delay: " + str(wd) + " minuti")
                if nd > 0:
                    streamlit.markdown("NAS delay: " + str(nd) + " minuti")
                if sd > 0:
                    streamlit.markdown("Security delay: " + str(sd) + " minuti")
                if lad > 0:
                    streamlit.markdown("Late aircraft delay: " + str(lad) + " minuti")

                sizes = [cd, wd, nd, sd, lad]
                mylabels = ["Carrier delay", "Weather delay", "NAS delay", "Security delay", "Late aircraft delay"]
                fig, ax = plt.subplots()
                ax.pie(sizes, labels = mylabels, shadow=True, startangle=90, labeldistance=1.15,
                       wedgeprops={'linewidth': 3, 'edgecolor': 'white'})
                ax.axis('equal')
                p = plt.gcf()
                p.gca().add_artist(plt.Circle((0, 0), 0.8, color='white'))
                fig.patch.set_facecolor('#0e1117')
                ax.legend(title="Legenda")
                streamlit.pyplot(fig)

        # Code for diverted flights
        elif flight.__getitem__("Diverted") == "1.00":
            streamlit.markdown(':red[Il volo è stato dirottato]')
            streamlit.markdown("""---""")

            if flight.__getitem__("DivReachedDest") == "1.00":
                col3, col4, col5, col6 = streamlit.columns(4)
            else:
                col3, col4, col5 = streamlit.columns(3)

            col3.metric(label="Orario di partenza previsto", value=flight.__getitem__("CRSDepTime").strftime("%H:%M"))
            if flight.__getitem__("DepDelay") == "0.00":
                deltaCol = "off"
            else:
                deltaCol = "inverse"
            col4.metric(label="Orario di partenza effettivo", value=flight.__getitem__("DepTime").strftime("%H:%M"),
                        delta=flight.__getitem__("DepDelay"), delta_color=deltaCol)

            col5.metric(label="Orario di arrivo previsto", value=flight.__getitem__("CRSArrTime").strftime("%H:%M"))

            if flight.__getitem__("DivReachedDest") == "1.00":
                if flight.__getitem__("DivArrDelay") == "0.00":
                    deltaCol = "off"
                else:
                    deltaCol = "inverse"
                streamlit.metric(label="Orario di arrivo effettivo", value=flight.__getitem__("DivArrDelay").strftime("%H:%M"),
                            delta=flight.__getitem__("DivArrDelay"), delta_color=deltaCol)
            else:
                streamlit.markdown("""---""")
                streamlit.markdown(':red[Il volo non ha raggiunto la destinazione]')


            streamlit.markdown("""---""")
            streamlit.markdown("Sono riportati gli aeroporti di deviazione")

            if flight.__getitem__("Div5Airport") is not None:
                col7, col8, col9, col10, col11 = streamlit.columns(5)
            elif flight.__getitem__("Div4Airport") is not None:
                col7, col8, col8, col10 = streamlit.columns(4)
            elif flight.__getitem__("Div3Airport") is not None:
                col7, col8, col8 = streamlit.columns(3)
            elif flight.__getitem__("Div2Airport") is not None:
                col7, col8 = streamlit.columns(2)
            else:
                col7 = streamlit.columns(1)

            if flight.__getitem__("Div5Airport") is not None:
                col11.metric(label="Quinto aeroporto", value=flight.__getitem__("Div5Airport"))
            if flight.__getitem__("Div4Airport") is not None:
                col10.metric(label="Quarto aeroporto", value=flight.__getitem__("Div4Airport"))
            if flight.__getitem__("Div3Airport") is not None:
                col9.metric(label="Terzo aeroporto", value=flight.__getitem__("Div3Airport"))
            if flight.__getitem__("Div2Airport") is not None:
                col8.metric(label="Secondo aeroporto", value=flight.__getitem__("Div2Airport"))
            col7.metric(label="Primo aeroporto", value=flight.__getitem__("Div1Airport"))


        # Code for a generic flight
        else:
            col3, col4, col5, col6 = streamlit.columns(4)
            col3.metric(label="Orario di partenza previsto", value=flight.__getitem__("CRSDepTime").strftime("%H:%M"))
            if flight.__getitem__("DepDelay") == "0.00":
                deltaCol = "off"
            else:
                deltaCol = "inverse"
            col4.metric(label="Orario di partenza effettivo", value=flight.__getitem__("DepTime").strftime("%H:%M"),
                        delta=flight.__getitem__("DepDelay"), delta_color=deltaCol)

            col5.metric(label="Orario di arrivo previsto", value=flight.__getitem__("CRSArrTime").strftime("%H:%M"))
            if flight.__getitem__("ArrDelay") == "0.00":
                deltaCol = "off"
            else:
                deltaCol = "inverse"
            col6.metric(label="Orario di arrivo effettivo", value=flight.__getitem__("ArrTime").strftime("%H:%M"),
                        delta=flight.__getitem__("ArrDelay"),delta_color=deltaCol)

            streamlit.markdown("""---""")
            col7, col8, col9, col10 = streamlit.columns(4)
            col7.metric(label="Tail Number", value=flight.__getitem__("Tail_Number"))
            col8.metric(label="Distanza percorsa", value=str(flight.__getitem__("Distance")) + " miles")
            ritardoMedio = flightsA.agg(avg("ArrDelay")).take(num=1)[0].__getitem__("avg(ArrDelay)")
            col9.metric(label="Ritardo medio", value=str(ritardoMedio) + " minuti")
            dfMedia = df.filter(col("OriginCityName") == origin).filter(col("DestCityName") == destination)
            canc = dfMedia.filter(col("Cancelled") == "1.00").count()
            total = dfMedia.count()
            col10.metric(label="Percentuale voli cancellati", value=str(round(canc/total * 100, 2)) + " %")
