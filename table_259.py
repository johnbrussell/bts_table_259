import pandas as pd


df = pd.read_csv("./bts_table_259_2017.csv")
df = df[df['SEATS'] > 0]
df_airline_route = df.groupby(['CARRIER', 'ORIGIN', 'DEST'])[
    'DEPARTURES_PERFORMED', 'SEATS', 'PASSENGERS'].sum().reset_index()
df_airline_route = df_airline_route[df_airline_route['CARRIER'].apply(lambda x: len(x) == 2)]
df_airline_route = df_airline_route[df_airline_route['DEPARTURES_PERFORMED'] >= 52]

df_airline_route['Seats_Per_Flight'] = df_airline_route['SEATS'] / df_airline_route['DEPARTURES_PERFORMED']
df_airline_route['Passengers_Per_Flight'] = df_airline_route['PASSENGERS'] / df_airline_route['DEPARTURES_PERFORMED']
df_airline_route['Load_Factor'] = df_airline_route['PASSENGERS'] / df_airline_route['SEATS'] * 100
df_airline_route['Avail_Seats_Per_Flight'] = df_airline_route['Seats_Per_Flight'] - \
    df_airline_route['Passengers_Per_Flight']


def route_analysis(origins, destinations):
    df_local = df_airline_route[(df_airline_route['ORIGIN'].isin(origins)) &
                                (df_airline_route['DEST'].isin(destinations))].copy()

    for col in df_local.columns:
        if col in ['DEST', 'ORIGIN']:
            continue
        df_local.rename(columns={col: "{}_flight_1".format(col)}, inplace=True)

    df_local = pd.concat([df_local, route_analysis_connecting(origins, destinations)])
    df_local = df_local[['CARRIER_flight_1',
                         'CARRIER_flight_2',
                         'ORIGIN',
                         'CONNECTION',
                         'DEST',
                         'SEATS_flight_1',
                         'PASSENGERS_flight_1',
                         'Seats_Per_Flight_flight_1',
                         'Passengers_Per_Flight_flight_1',
                         'Load_Factor_flight_1',
                         'Avail_Seats_Per_Flight_flight_1',
                         'SEATS_flight_2',
                         'PASSENGERS_flight_2',
                         'Seats_Per_Flight_flight_2',
                         'Passengers_Per_Flight_flight_2',
                         'Load_Factor_flight_2',
                         'Avail_Seats_Per_Flight_flight_2']]

    origins_string = "_".join(origins)
    destinations_string = "_".join(destinations)
    df_local.to_csv('./load_factor_from_{}_to_{}.csv'.format(origins_string, destinations_string),
                    index=False)


FREEBIRD_AIRLINES = ['AA', 'AS', 'B6', 'DL', 'F9', 'G4', 'HA', 'NK', 'SY', 'UA', 'VX', 'WN']


def route_analysis_connecting(origins, destinations):
    # Current implementation supports maximum one connection.
    df_origins = df_airline_route[df_airline_route['ORIGIN'].isin(origins)].copy()
    df_destinations = df_airline_route[df_airline_route['DEST'].isin(destinations)].copy()

    df_origins = df_origins[df_origins['DEST'].isin(df_destinations['ORIGIN'])]
    df_destinations = df_destinations[df_destinations['ORIGIN'].isin(df_origins['DEST'])]

    df_origins.rename(columns={'DEST': 'CONNECTION'}, inplace=True)
    for col in df_origins.columns:
        if col in ['CONNECTION', 'ORIGIN']:
            continue
        df_origins.rename(columns={col: "{}_flight_1".format(col)}, inplace=True)

    df_destinations.rename(columns={'ORIGIN': "CONNECTION"}, inplace=True)
    df_destinations = df_destinations[['DEST', 'Seats_Per_Flight', 'Load_Factor', 'Avail_Seats_Per_Flight',
                                       'CONNECTION', 'CARRIER', 'SEATS', 'PASSENGERS', 'Passengers_Per_Flight']]
    for col in df_destinations.columns:
        if col in ['DEST', 'CONNECTION']:
            continue
        df_destinations.rename(columns={col: "{}_flight_2".format(col)}, inplace=True)

    return df_origins.merge(df_destinations, how='outer', on=['CONNECTION'])


route_analysis(['LGA', 'JFK', 'EWR'], ['ORD', 'MDW'])
