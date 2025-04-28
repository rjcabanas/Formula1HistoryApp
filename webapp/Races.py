import streamlit as st
import pandas as pd
import altair as alt

df_races = pd.read_parquet(".\\silver\\races.parquet")
df_race_results = pd.read_parquet(".\\silver\\results.parquet")
df_drivers = pd.read_parquet(".\\silver\\driversDim.parquet")
df_lap_data = pd.read_parquet(".\\silver\\lapsFactTable.parquet")
status = pd.read_csv(".\\bronze\\status.csv")
drivers_standings = pd.read_parquet(".\\silver\\driver_standings.parquet")
grand_prix_flags = {
                    "Malaysian Grand Prix": "https://flagcdn.com/w40/my.png",
                    "German Grand Prix": "https://flagcdn.com/w40/de.png",
                    "Swiss Grand Prix": "https://flagcdn.com/w40/ch.png",
                    "Qatar Grand Prix": "https://flagcdn.com/w40/qa.png",
                    "Hungarian Grand Prix": "https://flagcdn.com/w40/hu.png",
                    "Australian Grand Prix": "https://flagcdn.com/w40/au.png",
                    "Luxembourg Grand Prix": "https://flagcdn.com/w40/lu.png",
                    "Canadian Grand Prix": "https://flagcdn.com/w40/ca.png",
                    "Pacific Grand Prix": "https://flagcdn.com/w40/jp.png",  # Japan
                    "Sakhir Grand Prix": "https://flagcdn.com/w40/bh.png",
                    "Mexican Grand Prix": "https://flagcdn.com/w40/mx.png",
                    "European Grand Prix": "https://flagcdn.com/w40/eu.png",  # Generic EU
                    "Las Vegas Grand Prix": "https://flagcdn.com/w40/us.png",
                    "British Grand Prix": "https://flagcdn.com/w40/gb.png",
                    "Azerbaijan Grand Prix": "https://flagcdn.com/w40/az.png",
                    "Swedish Grand Prix": "https://flagcdn.com/w40/se.png",
                    "Mexico City Grand Prix": "https://flagcdn.com/w40/mx.png",
                    "Emilia Romagna Grand Prix": "https://flagcdn.com/w40/it.png",
                    "Styrian Grand Prix": "https://flagcdn.com/w40/at.png",
                    "Tuscan Grand Prix": "https://flagcdn.com/w40/it.png",
                    "United States Grand Prix": "https://flagcdn.com/w40/us.png",
                    "Russian Grand Prix": "https://flagcdn.com/w40/ru.png",
                    "Belgian Grand Prix": "https://flagcdn.com/w40/be.png",
                    "San Marino Grand Prix": "https://flagcdn.com/w40/sm.png",
                    "French Grand Prix": "https://flagcdn.com/w40/fr.png",
                    "Korean Grand Prix": "https://flagcdn.com/w40/kr.png",
                    "Saudi Arabian Grand Prix": "https://flagcdn.com/w40/sa.png",
                    "São Paulo Grand Prix": "https://flagcdn.com/w40/br.png",
                    "Brazilian Grand Prix": "https://flagcdn.com/w40/br.png",
                    "Dallas Grand Prix": "https://flagcdn.com/w40/us.png",
                    "Austrian Grand Prix": "https://flagcdn.com/w40/at.png",
                    "Caesars Palace Grand Prix": "https://flagcdn.com/w40/us.png",
                    "Argentine Grand Prix": "https://flagcdn.com/w40/ar.png",
                    "70th Anniversary Grand Prix": "https://flagcdn.com/w40/gb.png",
                    "Miami Grand Prix": "https://flagcdn.com/w40/us.png",
                    "Japanese Grand Prix": "https://flagcdn.com/w40/jp.png",
                    "Bahrain Grand Prix": "https://flagcdn.com/w40/bh.png",
                    "Eifel Grand Prix": "https://flagcdn.com/w40/de.png",
                    "Pescara Grand Prix": "https://flagcdn.com/w40/it.png",
                    "Italian Grand Prix": "https://flagcdn.com/w40/it.png",
                    "Chinese Grand Prix": "https://flagcdn.com/w40/cn.png",
                    "Detroit Grand Prix": "https://flagcdn.com/w40/us.png",
                    "Turkish Grand Prix": "https://flagcdn.com/w40/tr.png",
                    "Portuguese Grand Prix": "https://flagcdn.com/w40/pt.png",
                    "Indian Grand Prix": "https://flagcdn.com/w40/in.png",
                    "Indianapolis 500": "https://flagcdn.com/w40/us.png",
                    "Dutch Grand Prix": "https://flagcdn.com/w40/nl.png",
                    "Monaco Grand Prix": "https://flagcdn.com/w40/mc.png",
                    "South African Grand Prix": "https://flagcdn.com/w40/za.png",
                    "United States Grand Prix West": "https://flagcdn.com/w40/us.png",
                    "Moroccan Grand Prix": "https://flagcdn.com/w40/ma.png",
                    "Singapore Grand Prix": "https://flagcdn.com/w40/sg.png",
                    "Abu Dhabi Grand Prix": "https://flagcdn.com/w40/ae.png",
                    "Spanish Grand Prix": "https://flagcdn.com/w40/es.png"
                }
driver_nationality_flags = {
                            'Thai': 'https://flagcdn.com/th.svg',
                            'Brazilian': 'https://flagcdn.com/br.svg',
                            'Czech': 'https://flagcdn.com/cz.svg',
                            'Uruguayan': 'https://flagcdn.com/uy.svg',
                            'Japanese': 'https://flagcdn.com/jp.svg',
                            'Venezuelan': 'https://flagcdn.com/ve.svg',
                            'Colombian': 'https://flagcdn.com/co.svg',
                            'Italian': 'https://flagcdn.com/it.svg',
                            'South African': 'https://flagcdn.com/za.svg',
                            'American-Italian': 'https://flagcdn.com/us.svg',
                            'British': 'https://flagcdn.com/gb.svg',
                            'Polish': 'https://flagcdn.com/pl.svg',
                            'New Zealander': 'https://flagcdn.com/nz.svg',
                            'Irish': 'https://flagcdn.com/ie.svg',
                            'Portuguese': 'https://flagcdn.com/pt.svg',
                            'Chilean': 'https://flagcdn.com/cl.svg',
                            'Australian': 'https://flagcdn.com/au.svg',
                            'Austrian': 'https://flagcdn.com/at.svg',
                            'Malaysian': 'https://flagcdn.com/my.svg',
                            'Argentinian': 'https://flagcdn.com/ar.svg',
                            'Argentine': 'https://flagcdn.com/ar.svg',
                            'Danish': 'https://flagcdn.com/dk.svg',
                            'Liechtensteiner': 'https://flagcdn.com/li.svg',
                            'Mexican': 'https://flagcdn.com/mx.svg',
                            'Chinese': 'https://flagcdn.com/cn.svg',
                            'French': 'https://flagcdn.com/fr.svg',
                            'American': 'https://flagcdn.com/us.svg',
                            'German': 'https://flagcdn.com/de.svg',
                            'Indonesian': 'https://flagcdn.com/id.svg',
                            'Swiss': 'https://flagcdn.com/ch.svg',
                            'Canadian': 'https://flagcdn.com/ca.svg',
                            'Monegasque': 'https://flagcdn.com/mc.svg',
                            'East German': 'https://flagcdn.com/de.svg',
                            'Finnish': 'https://flagcdn.com/fi.svg',
                            'Swedish': 'https://flagcdn.com/se.svg',
                            'Russian': 'https://flagcdn.com/ru.svg',
                            'Indian': 'https://flagcdn.com/in.svg',
                            'Hungarian': 'https://flagcdn.com/hu.svg',
                            'Dutch': 'https://flagcdn.com/nl.svg',
                            'Rhodesian': 'https://flagcdn.com/zw.svg',
                            'Spanish': 'https://flagcdn.com/es.svg',
                            'Argentine-Italian': 'https://flagcdn.com/ar.svg',
                            'Belgian': 'https://flagcdn.com/be.svg'
                        }


def number_of_rounds(year:int) -> list:
    filtered_df_races = df_races[df_races["year"] == year]

    return len(filtered_df_races) + 1

def get_raceID(year:int, round:int) -> list:
    raceID = df_races[(df_races["year"] == year) & (df_races["round"] == round)]["raceId"].values[0]

    return raceID

def get_drivers(raceID:int) -> list:
    drivers_id = df_race_results[df_race_results["raceId"] == raceID]["driverId"].tolist()
    drivers_name = []
    for id in drivers_id:
        drivers_name.append(df_drivers[df_drivers["driverId"] == id]["fullName"].values[0])

    return drivers_name

def get_race_laps(raceID:int) -> int:
    number_of_laps = df_race_results[df_race_results["raceId"] == raceID]["laps"].values[0]

    return number_of_laps

def get_race_data(raceID:int):
    all_race_data = df_lap_data[df_lap_data["raceId"] == raceID]

    return all_race_data

def race_results_view(raceID:int):
    race_results = df_race_results[df_race_results["raceId"] == raceID]
    race_results["Position Gained"] = race_results["grid"] - race_results["positionOrder"]
    drivers_name_merged = pd.merge(race_results, df_drivers[["driverId", "fullName", "nationality"]], on="driverId", how="left")
    race_status_merged = pd.merge(drivers_name_merged, status[["statusId", "status"]], on="statusId", how="left")
    race_status_merged["time"] = race_status_merged["status"].where(race_status_merged["time"] == "\\N", race_status_merged["time"])

    race_results = race_status_merged.drop(["resultId", "raceId", "driverId", "constructorId", "positionText", "position", "milliseconds", "fastestLap", "rank", "fastestLapSpeed", "statusId", "status"], axis=1)
    race_results = race_results[["positionOrder", "fullName", "nationality", "time", "points","grid", "Position Gained", "laps", "fastestLapTime"]]
    race_results = race_results.rename(columns={
                                                "positionOrder":"Final Position", 
                                                "fullName":"Driver Name",
                                                "nationality": "Nationality",
                                                "time":"Finishing Time", 
                                                "points":"Points Gained",
                                                "grid":"Starting Position", 
                                                "laps":"Laps Completed", 
                                                "fastestLapTime":"Fastest Lap Time"
                                          })

    race_results = race_results.sort_values(by="Final Position")
    return race_results

def current_drivers_standings(raceID:int):
    current_standings = drivers_standings[drivers_standings["raceId"] == raceID]
    drivers_name_merged = pd.merge(current_standings, df_drivers[["driverId", "fullName", "nationality"]], on="driverId", how="left")
    current_standings = drivers_name_merged.drop(["driverStandingsId", "raceId", "driverId", "positionText"], axis=1)
    current_standings = current_standings.rename(columns={
                                                            "points":"Points",
                                                            "position":"Position",
                                                            "wins":"Wins",
                                                            "fullName":"Driver Name",
                                                            "nationality":"Nationality"
                                                })
    
    current_standings = current_standings[["Position", "Driver Name", "Nationality", "Points", "Wins"]]
    current_standings = current_standings.sort_values(by="Position")

    return current_standings

# StreamLit WebApp Code

st.set_page_config(
    page_title="Race Summary",
    page_icon="🏎️",
    layout="wide"
)

st.title("Races 🏎️")
st.sidebar.success("Select a page above.")

st.text("You can use this page to select any race between 1996 to 2024 and get a snapshot of the race.")

year_options = list(range(1996, 2025,))
year = st.select_slider("Year:", year_options)
rounds = st.select_slider("Round:", list(range(1, number_of_rounds(year))))
raceID = get_raceID(year, rounds)

quad1, quad2 = st.columns(2)
with quad1:
    st.header(f"{df_races[df_races["raceId"] == raceID]["name"].values[0]}")
    st.image(grand_prix_flags[df_races[df_races["raceId"] == raceID]["name"].values[0]])

    race_data = get_race_data(raceID)

    race_results = race_results_view(raceID)
    race_winner = race_results[race_results["Final Position"] == 1]["Driver Name"].values[0]
    race_2nd = race_results[race_results["Final Position"] == 2]["Driver Name"].values[0]
    race_3rd = race_results[race_results["Final Position"] == 3]["Driver Name"].values[0]

    st.subheader(f"Podium")

    st.text(f"1st - {race_winner} 🥇")
    st.text(f"2nd - {race_2nd} 🥈")
    st.text(f"3rd - {race_3rd} 🥉")

with quad2:
    st.subheader("Race Summary")
    st.write(race_results)

quad3, quad4 = st.columns(2)

with quad3:
    st.subheader(f"{year} Formula 1 Season | Round {rounds} | Current Standings")

    current_standings = current_drivers_standings(raceID)
    st.write(current_standings)

with quad4:
    st.subheader("Race Development")
    race_chart = alt.Chart(race_data).mark_line(point=True).encode(
                x=alt.X("lap",
                        title='Lap Number'),
                y=alt.Y("position", 
                        sort='descending',
                        scale=alt.Scale(domain=[1, race_results["Final Position"].max() + 1]),
                        axis=alt.Axis(tickMinStep=1, format='d'),
                        title='Race Position'),
                        color=alt.Color('fullName',
                                        legend=alt.Legend(title="Driver")),          
                                        tooltip=["lap", "position", "fullName"]
                                                )

    st.altair_chart(race_chart, use_container_width=True)


