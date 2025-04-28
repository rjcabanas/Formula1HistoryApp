import streamlit as st
import streamlit as st
import pandas as pd

df_races = pd.read_parquet("silver\\races.parquet")
df_race_results = pd.read_parquet("silver\\results.parquet")
df_drivers = pd.read_parquet("silver\\driversDim.parquet")
df_constructors = pd.read_parquet("silver\\constructors.parquet")
df_lap_data = pd.read_parquet("silver\\lapsFactTable.parquet")
status = pd.read_csv("bronze\\status.csv")
drivers_standings = pd.read_parquet("silver\\driver_standings.parquet")
constructors_standings = pd.read_parquet("silver\\constructor_standings.parquet")
seasons = pd.read_csv("bronze\\seasons.csv")

def get_driver_champs(raceId_list:list) -> list:
    driver_champ_list = []
    for last_race in raceId_list:
        last_race_standings = drivers_standings[drivers_standings["raceId"] == last_race]
        max_points = last_race_standings["points"].max()
        driver_champ_id = last_race_standings[last_race_standings["points"] == max_points]["driverId"].values[0]
        driver_champ_name = df_drivers[df_drivers["driverId"] == driver_champ_id]["fullName"].values[0]
        driver_champ_list.append(driver_champ_name)

    return driver_champ_list    

def get_constructor_champs(raceId_list:list) -> list:
    
    const_champ_list = []
    const_standings_merged = pd.merge(constructors_standings, df_constructors[["constructorId", "constructorName"]], on="constructorId", how="left")
    
    for raceId in raceId_list:
        last_race_standings = const_standings_merged[const_standings_merged["raceId"] == raceId]
        max_const_points = last_race_standings["points"].max()
        const_champ = last_race_standings[last_race_standings["points"] == max_const_points]
        try:
            const_champ_name = const_champ["constructorName"].values[0]
            const_champ_list.append(const_champ_name)
        except(IndexError):
            const_champ_name = "Not Awarded Until 1958"
            const_champ_list.append(const_champ_name)
    
    return const_champ_list

def get_num_races(years:list) -> list:
    num_of_races_list = []

    for year in years:
        races = df_races[df_races["year"] == year]
        num_of_races = len(races)
        num_of_races_list.append(num_of_races)

    return num_of_races_list

def season_driver_standings(year:int):
    season_races = df_races[df_races["year"] == year]
    last_round = season_races["round"].max()
    last_round_raceId = season_races[season_races["round"] == last_round]["raceId"].values[0]
    season_driver_standing = drivers_standings[drivers_standings["raceId"] == last_round_raceId]

    drivers_name_merged = pd.merge(season_driver_standing, df_drivers[["driverId", "fullName", "nationality"]], on="driverId", how="left")
    season_driver_standing = drivers_name_merged.drop(["driverStandingsId", "raceId", "driverId", "positionText"], axis=1)
    season_driver_standing = season_driver_standing.rename(columns={
                                                            "points":"Points",
                                                            "position":"Position",
                                                            "wins":"Wins",
                                                            "fullName":"Driver Name",
                                                            "nationality":"Nationality"
                                                })
    
    season_driver_standing = season_driver_standing[["Position", "Driver Name", "Nationality", "Points", "Wins"]]
    season_driver_standing = season_driver_standing.sort_values(by="Position")

    return season_driver_standing

def season_constructor_standings(year:int):
    season_constructor_standings_cols = ["Position", "Constructor", "Wins", "Podiums", "Points"]
    season_constructor_standings = pd.DataFrame(columns=season_constructor_standings_cols)

    season_races = df_races[df_races["year"] == year]
    last_round = season_races["round"].max()
    last_round_raceId = season_races[season_races["round"] == last_round]["raceId"].values[0]   
    constructors_standings_merged = constructors_standings.merge(df_constructors, left_on="constructorId", right_on="constructorId", how="left")
    
    season_constructor_standings["Position"] = constructors_standings_merged[constructors_standings_merged["raceId"] == last_round_raceId]["position"]
    season_constructor_standings["Constructor"] = constructors_standings_merged[constructors_standings_merged["raceId"] == last_round_raceId]["constructorName"]
    season_constructor_standings["Points"] = constructors_standings_merged[constructors_standings_merged["raceId"] == last_round_raceId]["points"]
    season_constructor_standings["Wins"] = constructors_standings_merged[constructors_standings_merged["raceId"] == last_round_raceId]["wins"]

    season_races_results_merged = pd.merge(season_races, df_race_results, on="raceId", how="left")
    season_cons_list = constructors_standings_merged[constructors_standings_merged["raceId"] == last_round_raceId]["constructorId"].tolist()
    podiums_column = []

    for constructorId in season_cons_list:
        season_results = season_races_results_merged[season_races_results_merged["constructorId"] == constructorId]
        podiums_counter = 0
        for position in season_results["positionOrder"]:
            if position == 1 or position == 2 or position == 3:
                podiums_counter += 1
            else:
                continue 
        podiums_column.append(podiums_counter)
    
    season_constructor_standings["Podiums"] = podiums_column

    return season_constructor_standings.sort_values(by="Position")

def season_schedule(year:int):
    season_schedule_cols = ["Round", "Race Name", "Date"]
    season_schedule = pd.DataFrame(columns=season_schedule_cols)

    season_races = df_races[df_races["year"] == year]
    season_schedule["Round"] = season_races["round"]
    season_schedule["Race Name"] = season_races["name"]
    season_schedule["Date"] = season_races["date"]
    races_results_merged = pd.merge(season_races, df_race_results[["raceId", "driverId", "constructorId", "positionOrder", "grid"]], on="raceId", how="left")
    races_results_merged = pd.merge(races_results_merged, df_drivers[["driverId", "fullName"]], on="driverId", how="left")

    winners = races_results_merged[races_results_merged["positionOrder"] == 1][["name", "fullName"]]
    second_places = races_results_merged[races_results_merged["positionOrder"] == 2][["name", "fullName"]]
    third_places = races_results_merged[races_results_merged["positionOrder"] == 3][["name", "fullName"]]
    pole_positions = races_results_merged[races_results_merged["grid"] == 1][["name", "fullName"]]

    season_schedule = season_schedule.merge(winners, left_on="Race Name", right_on="name", how="left")
    season_schedule = season_schedule.rename(columns={"fullName": "Winner"}).drop(columns="name")
    season_schedule = season_schedule.merge(second_places, left_on="Race Name", right_on="name", how="left")
    season_schedule = season_schedule.rename(columns={"fullName": "2nd"}).drop(columns="name")
    season_schedule = season_schedule.merge(third_places, left_on="Race Name", right_on="name", how="left")
    season_schedule = season_schedule.rename(columns={"fullName": "3rd"}).drop(columns="name")
    season_schedule = season_schedule.merge(pole_positions, left_on="Race Name", right_on="name", how="left")
    season_schedule = season_schedule.rename(columns={"fullName": "Pole Position"}).drop(columns="name")

    season_schedule = season_schedule.sort_values(by="Round", ascending=True)

    return season_schedule

st.set_page_config(
    page_title="Seasons",
    page_icon="🏁",
    layout="wide"
)

st.title("Seasons 🏁")
st.sidebar.success("Select a page above.")

with st.container():
    st.header("Season Selection")

    st.text("You can use this page to get an overall view of a particular Formula 1 Season between 1958 to 2024.")
    year_options = list(range(1958, 2025,))
    year_selected = st.select_slider("Year:", year_options)

col1, col2 = st.columns([1,2])

with col1:
    st.header("Formula 1 Seasons History")
    
    seasons_cols = ["Season", "Driver Champion", "Constructor Champion", "No. of Races"]
    seasons_info = pd.DataFrame(columns=seasons_cols)
    seasons_info["Season"] = seasons["year"].sort_values(ascending=False)
    years = seasons_info["Season"].tolist()
    rounds = []
    for year in years:
        last_round = df_races[df_races["year"] == year].sort_values("round", ascending=False).iloc[0]["raceId"]
        rounds.append(last_round)

    seasons_info["Driver Champion"] = get_driver_champs(rounds)
    seasons_info["Constructor Champion"] = get_constructor_champs(rounds)
    seasons_info["No. of Races"] = get_num_races(seasons_info["Season"].tolist())
    
    st.dataframe(seasons_info, height=600)
    

with col2:
    with st.container():
        st.header(f"{year_selected} Formula 1 Season Summary 🏅")
        st.write(f"Driver Champion: {season_driver_standings(year_selected)[season_driver_standings(year_selected)["Position"] == 1]["Driver Name"].values[0]}")
        st.write(f"Constructor Champion: {season_constructor_standings(year_selected)[season_constructor_standings(year_selected)["Position"] == 1]["Constructor"].values[0]}")
        st.write(f"Most Wins by Driver: {season_driver_standings(year_selected)[season_driver_standings(year_selected)["Wins"] == season_driver_standings(year_selected)["Wins"].max()]["Driver Name"].values[0]}")
        st.write(f"Most Wins by Constructor: {season_constructor_standings(year_selected)[season_constructor_standings(year_selected)["Wins"] == season_constructor_standings(year_selected)["Wins"].max()]["Constructor"].values[0]}")
       
        st.write("")

    with st.container():
        championship_option = st.selectbox("Select Standings: ", ("Drivers", "Constructors"))
        if championship_option == "Drivers":
            st.header(f"{year_selected} Driver's Championship 🏆")
            season_driver_standing = season_driver_standings(year_selected)
            st.write(season_driver_standing)
        else:
            st.header(f"{year_selected} Constructor's Championship 🏆")
            cons_standings = season_constructor_standings(year_selected)
            st.write(cons_standings)

    with st.container():
        st.header(f"{year_selected} Formula 1 Season Schedule ⏲️")
        schedule = season_schedule(year_selected)
        st.write(schedule)