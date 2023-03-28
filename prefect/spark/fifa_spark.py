import argparse

from pyspark.sql import SparkSession


parser = argparse.ArgumentParser()

parser.add_argument('--project_id', required=True)

args = parser.parse_args()

PROJECT_ID = args.project_id


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
spark.conf.set('temporaryGcsBucket', f'dataproc_{PROJECT_ID}_temp')

# Load data from BigQuery.
fifa_teams = spark.read.format('bigquery') \
  .option('table', f'{PROJECT_ID}.fifa_data_all.fifa_teams') \
  .load()
fifa_teams.createOrReplaceTempView('fifa_teams')

fifa_players = spark.read.format('bigquery') \
  .option('table', f'{PROJECT_ID}.fifa_data_all.fifa_players') \
  .load()
fifa_players.createOrReplaceTempView('fifa_players')

last_version = spark.sql(
    """
    SELECT fifa_version, MAX(fifa_update) as latest_version 
    FROM fifa_teams
    GROUP BY fifa_version
    """)
last_version.createOrReplaceTempView('fifa_versions')

last_teams = spark.sql(
    """
    SELECT fifa_teams.team_id, fifa_teams.fifa_version, fifa_teams.team_name, fifa_teams.league_id, 
    fifa_teams.league_name, fifa_teams.league_level, fifa_teams.nationality_id, fifa_teams.nationality_name, 
    fifa_teams.overall, fifa_teams.attack, fifa_teams.midfield, 
    fifa_teams.international_prestige, fifa_teams.domestic_prestige
    FROM fifa_teams 
    JOIN fifa_versions 
    ON fifa_teams.fifa_version = fifa_versions.fifa_version 
    AND fifa_teams.fifa_update = fifa_versions.latest_version
    """)
last_teams.createOrReplaceTempView('latest_teams')

last_players = spark.sql(
    """
    SELECT fifa_players.player_id, fifa_players.fifa_version, fifa_players.short_name, fifa_players.overall, 
    fifa_players.potential, fifa_players.value_eur, fifa_players.wage_eur, fifa_players.age, fifa_players.dob, 
    fifa_players.height_cm, fifa_players.weight_kg, fifa_players.club_team_id, 
    fifa_players.club_position, fifa_players.nationality_id, fifa_players.nationality_name, 
    fifa_players.preferred_foot, fifa_players.international_reputation
    FROM fifa_players 
    JOIN fifa_versions 
    ON fifa_players.fifa_version = fifa_versions.fifa_version 
    AND fifa_players.fifa_update = fifa_versions.latest_version
    """)
last_players.createOrReplaceTempView('latest_players')

# Temporary table just to pick sort of randomly a name for a given team_id
# The same team can have different names across fifa versions
# For our visualizations, we need to just have one, so we pick one at random
distinct_teams = spark.sql(
    """
    SELECT DISTINCT(team_id) AS team_id, MAX(team_name) AS team_name 
    FROM latest_teams 
    GROUP BY team_id;
    """)
distinct_teams.createOrReplaceTempView('distinct_teams')

team_view = spark.sql(
    """
    SELECT latest_teams.fifa_version, distinct_teams.team_name, 
    CONCAT(latest_teams.league_name, ' - ', latest_teams.nationality_name) as league_name, 
    SUM(value_eur) AS total_value, 
    SUM(wage_eur) AS total_wages, 
    MAX(latest_players.overall) AS best_player, 
    MAX(latest_players.overall) AS best_potential_player,
    CAST(AVG(latest_players.height_cm) as INTEGER) AS avg_height, 
    CAST(AVG(latest_players.weight_kg) as INTEGER) AS avg_weight, 
    CAST(AVG(latest_players.age) as INTEGER) AS avg_age
    FROM latest_teams 
    JOIN latest_players 
    ON latest_teams.team_id = latest_players.club_team_id AND latest_teams.fifa_version = latest_players.fifa_version 
    JOIN distinct_teams 
    ON latest_teams.team_id = distinct_teams.team_id
    GROUP BY latest_teams.fifa_version, distinct_teams.team_name, 
    latest_teams.league_name, latest_teams.nationality_name
    """)
team_view.createOrReplaceTempView('team_view')

# Saving the data to BigQuery
team_view.write.format('bigquery') \
  .option('table', f'{PROJECT_ID}.fifa_data_all.team_view') \
  .save()
