import psycopg2 as pg
import pandas as pd
import numpy as np


class Recommender:

    __data: pd.DataFrame = None
    __cuisine: pd.DataFrame = None
    _corr_threshold = 0.3
    cache_recomm: dict = {}

    def __init__(self) -> None:
        # conn = psycopg2.connect(host="localhost",dbname="postgres",user="postgres",password="admin",port=5432)
        conn = pg.connect(
            host="localhost", dbname="feast_hub_sub", user="iktis", password="", port=5434)
        self.__load_cusines(conn)
        self.__load_user_ratings(conn)
        self.__cf_algorithm()

    def __load_user_ratings(self, conn: pg.connection) -> None:
        cur = conn.cursor()
        cur.execute('SELECT * FROM users_ratings')
        user_ratings = cur.fetchall()
        self.__data = pd.DataFrame(user_ratings)
        # Using DataFrame.iloc[] to drop last n columns
        # Drop first column of dataframe using drop()
        self.__data.drop(columns=self.__data.columns[0], axis=1,  inplace=True)
        self.__data = self.__data.iloc[:, :-2]
        # print(__data)
        self.__data.columns = ['userID', 'placeID',
                               'food_rating', 'service_rating']

        # add overall_rating column to this data
        self.__data['overall_rating'] = self.__data.apply(lambda row: round(
            (row['rating'] + row['food_rating'] + row['service_rating'])/3, 2), axis=1)

    def __load_cusines(self, conn: pg.connection) -> None:
        cur = conn.cursor()

        cur.execute("SELECT * FROM cusines")
        data_cus = cur.fetchall()
        self.__cuisine = pd.DataFrame(data_cus)
        # Using DataFrame.iloc[] to drop last n columns

        self.__cuisine = self.__cuisine.iloc[:, :-2]
        self.__cuisine.columns = ['id', 'placeID', 'Rcuisine']

    def __cf_algorithm(self):
        data_matrix = pd.pivot_table(self.__data, index=['userID'], columns=[
                                     'placeID'], values=['overall_rating'])
        # normalize matrix
        data_matrix = data_matrix.subtract(
            data_matrix.mean(axis=1), axis='rows')
        corr_matrix = data_matrix.T.corr(method='pearson')

        # ----------------------------------------------------------------
        # select unique users and cache their recommendation profile
        users: np.array = self.__data['userID'].unique()

        for user in users:

            # user: str = 'U1001'
            k_similar_users = corr_matrix[corr_matrix[user]
                                          > self._corr_threshold][user]

            # user that have already visited the restaurants
            user_visits = data_matrix[data_matrix.index ==
                                      user].dropna(axis=1, how='all')

            # Restaurant that similar user visited. Remove restaurants that none of the similar user have visited
            similar_user_score = data_matrix[data_matrix.index.isin(
                k_similar_users.index)].dropna(axis=1, how='all')

            # Remove visited restaurant from the list
            similar_user_score.drop(user_visits.columns,
                                    axis=1, inplace=True, errors='ignore')
            self.cache_recomm[user] = self.__predict(
                similar_user_score, k_similar_users).to_json()

    def __predict(self, similar_user_score: pd.DataFrame, k_similar_users: pd.Series) -> pd.DataFrame:
        # A dictionary to store item scores
        item_score = {}

        # Loop through items
        for i in similar_user_score.columns:
            # Get the ratings for restaurant i
            restaurant_rating = similar_user_score[i]
            # Create a variable to store the score
            total = 0
            # Create a variable to store the number of scores
            count = 0
            # Loop through similar users
            for u in k_similar_users.index:
                # If the restaurant has rating
                if pd.isna(restaurant_rating[u]) == False:
                    # Score is the sum of user similarity score multiply by the restaurant rating
                    score = k_similar_users[u] * restaurant_rating[u]
                    # Add the score to the total score for the restaurant so far
                    total += score
                    # Add 1 to the count
                    count += 1
            # Get the average score for the item
            item_score[i] = total / count

        # Convert dictionary to pandas dataframe
        item_score = pd.DataFrame(item_score.items(), columns=[
            'place', 'place_score'])

        # Sort the restaurant by score
        item_score.sort_values(
            by='place_score', ascending=False, inplace=True)
        item_score['place'] = [i[1] for i in item_score['place']]
        # Select top m restaurant
        # m = number_of_top_restaurants
        item_score.reset_index(inplace=True)
        item_score.drop(columns='index', inplace=True)

        return item_score

    def recommend_user_api(self, id):
        if (self.__data['userID'].isin(id)):
            return self.cache_recomm[id]
        return {"error": "No user found"}

    def refresh(self) -> None:
        pass
