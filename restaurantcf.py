import psycopg2;
import pandas as pd;
import sys;
#import numpy as np;
conn = None
cur1 = None
cur2 = None
try:
    # conn = psycopg2.connect(host="localhost",dbname="postgres",user="postgres",password="admin",port=5432)
    conn = psycopg2.connect(host="localhost",dbname="feast_hub_sub",user="iktis",password="",port=5434)
    cur1 = conn.cursor()
    cur2 = conn.cursor()
    cur1.execute('SELECT * FROM users_ratings')
    revisited_restaurants = cur1.fetchall()
    data = pd.DataFrame(revisited_restaurants)
    # Using DataFrame.iloc[] to drop last n columns
    # Drop first column of dataframe using drop()
    data.drop(columns=data.columns[0], axis=1,  inplace=True)
    data = data.iloc[:, :-2]
    # print(data)
    data.columns=['userID','placeID','overall_rating','food_rating','service_rating']
    
    cur2.execute("SELECT * FROM cusines")
    cuisine = cur2.fetchall()
    df = pd.DataFrame(cuisine)
    # Using DataFrame.iloc[] to drop last n columns
    
    df = df.iloc[:, :-2]
    df.columns=['id','placeID','Rcuisine']
    # print(df)
    # print(data)

    def matrix():
        matrix_data=data.pivot_table(index='userID', columns='placeID' , values='overall_rating')
        return matrix_data

    def matrixNormalization():
        matrix_list=matrix()
        matrix_norm=matrix_list.subtract(matrix_list.mean(axis=1),axis='rows')
        return matrix_norm

    # User Similarity matrix using Pearson's correlation
    def pearsonCorrelation():
        matrix_norm = matrixNormalization()
        user_similarity=matrix_norm.T.corr()
        return user_similarity

    def recommendRestaurants(picked_userID, number_of_similar_user,number_of_top_restaurants):
        pr=pearsonCorrelation()
        picked_userID= picked_userID
        #Remove picked user Id from the lsit
        pr.drop(index=picked_userID,inplace=True)

        # Number of similar users
        n= number_of_similar_user
        #User similarity threshold
        user_similarity_threshold=0.3
        #Get top n similar users
        similar_users = pr[pr[picked_userID]>user_similarity_threshold][picked_userID].sort_values(ascending=False)[:n]

        #Restaurant visited by the target user
        matrix_normalization= matrixNormalization()
        picked_userID_visited = matrix_normalization[ matrix_normalization.index == picked_userID].dropna(axis=1, how='all')

        #Restaurant that similar user visited. Remove restaurants that none of the similar user have visited   
        similar_user_visits = matrix_normalization[matrix_normalization.index.isin(similar_users.index)].dropna(axis=1, how='all')

        # Remove visited restaurant from the list
        similar_user_visits.drop(picked_userID_visited.columns,axis=1, inplace=True, errors='ignore')

        # A dictionary to store item scores
        item_score = {}

        # Loop through items
        for i in similar_user_visits.columns:
            # Get the ratings for restaurant i
            restaurant_rating = similar_user_visits[i]
            # Create a variable to store the score
            total = 0
            # Create a variable to store the number of scores
            count = 0
            # Loop through similar users
            for u in similar_users.index:
                # If the restaurant has rating
                if pd.isna(restaurant_rating[u]) == False:
                    # Score is the sum of user similarity score multiply by the restaurant rating
                    score = similar_users[u] * restaurant_rating[u]
                    # Add the score to the total score for the restaurant so far
                    total += score
                    # Add 1 to the count
                    count +=1
            # Get the average score for the item
            item_score[i] = total / count

        # Convert dictionary to pandas dataframe
        item_score = pd.DataFrame(item_score.items(), columns=['place', 'place_score'])
            
        # Sort the restaurant by score
        ranked_item_score = item_score.sort_values(by='place_score', ascending=False)

        # Select top m restaurant
        m = number_of_top_restaurants
        return ranked_item_score.head(m)

    def recommendPopularRestaurants():
        unique_place_list=df.placeID.unique().tolist()
        final_list= pd.DataFrame(columns=['placeID','overall_rating'])
        for place in unique_place_list:
            place_exists=data.loc[data['placeID'] == place]
            # drop the columns food, service rating
            place_exists = place_exists.drop(['food_rating', 'service_rating'], axis=1);
            if (len(place_exists.index)>0):
                # print(place)
                new_row = pd.DataFrame(place_exists.groupby('placeID', as_index=False).mean())
                # print(new_row)
                final_list = pd.concat([new_row,final_list.loc[:]]).reset_index(drop=True)
                # print(final_list)
                # final_list=pd.concat([final_list, pd.DataFrame(place_exists.groupby('placeID')['overall_rating'].mean())])
        # print(final_list)
        return final_list

    picked_userID=sys.argv[1]
    specificUserData=data.loc[data['userID'] == picked_userID]
    if (len(specificUserData.index)>2):  
        number_of_similar_user=100
        number_of_top_restaurants=100
        recommendList= recommendRestaurants(picked_userID,number_of_similar_user,number_of_top_restaurants)
        df1 = recommendList.rename(columns={'place':'placeID'})
        joined_frame = df1.merge(df, on='placeID', how='left')
        #Drop N/A
        joined_frame_without_NAN=joined_frame.dropna()
        result=joined_frame_without_NAN.drop('id', axis=1)
        print(result.to_json(orient='records', indent=2))
    else:
        pRestaurants = recommendPopularRestaurants()
        # join with restaurants
        popCopyRestaurants = df.copy()
        # print(popCopyRestaurants)
        restaurants =  popCopyRestaurants.merge(pRestaurants, on="placeID", how="right")
        # sort values by asc
        restaurants = restaurants.sort_values(by='overall_rating', ascending=False)
        # drop the id column
        restaurants = restaurants.drop(['id'], axis=1)
        # convert to json response
        print(restaurants.to_json(orient='records', indent=2))

except Exception as error:
    print(error)
finally:
    if cur1 is not None:
        cur1.close()
    if cur2 is not None:
        cur2.close()
    if conn is not None:    
        conn.close()