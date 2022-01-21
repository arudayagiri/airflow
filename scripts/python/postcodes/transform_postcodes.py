import numpy as np
from scipy.spatial import distance
import pandas as pd
from datetime import datetime, date
from pathlib import Path
import os


# method to read data
def read_input_data(path):
    df_in = pd.read_csv(path)
    df_in = df_in[df_in['In Use?'] == 'Yes']
    return df_in


# GET all unique codes from postcode districts and run a loop
# for all district codes to find farthest euclidean distance between them
# save the results in a csv file
def transform_data(input_path, output_path):
    df_in = read_input_data(input_path)
    list_dists = df_in['Postcode district'].to_list()
    list_dists = list(set(list_dists))

    final_df = pd.DataFrame(columns=['start_postcodes', 'Postcode district', 'end_postcode'])

    for post in list_dists:
        mydict = {}
        new_pairs = []
        list_pairs = []

        df_individual = df_in[df_in['Postcode district'] == post]
        coords = list(zip(df_individual.Latitude, df_individual.Longitude))
        coords = np.array(coords)
        dists = distance.cdist(coords, coords, 'euclidean')
        indices = np.where(dists == dists.max())

        x_y_coords = zip(indices[0], indices[1])

        for i in x_y_coords:
            list_pairs.append(i)

        for a, b in list_pairs:
            if (a, b) in new_pairs or (b, a) in new_pairs:
                pass
        else:
            new_pairs += [(a, b)]

        pd.set_option('display.max_columns', None)

        mydict['start_postcodes'] = [
            df_individual[((df_individual['Latitude'] == coords[a][0]) & (df_individual['Longitude'] == coords[a][1]))][
                'Postcode'].to_list()]
        mydict['end_postcode'] = [
            df_individual[((df_individual['Latitude'] == coords[b][0]) & (df_individual['Longitude'] == coords[b][1]))][
                'Postcode'].to_list()]

        mydict['Postcode district'] = post
        df3 = pd.DataFrame(mydict)

        final_df = final_df.append(df3)
    write_data(final_df, output_path)


# method to write data
def write_data(df_out, out_path):
    df_out.to_csv(out_path)


if __name__ == '__main__':
    in_path = "/opt/airflow/data/postcodes_data/raw_data/" + str(date.today()) + "/postcodes.csv"
    output_file = 'transformed.csv'
    output_dir = Path("/opt/airflow/data/postcodes_data/transformed_data/" + str(date.today()))
    output_dir.mkdir(parents=True, exist_ok=True)
    out_put_path = os.path.join(output_dir, output_file)

    transform_data(in_path, out_put_path)
