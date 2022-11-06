import sqlalchemy

import pandas as pd

path = './data.json'

def read_dataset() -> pd.DataFrame:
    return pd.read_json(path, encoding='windows-1251')

df = read_dataset()

df.head()

df.columns

new_df = df[['UNOM', 'SIMPLE_ADDRESS', 'geoData', 'geodata_center']]
new_df.columns

new_df = new_df.rename({'UNOM': 'unom', 'SIMPLE_ADDRESS': 'address', 'geoData': 'pos_geojson', 'geodata_center': 'pos_geojson_center'}, axis=1)
new_df.columns

# new_df.loc[new_df['SIMPLE_ADDRESS'] == 'улица 800-летия Москвы, дом 5, корпус 1']

# new_df = new_df.dropna()
new_df['unom'] = new_df['unom'].astype(int)
new_df['pos_geojson'] = new_df['pos_geojson'].astype(str)
new_df['pos_geojson_center'] = new_df['pos_geojson_center'].astype(str)

url = 'postgresql+psycopg2://<user>:<host>@<host>:<port>/<db>'

engine = sqlalchemy.create_engine(url)

new_df.to_sql('addresses', engine, index=False, if_exists='append')

engine.execute('''
BEGIN;

DELETE FROM addresses WHERE
	pos_geojson = 'nan' AND pos_geojson_center = 'nan';

ALTER TABLE IF EXISTS addresses
	ADD COLUMN IF NOT EXISTS center_gis geometry;
ALTER TABLE IF EXISTS addresses
	ADD COLUMN IF NOT EXISTS center_x float;
ALTER TABLE IF EXISTS addresses
	ADD COLUMN IF NOT EXISTS center_y float;

CREATE INDEX IF NOT EXISTS addr_idx ON addresses(address);
CREATE UNIQUE INDEX IF NOT EXISTS addr_unom_idx ON addresses(unom);
CREATE INDEX IF NOT EXISTS addr_x ON addresses(center_x);
CREATE INDEX IF NOT EXISTS addr_y ON addresses(center_y);

UPDATE addresses
	SET center_gis = ST_SetSRID(ST_Centroid(ST_GeomFromGeoJSON(pos_geojson)), 4326)
	WHERE pos_geojson_center = 'nan';

UPDATE addresses
	SET center_gis = ST_SetSRID(ST_GeomFromGeoJSON(pos_geojson_center), 4326)
	WHERE pos_geojson_center != 'nan';

UPDATE addresses
	SET
		center_x = ST_X(center_gis),
		center_y = ST_Y(center_gis);

COMMIT;
''')


