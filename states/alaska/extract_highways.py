import osmium
import pandas as pd

rows = []

class HighwayHandler(osmium.SimpleHandler):
    def way(self, w):
        if 'highway' in w.tags:
            name = w.tags.get('name')
            ref = w.tags.get('ref')

            if name:
                rows.append({
                    "name": name,
                    "ref": ref
                })

handler = HighwayHandler()
handler.apply_file("alaska-260304.osm.pbf")

df = pd.DataFrame(rows).drop_duplicates()

df.to_csv("alaska_highways.csv", index=False)

print("Saved alaska_highways.csv")